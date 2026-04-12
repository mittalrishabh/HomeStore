/*********************************************************************************
 * Copyright 2026 eBay Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 *********************************************************************************/

#include <cstring>
#include <memory>
#include <string>

#include <gtest/gtest.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/checkpoint/cp.hpp>
#include <homestore/checkpoint/cp_mgr.hpp>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/s3_cp.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Mock NVMe Reader
///////////////////////////////////////////////////////////////////////////////
class MockNvmeChunkReader : public NvmeChunkReader {
public:
    void set_chunk_data(chunk_id_t chunk_id, sisl::byte_array data) {
        m_chunks[chunk_id] = std::move(data);
    }

    std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t chunk_id, uint64_t chunk_size) override {
        auto it = m_chunks.find(chunk_id);
        if (it == m_chunks.end()) {
            S3Result r;
            r.status_code = 404;
            r.error_message = "Chunk not on NVMe";
            return {r, {}};
        }
        auto copy = sisl::make_byte_array(static_cast< uint32_t >(chunk_size), 0);
        auto sz = std::min(static_cast< uint64_t >(it->second->size()), chunk_size);
        std::memcpy(copy->bytes(), it->second->cbytes(), sz);
        S3Result ok;
        ok.status_code = 0;
        return {ok, std::move(copy)};
    }

private:
    std::unordered_map< chunk_id_t, sisl::byte_array > m_chunks;
};

/**
 * @brief Mock S3ObjectStore that records the order of put_object calls.
 *
 * Used to verify that chunks are flushed in the correct type order
 * (DATA → WAL → INDEX → METABLK).
 */
class OrderTrackingS3Store : public MockS3ObjectStore {
public:
    using MockS3ObjectStore::MockS3ObjectStore;

    folly::Future< S3Result > put_object(const std::string& key, sisl::io_blob_safe data) override {
        m_put_order.push_back(key);
        return MockS3ObjectStore::put_object(key, std::move(data));
    }

    const std::vector< std::string >& put_order() const { return m_put_order; }
    void clear_order() { m_put_order.clear(); }

private:
    std::vector< std::string > m_put_order;
};

///////////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////////
static sisl::byte_array make_test_data(uint32_t size, uint8_t pattern = 0xAB) {
    auto buf = sisl::make_byte_array(size, 0);
    std::memset(buf->bytes(), pattern, size);
    return buf;
}

static S3ObjectStoreConfig make_test_config() {
    S3ObjectStoreConfig cfg;
    cfg.bucket = "homestore-test";
    cfg.region = "us-east-1";
    cfg.retry_count = 1;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

///////////////////////////////////////////////////////////////////////////////
// Minimal CP stub for testing (doesn't need full CPManager)
///////////////////////////////////////////////////////////////////////////////
class TestCP : public CP {
public:
    TestCP() : CP{nullptr} {
        m_cp_id = 1;
        m_cp_status.store(cp_status_t::cp_flushing);
    }
};

///////////////////////////////////////////////////////////////////////////////
// S3 CP Tests
///////////////////////////////////////////////////////////////////////////////
class S3CpTest : public ::testing::Test {
protected:
    void SetUp() override {
        m_s3_store = std::make_shared< OrderTrackingS3Store >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();

        S3KeyMapper key_mapper{.volume_id = "vol-001"};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, key_mapper);

        // Create two S3 pdevs
        m_pdev1 = std::make_unique< S3PhysicalDev >(
            /*pdev_id=*/1, m_chunk_store, m_s3_store, "vol-001",
            /*dirty_cache_max_mb=*/1024, nullptr);

        m_pdev2 = std::make_unique< S3PhysicalDev >(
            /*pdev_id=*/2, m_chunk_store, m_s3_store, "vol-002",
            /*dirty_cache_max_mb=*/1024, nullptr);

        m_callbacks = std::make_unique< S3CpCallbacks >(
            std::vector< S3PhysicalDev* >{m_pdev1.get(), m_pdev2.get()});
    }

    std::shared_ptr< OrderTrackingS3Store > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_pdev1;
    std::unique_ptr< S3PhysicalDev > m_pdev2;
    std::unique_ptr< S3CpCallbacks > m_callbacks;
};

TEST_F(S3CpTest, SwitchoverDetectsDirtyPdevs) {
    m_pdev1->create_chunk(10, 4096, S3ChunkType::DATA, 1);
    m_pdev1->write(10, 0, make_test_data(512, 0xAA));

    auto cur_cp = std::make_unique< TestCP >();
    auto new_cp = std::make_unique< TestCP >();

    auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
    auto* ctx = static_cast< S3CpContext* >(ctx_ptr.get());

    ASSERT_EQ(ctx->m_dirty_pdevs.size(), 1u);
    ASSERT_EQ(ctx->m_dirty_pdevs[0], m_pdev1.get());
    ASSERT_EQ(ctx->m_total_chunks.load(), 1u);
    // Dirty cache should be drained at switchover
    ASSERT_EQ(ctx->m_drained_data.size(), 1u);
    ASSERT_EQ(ctx->m_drained_data[0].size(), 1u);
}

TEST_F(S3CpTest, SwitchoverDrainsCacheAtomically) {
    m_pdev1->create_chunk(10, 4096, S3ChunkType::DATA, 1);
    m_pdev1->write(10, 0, make_test_data(512, 0xAA));

    auto cur_cp = std::make_unique< TestCP >();
    auto new_cp = std::make_unique< TestCP >();
    m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());

    // After switchover, dirty cache should be empty (drained into context)
    ASSERT_FALSE(m_pdev1->is_chunk_dirty(10));
    ASSERT_EQ(m_pdev1->dirty_cache_size_bytes(), 0u);
}

TEST_F(S3CpTest, SwitchoverNoDirtyPdevs) {
    auto cur_cp = std::make_unique< TestCP >();
    auto new_cp = std::make_unique< TestCP >();

    auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
    auto* ctx = static_cast< S3CpContext* >(ctx_ptr.get());

    ASSERT_TRUE(ctx->m_dirty_pdevs.empty());
    ASSERT_EQ(ctx->m_total_chunks.load(), 0u);
}

TEST_F(S3CpTest, SwitchoverMultipleDirtyPdevs) {
    m_pdev1->create_chunk(10, 4096, S3ChunkType::DATA, 1);
    m_pdev1->write(10, 0, make_test_data(256));

    m_pdev2->create_chunk(20, 4096, S3ChunkType::INDEX, 2);
    m_pdev2->write(20, 0, make_test_data(128));

    auto cur_cp = std::make_unique< TestCP >();
    auto new_cp = std::make_unique< TestCP >();

    auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
    auto* ctx = static_cast< S3CpContext* >(ctx_ptr.get());

    ASSERT_EQ(ctx->m_dirty_pdevs.size(), 2u);
    ASSERT_EQ(ctx->m_total_chunks.load(), 2u);
    ASSERT_EQ(ctx->m_drained_data.size(), 2u);
}

TEST_F(S3CpTest, FlushUploadsDirtyChunksAndWritesSuperblock) {
    constexpr chunk_id_t CHUNK_ID = 10;
    constexpr uint64_t CHUNK_SIZE = 4096;

    auto nvme_data = make_test_data(CHUNK_SIZE, 0xDD);
    m_nvme_reader->set_chunk_data(CHUNK_ID, nvme_data);

    m_pdev1->create_chunk(CHUNK_ID, CHUNK_SIZE, S3ChunkType::DATA, 1);
    m_pdev1->write(CHUNK_ID, 0, make_test_data(512, 0x01));
    m_pdev1->write(CHUNK_ID, 512, make_test_data(512, 0x02));

    auto cur_cp = std::make_unique< TestCP >();
    auto new_cp = std::make_unique< TestCP >();
    auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
    new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx_ptr));

    auto flush_result = m_callbacks->cp_flush(new_cp.get()).get();
    ASSERT_TRUE(flush_result);

    // Verify: chunk was uploaded to S3
    auto s3_key = "vol-001/chunks/" + std::to_string(CHUNK_ID) + "/data.dat";
    ASSERT_TRUE(m_s3_store->has_object(s3_key));

    // Verify: superblock was written to S3
    auto sb_key = "vol-001/pdev_superblock.bin";
    ASSERT_TRUE(m_s3_store->has_object(sb_key));
    ASSERT_EQ(m_pdev1->superblock().generation(), 1u);
}

TEST_F(S3CpTest, FlushOrderIsDataWalIndexMetablk) {
    constexpr uint64_t CHUNK_SIZE = 4096;

    // Create chunks of each type
    m_pdev1->create_chunk(1, CHUNK_SIZE, S3ChunkType::METABLK, 1);
    m_pdev1->create_chunk(2, CHUNK_SIZE, S3ChunkType::INDEX, 1);
    m_pdev1->create_chunk(3, CHUNK_SIZE, S3ChunkType::DATA, 1);
    m_pdev1->create_chunk(4, CHUNK_SIZE, S3ChunkType::WAL, 1);

    // Set up NVMe data
    for (chunk_id_t cid = 1; cid <= 4; ++cid) {
        m_nvme_reader->set_chunk_data(cid, make_test_data(CHUNK_SIZE, static_cast< uint8_t >(cid)));
    }

    // Write dirty data to all chunks
    for (chunk_id_t cid = 1; cid <= 4; ++cid) {
        m_pdev1->write(cid, 0, make_test_data(128, static_cast< uint8_t >(cid)));
    }

    // Clear put order tracking
    m_s3_store->clear_order();

    // Flush
    auto cur_cp = std::make_unique< TestCP >();
    auto new_cp = std::make_unique< TestCP >();
    auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
    new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx_ptr));
    auto flush_result = m_callbacks->cp_flush(new_cp.get()).get();
    ASSERT_TRUE(flush_result);

    // Verify put order: DATA(3) → WAL(4) → INDEX(2) → METABLK(1) → superblock
    auto& order = m_s3_store->put_order();
    ASSERT_GE(order.size(), 5u); // 4 chunks + 1 superblock

    // Find the chunk data puts (not superblock)
    std::vector< std::string > chunk_puts;
    for (const auto& key : order) {
        if (key.find("/chunks/") != std::string::npos) {
            chunk_puts.push_back(key);
        }
    }
    ASSERT_EQ(chunk_puts.size(), 4u);

    // Expected order: chunk 3 (DATA), chunk 4 (WAL), chunk 2 (INDEX), chunk 1 (METABLK)
    EXPECT_NE(chunk_puts[0].find("/chunks/3/"), std::string::npos) << "First should be DATA (chunk 3)";
    EXPECT_NE(chunk_puts[1].find("/chunks/4/"), std::string::npos) << "Second should be WAL (chunk 4)";
    EXPECT_NE(chunk_puts[2].find("/chunks/2/"), std::string::npos) << "Third should be INDEX (chunk 2)";
    EXPECT_NE(chunk_puts[3].find("/chunks/1/"), std::string::npos) << "Fourth should be METABLK (chunk 1)";

    // Superblock should be the last put
    EXPECT_NE(order.back().find("pdev_superblock"), std::string::npos)
        << "Superblock should be the last S3 write";
}

TEST_F(S3CpTest, FlushMultipleChunksAcrossMultiplePdevs) {
    m_pdev1->create_chunk(10, 4096, S3ChunkType::DATA, 1);
    m_pdev1->create_chunk(11, 4096, S3ChunkType::INDEX, 1);
    m_nvme_reader->set_chunk_data(10, make_test_data(4096, 0xAA));
    m_nvme_reader->set_chunk_data(11, make_test_data(4096, 0xBB));
    m_pdev1->write(10, 0, make_test_data(256));
    m_pdev1->write(11, 0, make_test_data(128));

    m_pdev2->create_chunk(20, 8192, S3ChunkType::WAL, 2);
    m_nvme_reader->set_chunk_data(20, make_test_data(8192, 0xCC));
    m_pdev2->write(20, 0, make_test_data(512));

    auto cur_cp = std::make_unique< TestCP >();
    auto new_cp = std::make_unique< TestCP >();
    auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
    auto* ctx = static_cast< S3CpContext* >(ctx_ptr.get());
    ASSERT_EQ(ctx->m_total_chunks.load(), 3u);
    new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx_ptr));

    auto flush_result = m_callbacks->cp_flush(new_cp.get()).get();
    ASSERT_TRUE(flush_result);
}

TEST_F(S3CpTest, FlushWithNoDirtyData) {
    auto cur_cp = std::make_unique< TestCP >();
    auto new_cp = std::make_unique< TestCP >();
    auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
    new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx_ptr));

    auto flush_result = m_callbacks->cp_flush(new_cp.get()).get();
    ASSERT_TRUE(flush_result);
}

TEST_F(S3CpTest, ProgressReporting) {
    m_pdev1->create_chunk(10, 4096, S3ChunkType::DATA, 1);
    m_pdev1->write(10, 0, make_test_data(256));

    ASSERT_EQ(m_callbacks->cp_progress_percent(), 100);

    auto cur_cp = std::make_unique< TestCP >();
    auto new_cp = std::make_unique< TestCP >();
    auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
    new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx_ptr));
    ASSERT_EQ(m_callbacks->cp_progress_percent(), 0);

    m_nvme_reader->set_chunk_data(10, make_test_data(4096, 0xDD));
    m_callbacks->cp_flush(new_cp.get()).get();
    ASSERT_EQ(m_callbacks->cp_progress_percent(), 100);
}

TEST_F(S3CpTest, SuperblockGenerationIncrements) {
    constexpr chunk_id_t CHUNK_ID = 10;
    constexpr uint64_t CHUNK_SIZE = 4096;

    m_pdev1->create_chunk(CHUNK_ID, CHUNK_SIZE, S3ChunkType::DATA, 1);
    m_nvme_reader->set_chunk_data(CHUNK_ID, make_test_data(CHUNK_SIZE, 0xDD));

    auto initial_gen = m_pdev1->superblock().generation();

    // CP 1
    m_pdev1->write(CHUNK_ID, 0, make_test_data(256));
    {
        auto cur_cp = std::make_unique< TestCP >();
        auto new_cp = std::make_unique< TestCP >();
        auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
        new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx_ptr));
        m_callbacks->cp_flush(new_cp.get()).get();
    }
    ASSERT_EQ(m_pdev1->superblock().generation(), initial_gen + 1);

    // CP 2
    m_pdev1->write(CHUNK_ID, 256, make_test_data(128));
    {
        auto cur_cp = std::make_unique< TestCP >();
        auto new_cp = std::make_unique< TestCP >();
        auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
        new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx_ptr));
        m_callbacks->cp_flush(new_cp.get()).get();
    }
    ASSERT_EQ(m_pdev1->superblock().generation(), initial_gen + 2);
}

TEST_F(S3CpTest, CleanupIsNoOp) {
    auto cur_cp = std::make_unique< TestCP >();
    auto new_cp = std::make_unique< TestCP >();
    auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
    new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx_ptr));
    m_callbacks->cp_cleanup(new_cp.get());
}

TEST_F(S3CpTest, ConsumerIdExists) {
    ASSERT_EQ(static_cast< uint8_t >(cp_consumer_t::S3_SVC), 4u);
    ASSERT_EQ(static_cast< uint8_t >(cp_consumer_t::SENTINEL), 5u);
}

TEST_F(S3CpTest, S3CpCallbacksImplementsFlushCallback) {
    // Verify S3CpCallbacks can be used as S3CpFlushCallback
    S3CpFlushCallback* flush_cb = m_callbacks.get();
    ASSERT_NE(flush_cb, nullptr);
    // trigger_early_cp_flush() calls cp_mgr() which isn't initialized in test,
    // so we just verify the interface is wired up.
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_cp");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
