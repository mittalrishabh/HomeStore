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
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/chunk_eviction_manager.h>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/tiered_read_handler.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Mock NvmeChunkReader (for FullChunkStore::put — reads full chunk from NVMe)
///////////////////////////////////////////////////////////////////////////////
class MockNvmeChunkReader : public NvmeChunkReader {
public:
    void set_chunk_data(chunk_id_t chunk_id, sisl::byte_array data) {
        m_chunks[chunk_id] = std::move(data);
    }

    void remove_chunk_data(chunk_id_t chunk_id) {
        m_chunks.erase(chunk_id);
    }

    std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t chunk_id, uint64_t chunk_size) override {
        auto it = m_chunks.find(chunk_id);
        if (it == m_chunks.end()) {
            return {{.status_code = 404, .error_message = "Not on NVMe"}, {}};
        }
        auto copy = sisl::make_byte_array(static_cast< uint32_t >(chunk_size), 0);
        auto sz = std::min(static_cast< uint64_t >(it->second->size()), chunk_size);
        std::memcpy(copy->bytes(), it->second->cbytes(), sz);
        return {{.status_code = 0}, std::move(copy)};
    }

private:
    std::unordered_map< chunk_id_t, sisl::byte_array > m_chunks;
};

///////////////////////////////////////////////////////////////////////////////
// Mock NvmeChunkManager (for ChunkEvictionManager)
///////////////////////////////////////////////////////////////////////////////
class MockNvmeChunkManager : public NvmeChunkManager {
public:
    void set_on_nvme(chunk_id_t chunk_id, bool on) {
        if (on) m_on_nvme.insert(chunk_id);
        else m_on_nvme.erase(chunk_id);
    }

    bool is_chunk_on_nvme(chunk_id_t chunk_id) const override {
        return m_on_nvme.count(chunk_id) > 0;
    }

    std::error_code release_nvme_chunk(chunk_id_t chunk_id) override {
        if (m_fail_release) return std::make_error_code(std::errc::io_error);
        m_on_nvme.erase(chunk_id);
        m_released.insert(chunk_id);
        return {};
    }

    bool was_released(chunk_id_t chunk_id) const { return m_released.count(chunk_id) > 0; }
    void set_fail_release(bool fail) { m_fail_release = fail; }

private:
    std::set< chunk_id_t > m_on_nvme;
    std::set< chunk_id_t > m_released;
    bool m_fail_release{false};
};

///////////////////////////////////////////////////////////////////////////////
// Mock NvmeDeviceIO (for TieredReadHandler integration)
///////////////////////////////////////////////////////////////////////////////
class MockNvmeDeviceIO : public NvmeDeviceIO {
public:
    void store(chunk_id_t chunk_id, uint64_t offset, const char* data, uint64_t size) {
        auto key = std::to_string(chunk_id) + ":" + std::to_string(offset);
        auto buf = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(buf->bytes(), data, size);
        m_storage[key] = std::move(buf);
        m_on_nvme.insert(chunk_id);
    }

    void set_on_nvme(chunk_id_t chunk_id, bool on) {
        if (on) m_on_nvme.insert(chunk_id);
        else m_on_nvme.erase(chunk_id);
    }

    std::error_code nvme_read(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                              char* buf, uint64_t size) override {
        if (m_on_nvme.find(chunk_id) == m_on_nvme.end()) {
            return std::make_error_code(std::errc::no_such_device);
        }
        auto key = std::to_string(chunk_id) + ":" + std::to_string(offset_in_chunk);
        auto it = m_storage.find(key);
        if (it != m_storage.end()) {
            auto to_copy = std::min(size, static_cast< uint64_t >(it->second->size()));
            std::memcpy(buf, it->second->cbytes(), to_copy);
            return {};
        }
        return std::make_error_code(std::errc::no_such_file_or_directory);
    }

    std::error_code nvme_write(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                               const char* buf, uint64_t size) override {
        store(chunk_id, offset_in_chunk, buf, size);
        return {};
    }

    bool is_chunk_on_nvme(chunk_id_t chunk_id) const override {
        return m_on_nvme.count(chunk_id) > 0;
    }

private:
    std::unordered_map< std::string, sisl::byte_array > m_storage;
    std::set< chunk_id_t > m_on_nvme;
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
// Test Fixture
///////////////////////////////////////////////////////////////////////////////
class ChunkEvictionTest : public ::testing::Test {
protected:
    static constexpr uint64_t CHUNK_SIZE = 4096;

    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_chunk_reader = std::make_shared< MockNvmeChunkReader >();

        S3KeyMapper key_mapper{.volume_id = "vol-001"};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_chunk_reader, key_mapper);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, "vol-001", 1024, nullptr);

        m_nvme_mgr = std::make_shared< MockNvmeChunkManager >();

        m_eviction_mgr = std::make_unique< ChunkEvictionManager >(
            m_s3_pdev.get(), m_nvme_mgr);
    }

    void setup_chunk_on_both(chunk_id_t chunk_id, uint8_t pattern = 0xAB) {
        m_s3_pdev->create_chunk(chunk_id, CHUNK_SIZE, S3ChunkType::DATA, 1);
        auto data = make_test_data(CHUNK_SIZE, pattern);
        m_nvme_chunk_reader->set_chunk_data(chunk_id, data);
        m_chunk_store->put(chunk_id, {}, CHUNK_SIZE);
        m_nvme_mgr->set_on_nvme(chunk_id, true);
    }

    void setup_chunk_nvme_only(chunk_id_t chunk_id, uint8_t pattern = 0xCC) {
        auto data = make_test_data(CHUNK_SIZE, pattern);
        m_nvme_chunk_reader->set_chunk_data(chunk_id, data);
        m_nvme_mgr->set_on_nvme(chunk_id, true);
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_chunk_reader;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::shared_ptr< MockNvmeChunkManager > m_nvme_mgr;
    std::unique_ptr< ChunkEvictionManager > m_eviction_mgr;
};

///////////////////////////////////////////////////////////////////////////////
// Tests
///////////////////////////////////////////////////////////////////////////////

TEST_F(ChunkEvictionTest, SuccessfulEviction) {
    constexpr chunk_id_t CHUNK_ID = 10;
    setup_chunk_on_both(CHUNK_ID, 0xAA);

    auto result = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
    ASSERT_EQ(result, EvictionResult::SUCCESS);

    ASSERT_TRUE(m_nvme_mgr->was_released(CHUNK_ID));
    ASSERT_FALSE(m_nvme_mgr->is_chunk_on_nvme(CHUNK_ID));
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CHUNK_ID));
    ASSERT_FALSE(m_eviction_mgr->is_write_blocked(CHUNK_ID));
}

TEST_F(ChunkEvictionTest, EvictionRejectedNotOnS3) {
    constexpr chunk_id_t CHUNK_ID = 20;
    setup_chunk_nvme_only(CHUNK_ID);

    auto result = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
    ASSERT_EQ(result, EvictionResult::NOT_ON_S3);

    ASSERT_TRUE(m_nvme_mgr->is_chunk_on_nvme(CHUNK_ID));
    ASSERT_FALSE(m_eviction_mgr->is_chunk_evicted(CHUNK_ID));
}

TEST_F(ChunkEvictionTest, EvictionRejectedAlreadyEvicted) {
    constexpr chunk_id_t CHUNK_ID = 30;
    setup_chunk_on_both(CHUNK_ID);

    auto result1 = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
    ASSERT_EQ(result1, EvictionResult::SUCCESS);

    auto result2 = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
    ASSERT_EQ(result2, EvictionResult::ALREADY_EVICTED);
}

TEST_F(ChunkEvictionTest, EvictionFlushesAndDirtyData) {
    constexpr chunk_id_t CHUNK_ID = 40;
    setup_chunk_on_both(CHUNK_ID, 0xBB);

    // Write dirty data to the S3 pdev (cache only, not flushed to S3 yet)
    auto dirty = make_test_data(512, 0xDD);
    m_s3_pdev->write(CHUNK_ID, 0, dirty);
    ASSERT_TRUE(m_s3_pdev->is_chunk_dirty(CHUNK_ID));

    auto result = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
    ASSERT_EQ(result, EvictionResult::SUCCESS);

    // Dirty cache should be drained
    ASSERT_FALSE(m_s3_pdev->is_chunk_dirty(CHUNK_ID));
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CHUNK_ID));
}

TEST_F(ChunkEvictionTest, ReadAfterEvictionGoesToS3) {
    constexpr chunk_id_t CHUNK_ID = 50;
    constexpr uint8_t PATTERN = 0xEE;
    setup_chunk_on_both(CHUNK_ID, PATTERN);

    // Set up TieredReadHandler to verify S3 reads work after eviction
    auto nvme_io = std::make_shared< MockNvmeDeviceIO >();
    auto data = make_test_data(CHUNK_SIZE, PATTERN);
    nvme_io->store(CHUNK_ID, 0, reinterpret_cast< const char* >(data->cbytes()), CHUNK_SIZE);

    TieredReadHandler handler{m_s3_pdev.get(), nvme_io,
                              {.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false}};

    // Before eviction: NVMe fast path
    auto [ec1, data1] = handler.async_read(CHUNK_ID, 0, 64).get();
    ASSERT_FALSE(ec1);
    ASSERT_EQ(data1->cbytes()[0], PATTERN);

    // Evict the chunk
    auto result = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
    ASSERT_EQ(result, EvictionResult::SUCCESS);

    // Mark NVMe side as no longer available
    nvme_io->set_on_nvme(CHUNK_ID, false);

    // After eviction: read should go to S3
    auto [ec2, data2] = handler.async_read(CHUNK_ID, 0, 64).get();
    ASSERT_FALSE(ec2) << ec2.message();
    ASSERT_NE(data2, nullptr);
    ASSERT_EQ(data2->cbytes()[0], PATTERN);
}

TEST_F(ChunkEvictionTest, BitmapUnchangedAfterEviction) {
    constexpr chunk_id_t CHUNK_ID = 55;
    setup_chunk_on_both(CHUNK_ID, 0xCC);

    // Verify chunk is registered on S3 pdev (has superblock entry)
    ASSERT_TRUE(m_s3_pdev->has_chunk(CHUNK_ID));

    auto result = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
    ASSERT_EQ(result, EvictionResult::SUCCESS);

    // S3 pdev superblock entry should still exist (bitmap backed by S3)
    ASSERT_TRUE(m_s3_pdev->has_chunk(CHUNK_ID));
}

TEST_F(ChunkEvictionTest, NvmeReleaseFailureRollsBack) {
    constexpr chunk_id_t CHUNK_ID = 60;
    setup_chunk_on_both(CHUNK_ID);

    m_nvme_mgr->set_fail_release(true);

    auto result = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
    ASSERT_EQ(result, EvictionResult::INTERNAL_ERROR);

    // State should be rolled back to NORMAL (not evicted)
    ASSERT_FALSE(m_eviction_mgr->is_chunk_evicted(CHUNK_ID));
    ASSERT_FALSE(m_eviction_mgr->is_write_blocked(CHUNK_ID));
}

TEST_F(ChunkEvictionTest, WriteBlockedDuringEviction) {
    constexpr chunk_id_t CHUNK_ID = 70;
    setup_chunk_on_both(CHUNK_ID);

    // Before eviction, writes should not be blocked
    ASSERT_FALSE(m_eviction_mgr->is_write_blocked(CHUNK_ID));

    // After eviction, writes are not "blocked" (eviction is done)
    // but the chunk is evicted — new writes should trigger hydration
    auto result = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
    ASSERT_EQ(result, EvictionResult::SUCCESS);

    // is_write_blocked returns false (eviction complete, not in progress)
    ASSERT_FALSE(m_eviction_mgr->is_write_blocked(CHUNK_ID));
    // but the chunk IS evicted
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CHUNK_ID));
}

TEST_F(ChunkEvictionTest, MarkHydratedRestorescChunk) {
    constexpr chunk_id_t CHUNK_ID = 80;
    setup_chunk_on_both(CHUNK_ID);

    auto result = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
    ASSERT_EQ(result, EvictionResult::SUCCESS);
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CHUNK_ID));

    // After hydration, mark as restored
    m_eviction_mgr->mark_hydrated(CHUNK_ID);
    ASSERT_FALSE(m_eviction_mgr->is_chunk_evicted(CHUNK_ID));
}

TEST_F(ChunkEvictionTest, ConcurrentEvictions) {
    constexpr uint32_t NUM_CHUNKS = 8;

    for (uint32_t i = 0; i < NUM_CHUNKS; ++i) {
        setup_chunk_on_both(100 + i, static_cast< uint8_t >(0xA0 + i));
    }

    std::vector< std::thread > threads;
    std::vector< EvictionResult > results(NUM_CHUNKS);

    for (uint32_t i = 0; i < NUM_CHUNKS; ++i) {
        threads.emplace_back([this, i, &results]() {
            results[i] = m_eviction_mgr->evict_chunk(100 + i, CHUNK_SIZE);
        });
    }

    for (auto& t : threads) { t.join(); }

    for (uint32_t i = 0; i < NUM_CHUNKS; ++i) {
        ASSERT_EQ(results[i], EvictionResult::SUCCESS) << "chunk " << (100 + i);
        ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(100 + i));
        ASSERT_TRUE(m_nvme_mgr->was_released(100 + i));
    }
}

TEST_F(ChunkEvictionTest, DoubleEvictionFromConcurrentThreads) {
    constexpr chunk_id_t CHUNK_ID = 200;
    setup_chunk_on_both(CHUNK_ID);

    std::vector< std::thread > threads;
    std::vector< EvictionResult > results(4);

    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([this, i, &results]() {
            results[i] = m_eviction_mgr->evict_chunk(CHUNK_ID, CHUNK_SIZE);
        });
    }

    for (auto& t : threads) { t.join(); }

    int successes = 0;
    for (auto r : results) {
        if (r == EvictionResult::SUCCESS) ++successes;
        else {
            ASSERT_TRUE(r == EvictionResult::ALREADY_EVICTED ||
                        r == EvictionResult::EVICTION_IN_PROGRESS)
                << "unexpected result: " << to_string(r);
        }
    }
    // At least one thread should succeed
    ASSERT_GE(successes, 1);
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CHUNK_ID));
}

TEST_F(ChunkEvictionTest, EvictionResultToString) {
    ASSERT_EQ(to_string(EvictionResult::SUCCESS), "SUCCESS");
    ASSERT_EQ(to_string(EvictionResult::NOT_ON_S3), "NOT_ON_S3");
    ASSERT_EQ(to_string(EvictionResult::ALREADY_EVICTED), "ALREADY_EVICTED");
    ASSERT_EQ(to_string(EvictionResult::EVICTION_IN_PROGRESS), "EVICTION_IN_PROGRESS");
    ASSERT_EQ(to_string(EvictionResult::HAS_DIRTY_DATA), "HAS_DIRTY_DATA");
    ASSERT_EQ(to_string(EvictionResult::FLUSH_FAILED), "FLUSH_FAILED");
    ASSERT_EQ(to_string(EvictionResult::INTERNAL_ERROR), "INTERNAL_ERROR");
}

TEST_F(ChunkEvictionTest, UnknownChunkNotEvicted) {
    // Chunk 999 was never registered — should not be evicted
    ASSERT_FALSE(m_eviction_mgr->is_chunk_evicted(999));
    ASSERT_FALSE(m_eviction_mgr->is_write_blocked(999));
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_chunk_eviction");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
