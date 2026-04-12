/*********************************************************************************
 * Copyright 2024 eBay Inc.
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

#include <homestore/s3/chunk_store.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_physical_dev.h>
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

///////////////////////////////////////////////////////////////////////////////
// Mock CP Flush Callback
///////////////////////////////////////////////////////////////////////////////
class MockCpFlushCallback : public S3CpFlushCallback {
public:
    void trigger_early_cp_flush() override { ++flush_count; }
    int flush_count{0};
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
// S3PhysicalDev Tests
///////////////////////////////////////////////////////////////////////////////
class S3PhysicalDevTest : public ::testing::Test {
protected:
    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();
        m_cp_flush_cb = std::make_shared< MockCpFlushCallback >();

        S3KeyMapper key_mapper{.volume_id = "vol-001"};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, key_mapper);

        m_pdev = std::make_unique< S3PhysicalDev >(
            /*pdev_id=*/1, m_chunk_store, m_s3_store, "vol-001",
            /*dirty_cache_max_mb=*/1, // 1MB for testing
            m_cp_flush_cb);
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< MockCpFlushCallback > m_cp_flush_cb;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_pdev;
};

TEST_F(S3PhysicalDevTest, WriteStashesDirtyBlock) {
    constexpr chunk_id_t CHUNK_ID = 10;
    auto data = make_test_data(512, 0xAA);

    m_pdev->create_chunk(CHUNK_ID, 4096, S3ChunkType::DATA, 1);
    m_pdev->write(CHUNK_ID, 0, data);

    ASSERT_TRUE(m_pdev->is_chunk_dirty(CHUNK_ID));
    ASSERT_EQ(m_pdev->dirty_cache_size_bytes(), 512u);
}

TEST_F(S3PhysicalDevTest, WriteIsNotACopy) {
    constexpr chunk_id_t CHUNK_ID = 20;
    auto data = make_test_data(256, 0xBB);

    m_pdev->create_chunk(CHUNK_ID, 4096, S3ChunkType::DATA, 1);
    auto use_count_before = data.use_count();
    m_pdev->write(CHUNK_ID, 100, data);

    ASSERT_GT(data.use_count(), use_count_before);
}

TEST_F(S3PhysicalDevTest, MultipleWritesSameChunk) {
    constexpr chunk_id_t CHUNK_ID = 5;

    m_pdev->create_chunk(CHUNK_ID, 4096, S3ChunkType::DATA, 1);
    m_pdev->write(CHUNK_ID, 0, make_test_data(128, 0x01));
    m_pdev->write(CHUNK_ID, 128, make_test_data(256, 0x02));
    m_pdev->write(CHUNK_ID, 384, make_test_data(128, 0x03));

    ASSERT_TRUE(m_pdev->is_chunk_dirty(CHUNK_ID));
    ASSERT_EQ(m_pdev->dirty_cache_size_bytes(), 512u);
}

TEST_F(S3PhysicalDevTest, WriteToUnknownChunkIsRejected) {
    constexpr chunk_id_t CHUNK_ID = 999;
    auto data = make_test_data(128, 0xFF);

    // write() to a chunk that was never create_chunk()'d should be silently ignored
    m_pdev->write(CHUNK_ID, 0, data);

    ASSERT_FALSE(m_pdev->is_chunk_dirty(CHUNK_ID));
    ASSERT_EQ(m_pdev->dirty_cache_size_bytes(), 0u);
}

TEST_F(S3PhysicalDevTest, DrainDirtyCacheReturnsBlocks) {
    constexpr chunk_id_t CHUNK_ID = 15;

    m_pdev->create_chunk(CHUNK_ID, 4096, S3ChunkType::DATA, 1);
    m_pdev->write(CHUNK_ID, 0, make_test_data(100, 0xAA));
    m_pdev->write(CHUNK_ID, 200, make_test_data(100, 0xBB));

    auto blocks = m_pdev->drain_dirty_cache(CHUNK_ID);
    ASSERT_EQ(blocks.size(), 2u);
    ASSERT_EQ(blocks[0].offset, 0u);
    ASSERT_EQ(blocks[0].data->size(), 100u);
    ASSERT_EQ(blocks[0].data->cbytes()[0], 0xAA);
    ASSERT_EQ(blocks[1].offset, 200u);
    ASSERT_EQ(blocks[1].data->cbytes()[0], 0xBB);

    ASSERT_FALSE(m_pdev->is_chunk_dirty(CHUNK_ID));
    ASSERT_EQ(m_pdev->dirty_cache_size_bytes(), 0u);
}

TEST_F(S3PhysicalDevTest, DrainEmptyChunkReturnsEmpty) {
    auto blocks = m_pdev->drain_dirty_cache(999);
    ASSERT_TRUE(blocks.empty());
}

TEST_F(S3PhysicalDevTest, DrainAllDirtyCache) {
    m_pdev->create_chunk(1, 4096, S3ChunkType::DATA, 1);
    m_pdev->create_chunk(2, 4096, S3ChunkType::DATA, 1);
    m_pdev->create_chunk(3, 4096, S3ChunkType::DATA, 1);
    m_pdev->write(1, 0, make_test_data(64));
    m_pdev->write(2, 0, make_test_data(128));
    m_pdev->write(3, 0, make_test_data(256));

    auto dirty = m_pdev->get_dirty_chunks();
    ASSERT_EQ(dirty.size(), 3u);

    auto all_blocks = m_pdev->drain_all_dirty_cache();
    ASSERT_EQ(all_blocks.size(), 3u);
    ASSERT_EQ(m_pdev->dirty_cache_size_bytes(), 0u);
    ASSERT_TRUE(m_pdev->get_dirty_chunks().empty());
}

TEST_F(S3PhysicalDevTest, ReadGoesToChunkStore) {
    constexpr chunk_id_t CHUNK_ID = 42;
    constexpr uint64_t CHUNK_SIZE = 4096;

    auto nvme_data = make_test_data(CHUNK_SIZE, 0xCC);
    m_nvme_reader->set_chunk_data(CHUNK_ID, nvme_data);
    m_chunk_store->put(CHUNK_ID, {}, CHUNK_SIZE);

    auto [result, data] = m_pdev->read(CHUNK_ID, 0, 64).get();
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(data->size(), 64u);
    ASSERT_EQ(data->cbytes()[0], 0xCC);
}

TEST_F(S3PhysicalDevTest, CreateAndHasChunk) {
    m_pdev->create_chunk(10, 4096, S3ChunkType::DATA, 1);
    ASSERT_TRUE(m_pdev->has_chunk(10));
    ASSERT_FALSE(m_pdev->has_chunk(99));

    auto* entry = m_pdev->superblock().find_chunk(10);
    ASSERT_NE(entry, nullptr);
    ASSERT_EQ(entry->chunk_size, 4096u);
    ASSERT_EQ(entry->chunk_type, S3ChunkType::DATA);
}

TEST_F(S3PhysicalDevTest, RemoveChunkClearsDirtyCacheAndSuperblock) {
    constexpr chunk_id_t CHUNK_ID = 25;

    m_pdev->create_chunk(CHUNK_ID, 2048, S3ChunkType::INDEX, 2);
    m_pdev->write(CHUNK_ID, 0, make_test_data(128));
    ASSERT_TRUE(m_pdev->is_chunk_dirty(CHUNK_ID));

    m_pdev->remove_chunk(CHUNK_ID);
    ASSERT_FALSE(m_pdev->has_chunk(CHUNK_ID));
    ASSERT_FALSE(m_pdev->is_chunk_dirty(CHUNK_ID));
    ASSERT_EQ(m_pdev->dirty_cache_size_bytes(), 0u);
}

TEST_F(S3PhysicalDevTest, WriteAndReadSuperblock) {
    m_pdev->create_chunk(1, 4096, S3ChunkType::DATA, 1);
    m_pdev->create_chunk(2, 8192, S3ChunkType::METABLK, 2);

    auto write_result = m_pdev->write_superblock();
    ASSERT_TRUE(write_result.ok());

    auto pdev2 = std::make_unique< S3PhysicalDev >(
        1, m_chunk_store, m_s3_store, "vol-001", 1, nullptr);

    auto load_result = pdev2->load_superblock();
    ASSERT_TRUE(load_result.ok());

    ASSERT_EQ(pdev2->superblock().num_chunks(), 2u);
    ASSERT_NE(pdev2->superblock().find_chunk(1), nullptr);
    ASSERT_NE(pdev2->superblock().find_chunk(2), nullptr);
}

TEST_F(S3PhysicalDevTest, SuperblockGenerationIncrements) {
    ASSERT_EQ(m_pdev->superblock().generation(), 0u);
    m_pdev->write_superblock();
    ASSERT_EQ(m_pdev->superblock().generation(), 1u);
    m_pdev->write_superblock();
    ASSERT_EQ(m_pdev->superblock().generation(), 2u);
}

TEST_F(S3PhysicalDevTest, DirtyCacheThresholdTriggersEarlyFlush) {
    constexpr uint32_t BIG_SIZE = 512 * 1024;

    m_pdev->create_chunk(1, BIG_SIZE, S3ChunkType::DATA, 1);
    m_pdev->create_chunk(2, BIG_SIZE, S3ChunkType::DATA, 1);
    m_pdev->create_chunk(3, BIG_SIZE, S3ChunkType::DATA, 1);

    m_pdev->write(1, 0, make_test_data(BIG_SIZE));
    ASSERT_EQ(m_cp_flush_cb->flush_count, 0);

    m_pdev->write(2, 0, make_test_data(BIG_SIZE));
    ASSERT_EQ(m_cp_flush_cb->flush_count, 0);

    m_pdev->write(3, 0, make_test_data(BIG_SIZE)); // 1.5MB > 1MB
    ASSERT_GE(m_cp_flush_cb->flush_count, 1);
}

TEST_F(S3PhysicalDevTest, PdevId) {
    ASSERT_EQ(m_pdev->pdev_id(), 1u);
    ASSERT_EQ(m_pdev->volume_id(), "vol-001");
}

TEST_F(S3PhysicalDevTest, CreateMultipleChunkTypes) {
    m_pdev->create_chunk(1, 4096, S3ChunkType::DATA, 1);
    m_pdev->create_chunk(2, 4096, S3ChunkType::INDEX, 1);
    m_pdev->create_chunk(3, 4096, S3ChunkType::WAL, 2);
    m_pdev->create_chunk(4, 4096, S3ChunkType::METABLK, 3);

    ASSERT_EQ(m_pdev->superblock().num_chunks(), 4u);
    ASSERT_EQ(m_pdev->superblock().get_chunks_by_type(S3ChunkType::DATA).size(), 1u);
    ASSERT_EQ(m_pdev->superblock().get_chunks_by_type(S3ChunkType::METABLK).size(), 1u);
}

TEST_F(S3PhysicalDevTest, FullCPFlowSimulation) {
    constexpr chunk_id_t CHUNK_ID = 50;
    constexpr uint64_t CHUNK_SIZE = 4096;

    auto nvme_data = make_test_data(CHUNK_SIZE, 0xDD);
    m_nvme_reader->set_chunk_data(CHUNK_ID, nvme_data);

    m_pdev->create_chunk(CHUNK_ID, CHUNK_SIZE, S3ChunkType::DATA, 1);

    m_pdev->write(CHUNK_ID, 0, make_test_data(512, 0x01));
    m_pdev->write(CHUNK_ID, 512, make_test_data(512, 0x02));

    auto dirty_blocks = m_pdev->drain_dirty_cache(CHUNK_ID);
    ASSERT_EQ(dirty_blocks.size(), 2u);

    auto put_result = m_chunk_store->put(CHUNK_ID, dirty_blocks, CHUNK_SIZE);
    ASSERT_TRUE(put_result.ok());

    auto new_key = "vol-001/chunks/" + std::to_string(CHUNK_ID) + "/data.dat";
    m_pdev->superblock_mutable().update_chunk_key(CHUNK_ID, new_key, 1);

    auto sb_result = m_pdev->write_superblock();
    ASSERT_TRUE(sb_result.ok());

    ASSERT_FALSE(m_pdev->is_chunk_dirty(CHUNK_ID));
    ASSERT_EQ(m_pdev->dirty_cache_size_bytes(), 0u);

    auto [read_result, read_data] = m_pdev->read(CHUNK_ID, 0, 64).get();
    ASSERT_TRUE(read_result.ok());
    ASSERT_EQ(read_data->cbytes()[0], 0xDD);
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_physical_dev");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
