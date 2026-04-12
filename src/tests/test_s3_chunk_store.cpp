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
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Mock NVMe Chunk Reader — returns deterministic test data
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
// Helpers
///////////////////////////////////////////////////////////////////////////////
static sisl::byte_array make_test_data(uint32_t size, uint8_t pattern = 0xAB) {
    auto buf = sisl::make_byte_array(size, 0);
    std::memset(buf->bytes(), pattern, size);
    return buf;
}

static sisl::byte_array make_patterned_data(uint32_t size) {
    auto buf = sisl::make_byte_array(size, 0);
    for (uint32_t i = 0; i < size; ++i) {
        buf->bytes()[i] = static_cast< uint8_t >(i & 0xFF);
    }
    return buf;
}

static S3ObjectStoreConfig make_test_config() {
    S3ObjectStoreConfig cfg;
    cfg.bucket = "homestore-test-cluster";
    cfg.region = "us-east-1";
    cfg.retry_count = 1;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

///////////////////////////////////////////////////////////////////////////////
// FullChunkStore Tests
///////////////////////////////////////////////////////////////////////////////
class FullChunkStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();
        m_key_mapper = S3KeyMapper{.volume_id = "vol-001"};
        m_chunk_store = std::make_unique< FullChunkStore >(m_s3_store, m_nvme_reader, m_key_mapper);
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    S3KeyMapper m_key_mapper;
    std::unique_ptr< FullChunkStore > m_chunk_store;
};

TEST_F(FullChunkStoreTest, PutUploadsFullChunkToS3) {
    constexpr chunk_id_t CHUNK_ID = 42;
    constexpr uint64_t CHUNK_SIZE = 4096;

    auto nvme_data = make_patterned_data(CHUNK_SIZE);
    m_nvme_reader->set_chunk_data(CHUNK_ID, nvme_data);

    std::vector< DirtyBlock > dirty_blocks;
    auto result = m_chunk_store->put(CHUNK_ID, dirty_blocks, CHUNK_SIZE);
    ASSERT_TRUE(result.ok()) << result.error_message;

    auto expected_key = m_key_mapper.chunk_data_key(CHUNK_ID);
    ASSERT_TRUE(m_s3_store->has_object(expected_key));
}

TEST_F(FullChunkStoreTest, PutThenGetRoundTrip) {
    constexpr chunk_id_t CHUNK_ID = 7;
    constexpr uint64_t CHUNK_SIZE = 8192;

    auto nvme_data = make_patterned_data(CHUNK_SIZE);
    m_nvme_reader->set_chunk_data(CHUNK_ID, nvme_data);

    auto put_result = m_chunk_store->put(CHUNK_ID, {}, CHUNK_SIZE);
    ASSERT_TRUE(put_result.ok());

    constexpr offset_t OFFSET = 1024;
    constexpr uint64_t READ_SIZE = 256;
    auto get_future = m_chunk_store->get(CHUNK_ID, OFFSET, READ_SIZE);
    auto [get_result, data] = std::move(get_future).get();
    ASSERT_TRUE(get_result.ok()) << get_result.error_message;
    ASSERT_NE(data, nullptr);
    ASSERT_EQ(data->size(), READ_SIZE);

    for (uint64_t i = 0; i < READ_SIZE; ++i) {
        EXPECT_EQ(data->cbytes()[i], static_cast< uint8_t >((OFFSET + i) & 0xFF))
            << "Mismatch at byte " << i;
    }
}

TEST_F(FullChunkStoreTest, GetCachesLocally) {
    constexpr chunk_id_t CHUNK_ID = 10;
    constexpr uint64_t CHUNK_SIZE = 4096;

    auto nvme_data = make_test_data(CHUNK_SIZE, 0xCC);
    m_nvme_reader->set_chunk_data(CHUNK_ID, nvme_data);
    m_chunk_store->put(CHUNK_ID, {}, CHUNK_SIZE);

    auto [r1, d1] = m_chunk_store->get(CHUNK_ID, 0, 64).get();
    ASSERT_TRUE(r1.ok());

    // Delete from S3 to prove second get uses cache
    auto s3_key = m_key_mapper.chunk_data_key(CHUNK_ID);
    m_s3_store->delete_object(s3_key).get();
    ASSERT_FALSE(m_s3_store->has_object(s3_key));

    auto [r2, d2] = m_chunk_store->get(CHUNK_ID, 0, 64).get();
    ASSERT_TRUE(r2.ok());
    ASSERT_EQ(d2->cbytes()[0], 0xCC);
}

TEST_F(FullChunkStoreTest, GetInvalidOffsetReturnsError) {
    constexpr chunk_id_t CHUNK_ID = 20;
    constexpr uint64_t CHUNK_SIZE = 1024;

    auto nvme_data = make_test_data(CHUNK_SIZE);
    m_nvme_reader->set_chunk_data(CHUNK_ID, nvme_data);
    m_chunk_store->put(CHUNK_ID, {}, CHUNK_SIZE);

    auto [result, data] = m_chunk_store->get(CHUNK_ID, 900, 200).get();
    ASSERT_FALSE(result.ok());
}

TEST_F(FullChunkStoreTest, GetNonexistentChunkReturnsError) {
    auto [result, data] = m_chunk_store->get(999, 0, 64).get();
    ASSERT_FALSE(result.ok());
}

TEST_F(FullChunkStoreTest, CompactIsNoOp) {
    auto result = m_chunk_store->compact(42);
    ASSERT_TRUE(result.ok());
}

TEST_F(FullChunkStoreTest, RecoverDownloadsFromS3) {
    constexpr chunk_id_t CHUNK_ID = 5;
    constexpr uint64_t CHUNK_SIZE = 2048;

    // Directly put data in S3
    auto s3_key = m_key_mapper.chunk_data_key(CHUNK_ID);
    auto test_data = make_patterned_data(CHUNK_SIZE);
    sisl::io_blob_safe blob(CHUNK_SIZE, 0);
    std::memcpy(blob.bytes(), test_data->cbytes(), CHUNK_SIZE);
    m_s3_store->put_object(s3_key, std::move(blob)).get();

    auto state = m_chunk_store->recover(CHUNK_ID);
    ASSERT_TRUE(state.valid);
    ASSERT_EQ(state.chunk_id, CHUNK_ID);
    ASSERT_EQ(state.chunk_size, CHUNK_SIZE);
    ASSERT_NE(state.data, nullptr);

    for (uint64_t i = 0; i < CHUNK_SIZE; ++i) {
        EXPECT_EQ(state.data->cbytes()[i], static_cast< uint8_t >(i & 0xFF));
    }
}

TEST_F(FullChunkStoreTest, RecoverNonexistentChunkReturnsInvalid) {
    auto state = m_chunk_store->recover(999);
    ASSERT_FALSE(state.valid);
}

TEST_F(FullChunkStoreTest, DescribeReturnsCorrectMetadata) {
    constexpr chunk_id_t CHUNK_ID = 15;
    constexpr uint64_t CHUNK_SIZE = 4096;

    auto nvme_data = make_test_data(CHUNK_SIZE);
    m_nvme_reader->set_chunk_data(CHUNK_ID, nvme_data);
    m_chunk_store->put(CHUNK_ID, {}, CHUNK_SIZE);

    auto meta = m_chunk_store->describe(CHUNK_ID);
    ASSERT_EQ(meta.chunk_id, CHUNK_ID);
    ASSERT_EQ(meta.s3_key, m_key_mapper.chunk_data_key(CHUNK_ID));
    ASSERT_EQ(meta.s3_object_size, CHUNK_SIZE);
}

TEST_F(FullChunkStoreTest, InvalidateCacheForcesFetchFromS3) {
    constexpr chunk_id_t CHUNK_ID = 30;
    constexpr uint64_t CHUNK_SIZE = 1024;

    auto nvme_data = make_test_data(CHUNK_SIZE, 0xAA);
    m_nvme_reader->set_chunk_data(CHUNK_ID, nvme_data);
    m_chunk_store->put(CHUNK_ID, {}, CHUNK_SIZE);

    m_chunk_store->get(CHUNK_ID, 0, 64).get();

    // Update S3 data directly
    auto s3_key = m_key_mapper.chunk_data_key(CHUNK_ID);
    auto new_data = make_test_data(CHUNK_SIZE, 0xBB);
    sisl::io_blob_safe blob(CHUNK_SIZE, 0);
    std::memcpy(blob.bytes(), new_data->cbytes(), CHUNK_SIZE);
    m_s3_store->put_object(s3_key, std::move(blob)).get();

    m_chunk_store->invalidate_cache(CHUNK_ID);

    auto [result, data] = m_chunk_store->get(CHUNK_ID, 0, 64).get();
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(data->cbytes()[0], 0xBB);
}

TEST_F(FullChunkStoreTest, PutWithDirtyBlocksStillWorks) {
    constexpr chunk_id_t CHUNK_ID = 50;
    constexpr uint64_t CHUNK_SIZE = 4096;

    auto nvme_data = make_patterned_data(CHUNK_SIZE);
    m_nvme_reader->set_chunk_data(CHUNK_ID, nvme_data);

    std::vector< DirtyBlock > dirty_blocks;
    DirtyBlock db;
    db.offset = 100;
    db.data = make_test_data(512, 0xFF);
    dirty_blocks.push_back(std::move(db));

    auto result = m_chunk_store->put(CHUNK_ID, dirty_blocks, CHUNK_SIZE);
    ASSERT_TRUE(result.ok());
}

///////////////////////////////////////////////////////////////////////////////
// S3KeyMapper Tests
///////////////////////////////////////////////////////////////////////////////
TEST(S3KeyMapperTest, ChunkDataKeyNoSnapshot) {
    S3KeyMapper mapper{.volume_id = "vol-123"};
    ASSERT_EQ(mapper.chunk_data_key(42), "vol-123/chunks/42/data.dat");
}

TEST(S3KeyMapperTest, ChunkDataKeyWithGeneration) {
    S3KeyMapper mapper{.volume_id = "vol-123"};
    ASSERT_EQ(mapper.chunk_data_key(42, 5), "vol-123/chunks/42/data_gen5.dat");
}

TEST(S3KeyMapperTest, ChunkPrefix) {
    S3KeyMapper mapper{.volume_id = "vol-abc"};
    ASSERT_EQ(mapper.chunk_prefix(7), "vol-abc/chunks/7/");
}

///////////////////////////////////////////////////////////////////////////////
// PdevS3Superblock Tests
///////////////////////////////////////////////////////////////////////////////
class PdevS3SuperblockTest : public ::testing::Test {
protected:
    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
};

TEST_F(PdevS3SuperblockTest, SerializeDeserializeRoundTrip) {
    PdevS3Superblock sb;
    sb.set_pdev_id(1);
    sb.set_generation(42);

    s3_chunk_entry entry1;
    entry1.chunk_id = 10;
    entry1.chunk_size = 4096;
    entry1.chunk_type = S3ChunkType::DATA;
    entry1.vdev_id = 1;
    entry1.set_s3_key("vol-001/chunks/10/data.dat");
    sb.add_chunk(entry1);

    s3_chunk_entry entry2;
    entry2.chunk_id = 20;
    entry2.chunk_size = 8192;
    entry2.chunk_type = S3ChunkType::METABLK;
    entry2.vdev_id = 2;
    entry2.set_s3_key("vol-001/chunks/20/data.dat");
    sb.add_chunk(entry2);

    auto buf = sb.serialize();
    ASSERT_NE(buf, nullptr);
    ASSERT_GT(buf->size(), 0u);

    PdevS3Superblock sb2;
    ASSERT_TRUE(sb2.deserialize(buf));

    ASSERT_EQ(sb2.pdev_id(), 1u);
    ASSERT_EQ(sb2.generation(), 42u);
    ASSERT_EQ(sb2.num_chunks(), 2u);

    auto* c1 = sb2.find_chunk(10);
    ASSERT_NE(c1, nullptr);
    ASSERT_EQ(c1->chunk_size, 4096u);
    ASSERT_EQ(c1->chunk_type, S3ChunkType::DATA);
    ASSERT_EQ(c1->get_s3_key(), "vol-001/chunks/10/data.dat");

    auto* c2 = sb2.find_chunk(20);
    ASSERT_NE(c2, nullptr);
    ASSERT_EQ(c2->chunk_size, 8192u);
    ASSERT_EQ(c2->chunk_type, S3ChunkType::METABLK);
}

TEST_F(PdevS3SuperblockTest, ChecksumCatchesCorruption) {
    PdevS3Superblock sb;
    sb.set_pdev_id(1);
    sb.set_generation(1);

    s3_chunk_entry entry;
    entry.chunk_id = 5;
    entry.chunk_size = 1024;
    entry.chunk_type = S3ChunkType::WAL;
    sb.add_chunk(entry);

    auto buf = sb.serialize();
    buf->bytes()[buf->size() / 2] ^= 0xFF;

    PdevS3Superblock sb2;
    ASSERT_FALSE(sb2.deserialize(buf));
}

TEST_F(PdevS3SuperblockTest, DeserializeEmptyBufferFails) {
    PdevS3Superblock sb;
    auto empty = sisl::make_byte_array(4, 0);
    ASSERT_FALSE(sb.deserialize(empty));
}

TEST_F(PdevS3SuperblockTest, DeserializeWrongMagicFails) {
    PdevS3Superblock sb;
    sb.set_pdev_id(1);
    auto buf = sb.serialize();
    buf->bytes()[0] = 0x00;
    buf->bytes()[1] = 0x00;

    PdevS3Superblock sb2;
    ASSERT_FALSE(sb2.deserialize(buf));
}

TEST_F(PdevS3SuperblockTest, WriteAndReadFromS3) {
    PdevS3Superblock sb;
    sb.set_pdev_id(2);
    sb.set_generation(100);

    s3_chunk_entry entry;
    entry.chunk_id = 1;
    entry.chunk_size = 2048;
    entry.chunk_type = S3ChunkType::INDEX;
    entry.vdev_id = 3;
    entry.set_s3_key("vol-002/chunks/1/data.dat");
    sb.add_chunk(entry);

    auto write_result = sb.write_to_s3(*m_s3_store, "vol-002");
    ASSERT_TRUE(write_result.ok()) << write_result.error_message;

    PdevS3Superblock sb2;
    auto read_result = sb2.read_from_s3(*m_s3_store, "vol-002");
    ASSERT_TRUE(read_result.ok()) << read_result.error_message;

    ASSERT_EQ(sb2.pdev_id(), 2u);
    ASSERT_EQ(sb2.generation(), 100u);
    ASSERT_EQ(sb2.num_chunks(), 1u);
    auto* c = sb2.find_chunk(1);
    ASSERT_NE(c, nullptr);
    ASSERT_EQ(c->get_s3_key(), "vol-002/chunks/1/data.dat");
}

TEST_F(PdevS3SuperblockTest, ReadFromS3NonexistentFails) {
    PdevS3Superblock sb;
    auto result = sb.read_from_s3(*m_s3_store, "nonexistent-volume");
    ASSERT_FALSE(result.ok());
}

TEST_F(PdevS3SuperblockTest, RemoveChunk) {
    PdevS3Superblock sb;

    s3_chunk_entry e1;
    e1.chunk_id = 1;
    sb.add_chunk(e1);

    s3_chunk_entry e2;
    e2.chunk_id = 2;
    sb.add_chunk(e2);

    ASSERT_EQ(sb.num_chunks(), 2u);
    ASSERT_TRUE(sb.remove_chunk(1));
    ASSERT_EQ(sb.num_chunks(), 1u);
    ASSERT_EQ(sb.find_chunk(1), nullptr);
    ASSERT_NE(sb.find_chunk(2), nullptr);

    ASSERT_FALSE(sb.remove_chunk(99));
}

TEST_F(PdevS3SuperblockTest, UpdateChunkKey) {
    PdevS3Superblock sb;

    s3_chunk_entry entry;
    entry.chunk_id = 10;
    entry.set_s3_key("old/key.dat");
    entry.generation = 1;
    sb.add_chunk(entry);

    ASSERT_TRUE(sb.update_chunk_key(10, "new/key_gen2.dat", 2));
    auto* c = sb.find_chunk(10);
    ASSERT_NE(c, nullptr);
    ASSERT_EQ(c->get_s3_key(), "new/key_gen2.dat");
    ASSERT_EQ(c->generation, 2u);

    ASSERT_FALSE(sb.update_chunk_key(99, "x.dat"));
}

TEST_F(PdevS3SuperblockTest, GetChunksByType) {
    PdevS3Superblock sb;

    s3_chunk_entry e1;
    e1.chunk_id = 1;
    e1.chunk_type = S3ChunkType::DATA;
    sb.add_chunk(e1);

    s3_chunk_entry e2;
    e2.chunk_id = 2;
    e2.chunk_type = S3ChunkType::METABLK;
    sb.add_chunk(e2);

    s3_chunk_entry e3;
    e3.chunk_id = 3;
    e3.chunk_type = S3ChunkType::DATA;
    sb.add_chunk(e3);

    s3_chunk_entry e4;
    e4.chunk_id = 4;
    e4.chunk_type = S3ChunkType::WAL;
    sb.add_chunk(e4);

    ASSERT_EQ(sb.get_chunks_by_type(S3ChunkType::DATA).size(), 2u);
    ASSERT_EQ(sb.get_chunks_by_type(S3ChunkType::METABLK).size(), 1u);
    ASSERT_EQ(sb.get_chunks_by_type(S3ChunkType::WAL).size(), 1u);
    ASSERT_EQ(sb.get_chunks_by_type(S3ChunkType::INDEX).size(), 0u);
}

TEST_F(PdevS3SuperblockTest, EmptySuperblockSerializesDeserializes) {
    PdevS3Superblock sb;
    auto buf = sb.serialize();
    ASSERT_NE(buf, nullptr);

    PdevS3Superblock sb2;
    ASSERT_TRUE(sb2.deserialize(buf));
    ASSERT_EQ(sb2.num_chunks(), 0u);
}

TEST_F(PdevS3SuperblockTest, S3KeyGeneration) {
    ASSERT_EQ(PdevS3Superblock::s3_key("vol-001"), "vol-001/pdev_superblock.bin");
}

TEST_F(PdevS3SuperblockTest, IncrementGeneration) {
    PdevS3Superblock sb;
    ASSERT_EQ(sb.generation(), 0u);
    sb.increment_generation();
    ASSERT_EQ(sb.generation(), 1u);
    sb.increment_generation();
    ASSERT_EQ(sb.generation(), 2u);
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_chunk_store");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
