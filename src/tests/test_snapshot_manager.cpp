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
#include <unordered_map>

#include <gtest/gtest.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/snapshot_manager.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Mock NvmeChunkReader
///////////////////////////////////////////////////////////////////////////////
class MockNvmeChunkReader : public NvmeChunkReader {
public:
    void set_chunk_data(chunk_id_t chunk_id, sisl::byte_array data) {
        m_chunks[chunk_id] = std::move(data);
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
// Helpers
///////////////////////////////////////////////////////////////////////////////
static sisl::byte_array make_test_data(uint32_t size, uint8_t pattern = 0xAB) {
    auto buf = sisl::make_byte_array(size, 0);
    std::memset(buf->bytes(), pattern, size);
    return buf;
}

static S3ObjectStoreConfig make_test_config() {
    S3ObjectStoreConfig cfg;
    cfg.bucket = "homestore-test-snap";
    cfg.region = "us-east-1";
    cfg.retry_count = 1;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

///////////////////////////////////////////////////////////////////////////////
// Test Fixture
///////////////////////////////////////////////////////////////////////////////
class SnapshotManagerTest : public ::testing::Test {
protected:
    static constexpr uint64_t CHUNK_SIZE = 4096;

    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();
        m_key_mapper = S3KeyMapper{.volume_id = m_volume_id};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, m_key_mapper);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, m_volume_id, 1024, nullptr);

        // Set up superblock with some chunks
        auto& sb = m_s3_pdev->superblock_mutable();
        sb.set_pdev_id(1);
        sb.set_generation(5);

        auto add_chunk = [&](chunk_id_t cid, S3ChunkType type) {
            s3_chunk_entry e;
            e.chunk_id = cid;
            e.chunk_size = CHUNK_SIZE;
            e.chunk_type = type;
            e.vdev_id = 1;
            e.set_s3_key(m_key_mapper.chunk_data_key(cid));
            sb.add_chunk(e);

            m_nvme_reader->set_chunk_data(cid, make_test_data(CHUNK_SIZE, static_cast< uint8_t >(cid)));
            m_s3_pdev->create_chunk(cid, CHUNK_SIZE, type, 1);
        };

        add_chunk(1, S3ChunkType::METABLK);
        add_chunk(2, S3ChunkType::WAL);
        add_chunk(10, S3ChunkType::DATA);
        add_chunk(11, S3ChunkType::DATA);

        m_cp_flush_success = true;
        auto cp_cb = [this]() -> bool { return m_cp_flush_success; };

        m_snap_mgr = std::make_unique< SnapshotManager >(
            m_s3_pdev.get(), m_chunk_store.get(), cp_cb);
    }

    std::string m_volume_id{"vol-snap-test"};
    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    S3KeyMapper m_key_mapper;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::unique_ptr< SnapshotManager > m_snap_mgr;
    bool m_cp_flush_success{true};
};

///////////////////////////////////////////////////////////////////////////////
// Tests
///////////////////////////////////////////////////////////////////////////////

TEST_F(SnapshotManagerTest, CreateSnapshotSucceeds) {
    auto result = m_snap_mgr->create_snapshot(100, 0xDEAD);

    ASSERT_EQ(result.status, SnapshotResult::SUCCESS);
    EXPECT_EQ(result.snap_id, 100u);
    EXPECT_EQ(result.generation, 5u);
    EXPECT_EQ(result.num_chunks_pinned, 4u);
}

TEST_F(SnapshotManagerTest, SnapshotRecordedInSuperblock) {
    m_snap_mgr->create_snapshot(100, 0xDEAD);

    const auto& sb = m_s3_pdev->superblock();
    ASSERT_EQ(sb.num_snapshots(), 1u);
    ASSERT_TRUE(sb.has_snapshots());

    auto* snap = sb.find_snapshot(100);
    ASSERT_NE(snap, nullptr);
    EXPECT_EQ(snap->header.snap_id, 100u);
    EXPECT_EQ(snap->header.generation, 5u);
    EXPECT_EQ(snap->header.btree_root_blkid, 0xDEADu);
    EXPECT_EQ(snap->chunk_keys.size(), 4u);
}

TEST_F(SnapshotManagerTest, SnapshotPinsCurrentS3Keys) {
    m_snap_mgr->create_snapshot(100);

    auto* snap = m_s3_pdev->superblock().find_snapshot(100);
    ASSERT_NE(snap, nullptr);

    // Pinned keys should be the original (no generation) keys
    for (const auto& ck : snap->chunk_keys) {
        auto expected = m_key_mapper.chunk_data_key(ck.chunk_id, 0);
        EXPECT_EQ(ck.get_s3_key(), expected)
            << "chunk_id=" << ck.chunk_id;
    }
}

TEST_F(SnapshotManagerTest, ChunkStoreGenerationSwitchedAfterSnapshot) {
    EXPECT_EQ(m_chunk_store->active_generation(), 0u);

    m_snap_mgr->create_snapshot(100);

    EXPECT_GT(m_chunk_store->active_generation(), 0u);
}

TEST_F(SnapshotManagerTest, ChunkEntriesUpdatedToNewGeneration) {
    m_snap_mgr->create_snapshot(100);

    auto next_gen = m_chunk_store->active_generation();
    const auto& sb = m_s3_pdev->superblock();

    for (const auto& chunk : sb.chunks()) {
        auto expected_key = m_key_mapper.chunk_data_key(chunk.chunk_id, next_gen);
        EXPECT_EQ(chunk.get_s3_key(), expected_key)
            << "chunk_id=" << chunk.chunk_id;
        EXPECT_EQ(chunk.generation, next_gen);
    }
}

TEST_F(SnapshotManagerTest, DuplicateSnapIdRejected) {
    m_snap_mgr->create_snapshot(100);
    auto result = m_snap_mgr->create_snapshot(100);

    EXPECT_EQ(result.status, SnapshotResult::DUPLICATE_SNAP_ID);
    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 1u);
}

TEST_F(SnapshotManagerTest, CpFlushFailureAbortsSnapshot) {
    m_cp_flush_success = false;
    auto result = m_snap_mgr->create_snapshot(200);

    EXPECT_EQ(result.status, SnapshotResult::CP_FLUSH_FAILED);
    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 0u);
    EXPECT_EQ(m_chunk_store->active_generation(), 0u);
}

TEST_F(SnapshotManagerTest, MultipleSnapshotsAccumulate) {
    m_snap_mgr->create_snapshot(100);
    auto result2 = m_snap_mgr->create_snapshot(200, 0xBEEF);

    ASSERT_EQ(result2.status, SnapshotResult::SUCCESS);

    const auto& sb = m_s3_pdev->superblock();
    EXPECT_EQ(sb.num_snapshots(), 2u);
    EXPECT_NE(sb.find_snapshot(100), nullptr);
    EXPECT_NE(sb.find_snapshot(200), nullptr);
}

TEST_F(SnapshotManagerTest, SecondSnapshotPinsGenerationStampedKeys) {
    m_snap_mgr->create_snapshot(100);

    auto gen_after_first = m_chunk_store->active_generation();

    m_snap_mgr->create_snapshot(200);

    auto* snap2 = m_s3_pdev->superblock().find_snapshot(200);
    ASSERT_NE(snap2, nullptr);

    // Second snapshot should pin the generation-stamped keys from after first snapshot
    for (const auto& ck : snap2->chunk_keys) {
        auto expected = m_key_mapper.chunk_data_key(ck.chunk_id, gen_after_first);
        EXPECT_EQ(ck.get_s3_key(), expected)
            << "chunk_id=" << ck.chunk_id;
    }
}

TEST_F(SnapshotManagerTest, DeleteSnapshotSucceeds) {
    m_snap_mgr->create_snapshot(100);
    ASSERT_EQ(m_s3_pdev->superblock().num_snapshots(), 1u);

    ASSERT_TRUE(m_snap_mgr->delete_snapshot(100));
    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 0u);
    EXPECT_EQ(m_s3_pdev->superblock().find_snapshot(100), nullptr);
}

TEST_F(SnapshotManagerTest, DeleteNonexistentSnapshotReturnsFalse) {
    EXPECT_FALSE(m_snap_mgr->delete_snapshot(999));
}

TEST_F(SnapshotManagerTest, DeleteLastSnapshotRevertsToOverwriteMode) {
    m_snap_mgr->create_snapshot(100);
    EXPECT_GT(m_chunk_store->active_generation(), 0u);

    m_snap_mgr->delete_snapshot(100);
    EXPECT_EQ(m_chunk_store->active_generation(), 0u);

    // Chunk entries should revert to non-generation keys
    for (const auto& chunk : m_s3_pdev->superblock().chunks()) {
        auto expected = m_key_mapper.chunk_data_key(chunk.chunk_id, 0);
        EXPECT_EQ(chunk.get_s3_key(), expected);
        EXPECT_EQ(chunk.generation, 0u);
    }
}

TEST_F(SnapshotManagerTest, DeleteOneOfTwoSnapshotsKeepsGenerationMode) {
    m_snap_mgr->create_snapshot(100);
    m_snap_mgr->create_snapshot(200);

    m_snap_mgr->delete_snapshot(100);
    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 1u);
    EXPECT_GT(m_chunk_store->active_generation(), 0u);
}

TEST_F(SnapshotManagerTest, SuperblockRoundTripWithSnapshots) {
    m_snap_mgr->create_snapshot(100, 0xCAFE);

    // Serialize → deserialize
    auto buf = m_s3_pdev->superblock().serialize();
    PdevS3Superblock deserialized;
    ASSERT_TRUE(deserialized.deserialize(buf));

    EXPECT_EQ(deserialized.num_snapshots(), 1u);
    auto* snap = deserialized.find_snapshot(100);
    ASSERT_NE(snap, nullptr);
    EXPECT_EQ(snap->header.snap_id, 100u);
    EXPECT_EQ(snap->header.generation, 5u);
    EXPECT_EQ(snap->header.btree_root_blkid, 0xCAFEu);
    EXPECT_EQ(snap->chunk_keys.size(), 4u);

    for (const auto& ck : snap->chunk_keys) {
        EXPECT_FALSE(ck.get_s3_key().empty());
    }
}

TEST_F(SnapshotManagerTest, SuperblockRoundTripMultipleSnapshots) {
    m_snap_mgr->create_snapshot(100, 0xAA);
    m_snap_mgr->create_snapshot(200, 0xBB);

    auto buf = m_s3_pdev->superblock().serialize();
    PdevS3Superblock deserialized;
    ASSERT_TRUE(deserialized.deserialize(buf));

    EXPECT_EQ(deserialized.num_snapshots(), 2u);
    EXPECT_NE(deserialized.find_snapshot(100), nullptr);
    EXPECT_NE(deserialized.find_snapshot(200), nullptr);
    EXPECT_EQ(deserialized.find_snapshot(100)->header.btree_root_blkid, 0xAAu);
    EXPECT_EQ(deserialized.find_snapshot(200)->header.btree_root_blkid, 0xBBu);
}

TEST_F(SnapshotManagerTest, V1SuperblockDeserializesWithZeroSnapshots) {
    // Simulate a v1 superblock (no snapshots field)
    PdevS3Superblock v1_sb;
    v1_sb.set_pdev_id(1);
    v1_sb.set_generation(3);

    s3_chunk_entry e;
    e.chunk_id = 1;
    e.chunk_size = 1024;
    e.chunk_type = S3ChunkType::DATA;
    e.set_s3_key("vol/chunks/1/data.dat");
    v1_sb.add_chunk(e);

    auto buf = v1_sb.serialize();

    PdevS3Superblock loaded;
    ASSERT_TRUE(loaded.deserialize(buf));
    EXPECT_EQ(loaded.num_chunks(), 1u);
    EXPECT_EQ(loaded.num_snapshots(), 0u);
    EXPECT_FALSE(loaded.has_snapshots());
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_snapshot_manager");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
