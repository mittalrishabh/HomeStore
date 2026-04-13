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
#include <system_error>
#include <unordered_map>

#include <gtest/gtest.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/chunk_store.h>
#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_recovery.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Test Helpers
///////////////////////////////////////////////////////////////////////////////
static sisl::byte_array make_test_data(uint32_t size, uint8_t pattern = 0xAB) {
    auto buf = sisl::make_byte_array(size, 0);
    std::memset(buf->bytes(), pattern, size);
    return buf;
}

static sisl::byte_array make_patterned_data(uint32_t size, uint8_t seed = 0) {
    auto buf = sisl::make_byte_array(size, 0);
    for (uint32_t i = 0; i < size; ++i) {
        buf->bytes()[i] = static_cast< uint8_t >((seed + i) & 0xFF);
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
// Mock NvmeRecoveryWriter — tracks writes and simulates NVMe state
///////////////////////////////////////////////////////////////////////////////
class MockNvmeRecoveryWriter : public NvmeRecoveryWriter {
public:
    void set_nvme_state(NvmeState state) { m_state = state; }

    NvmeState check_nvme_state() override { return m_state; }

    std::error_code write_chunk(chunk_id_t chunk_id, const sisl::byte_array& data) override {
        if (m_fail_chunks.count(chunk_id)) {
            return std::make_error_code(std::errc::io_error);
        }
        m_written_chunks[chunk_id] = data;
        return {};
    }

    void set_chunk_fail(chunk_id_t chunk_id) { m_fail_chunks.insert(chunk_id); }

    bool has_chunk(chunk_id_t chunk_id) const {
        return m_written_chunks.count(chunk_id) > 0;
    }

    const sisl::byte_array& get_chunk(chunk_id_t chunk_id) const {
        return m_written_chunks.at(chunk_id);
    }

    size_t written_count() const { return m_written_chunks.size(); }

private:
    NvmeState m_state{NvmeState::EMPTY};
    std::unordered_map< chunk_id_t, sisl::byte_array > m_written_chunks;
    std::set< chunk_id_t > m_fail_chunks;
};

///////////////////////////////////////////////////////////////////////////////
// Mock NvmeChunkReader — for FullChunkStore construction
///////////////////////////////////////////////////////////////////////////////
class MockNvmeChunkReader : public NvmeChunkReader {
public:
    std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t, uint64_t) override {
        S3Result r;
        r.status_code = 404;
        r.error_message = "NVMe is empty (recovery scenario)";
        return {r, {}};
    }
};

///////////////////////////////////////////////////////////////////////////////
// Helper: populate S3 with chunk data matching a superblock
///////////////////////////////////////////////////////////////////////////////
static void populate_s3_with_chunks(MockS3ObjectStore& s3_store,
                                     const PdevS3Superblock& sb,
                                     uint8_t base_pattern = 0) {
    for (const auto& entry : sb.chunks()) {
        auto data = make_patterned_data(static_cast< uint32_t >(entry.chunk_size),
                                        static_cast< uint8_t >(entry.chunk_id + base_pattern));
        sisl::io_blob_safe blob(entry.chunk_size, 0);
        std::memcpy(blob.bytes(), data->cbytes(), entry.chunk_size);
        s3_store.put_object(entry.get_s3_key(), std::move(blob)).get();
    }
}

static PdevS3Superblock make_test_superblock(const std::string& volume_id) {
    PdevS3Superblock sb;
    sb.set_pdev_id(1);
    sb.set_generation(10);

    auto add = [&](uint64_t cid, uint64_t size, S3ChunkType type) {
        s3_chunk_entry e;
        e.chunk_id = cid;
        e.chunk_size = size;
        e.chunk_type = type;
        e.vdev_id = 1;
        S3KeyMapper mapper{volume_id};
        e.set_s3_key(mapper.chunk_data_key(static_cast< chunk_id_t >(cid)));
        sb.add_chunk(e);
    };

    // Essential chunks (small)
    add(1, 1024, S3ChunkType::METABLK);
    add(2, 2048, S3ChunkType::WAL);
    add(3, 1024, S3ChunkType::INDEX);

    // Data chunks (larger)
    add(10, 4096, S3ChunkType::DATA);
    add(11, 4096, S3ChunkType::DATA);
    add(12, 8192, S3ChunkType::DATA);

    return sb;
}

///////////////////////////////////////////////////////////////////////////////
// S3RecoveryManager Tests
///////////////////////////////////////////////////////////////////////////////
class S3RecoveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();
        m_nvme_writer = std::make_shared< MockNvmeRecoveryWriter >();
        m_key_mapper = S3KeyMapper{.volume_id = m_volume_id};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, m_key_mapper);
    }

    std::unique_ptr< S3RecoveryManager > make_manager(uint32_t concurrency = 2,
                                                       uint32_t retry_count = 2,
                                                       uint32_t retry_backoff_ms = 0) {
        return std::make_unique< S3RecoveryManager >(
            m_chunk_store, m_s3_store, m_nvme_writer, m_volume_id,
            concurrency, retry_count, retry_backoff_ms);
    }

    std::string m_volume_id{"vol-recovery-test"};
    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< MockNvmeRecoveryWriter > m_nvme_writer;
    S3KeyMapper m_key_mapper;
    std::shared_ptr< FullChunkStore > m_chunk_store;
};

TEST_F(S3RecoveryTest, NeedsRecoveryReturnsFalseWhenNvmeValid) {
    m_nvme_writer->set_nvme_state(NvmeState::VALID);
    auto mgr = make_manager();
    ASSERT_FALSE(mgr->needs_recovery());
}

TEST_F(S3RecoveryTest, NeedsRecoveryReturnsFalseWhenNoSuperblockOnS3) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    auto mgr = make_manager();
    ASSERT_FALSE(mgr->needs_recovery());
}

TEST_F(S3RecoveryTest, NeedsRecoveryReturnsTrueWhenNvmeEmptyAndS3HasData) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);

    auto mgr = make_manager();
    ASSERT_TRUE(mgr->needs_recovery());
}

TEST_F(S3RecoveryTest, NeedsRecoveryReturnsTrueWhenNvmeCorrupted) {
    m_nvme_writer->set_nvme_state(NvmeState::CORRUPTED);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);

    auto mgr = make_manager();
    ASSERT_TRUE(mgr->needs_recovery());
}

TEST_F(S3RecoveryTest, RecoverFromS3SuccessfullyRecoversAllChunks) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);
    populate_s3_with_chunks(*m_s3_store, sb);

    auto mgr = make_manager();
    ASSERT_TRUE(mgr->needs_recovery());

    auto result = mgr->recover_from_s3();
    ASSERT_TRUE(result.success) << result.error_message;
    ASSERT_EQ(result.total_chunks, 6u);
    ASSERT_EQ(result.chunks_recovered, 6u);
    ASSERT_EQ(result.chunks_failed, 0u);
    ASSERT_GT(result.total_bytes, 0u);
    ASSERT_GT(result.elapsed.count(), 0);

    // Verify all chunks were written to NVMe
    ASSERT_EQ(m_nvme_writer->written_count(), 6u);
    ASSERT_TRUE(m_nvme_writer->has_chunk(1));
    ASSERT_TRUE(m_nvme_writer->has_chunk(2));
    ASSERT_TRUE(m_nvme_writer->has_chunk(3));
    ASSERT_TRUE(m_nvme_writer->has_chunk(10));
    ASSERT_TRUE(m_nvme_writer->has_chunk(11));
    ASSERT_TRUE(m_nvme_writer->has_chunk(12));
}

TEST_F(S3RecoveryTest, RecoverVerifiesChunkDataIntegrity) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);
    populate_s3_with_chunks(*m_s3_store, sb);

    auto mgr = make_manager();
    mgr->needs_recovery();
    auto result = mgr->recover_from_s3();
    ASSERT_TRUE(result.success);

    // Verify data content for a specific chunk
    const auto& written = m_nvme_writer->get_chunk(10);
    ASSERT_EQ(written->size(), 4096u);
    for (uint32_t i = 0; i < 4096; ++i) {
        EXPECT_EQ(written->cbytes()[i], static_cast< uint8_t >((10 + i) & 0xFF))
            << "Data mismatch at byte " << i << " of chunk 10";
    }
}

TEST_F(S3RecoveryTest, RecoverRespectsTypeOrdering) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);
    populate_s3_with_chunks(*m_s3_store, sb);

    auto mgr = make_manager();
    mgr->needs_recovery();
    auto result = mgr->recover_from_s3();
    ASSERT_TRUE(result.success);

    // Verify ordering: METABLK → WAL → INDEX → DATA
    auto idx_metablk = std::find_if(result.chunk_results.begin(), result.chunk_results.end(),
                                     [](const auto& r) { return r.chunk_type == S3ChunkType::METABLK; });
    auto idx_wal = std::find_if(result.chunk_results.begin(), result.chunk_results.end(),
                                 [](const auto& r) { return r.chunk_type == S3ChunkType::WAL; });
    auto idx_index = std::find_if(result.chunk_results.begin(), result.chunk_results.end(),
                                   [](const auto& r) { return r.chunk_type == S3ChunkType::INDEX; });
    auto idx_data = std::find_if(result.chunk_results.begin(), result.chunk_results.end(),
                                  [](const auto& r) { return r.chunk_type == S3ChunkType::DATA; });

    ASSERT_NE(idx_metablk, result.chunk_results.end());
    ASSERT_NE(idx_wal, result.chunk_results.end());
    ASSERT_NE(idx_index, result.chunk_results.end());
    ASSERT_NE(idx_data, result.chunk_results.end());

    // All essential chunks should come before any data chunk
    auto data_start = std::distance(result.chunk_results.begin(), idx_data);
    auto metablk_pos = std::distance(result.chunk_results.begin(), idx_metablk);
    auto wal_pos = std::distance(result.chunk_results.begin(), idx_wal);
    auto index_pos = std::distance(result.chunk_results.begin(), idx_index);

    EXPECT_LT(metablk_pos, data_start);
    EXPECT_LT(wal_pos, data_start);
    EXPECT_LT(index_pos, data_start);
}

TEST_F(S3RecoveryTest, RecoverAbortsOnEssentialChunkFailure) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);

    // Populate all EXCEPT the METABLK chunk — so it fails
    for (const auto& entry : sb.chunks()) {
        if (entry.chunk_type == S3ChunkType::METABLK) continue;
        auto data = make_patterned_data(static_cast< uint32_t >(entry.chunk_size));
        sisl::io_blob_safe blob(entry.chunk_size, 0);
        std::memcpy(blob.bytes(), data->cbytes(), entry.chunk_size);
        m_s3_store->put_object(entry.get_s3_key(), std::move(blob)).get();
    }

    auto mgr = make_manager(2, 0, 0); // 0 retries for speed
    mgr->needs_recovery();
    auto result = mgr->recover_from_s3();

    ASSERT_FALSE(result.success);
    ASSERT_GT(result.chunks_failed, 0u);
    // Should abort before attempting data chunks
    EXPECT_LT(result.chunks_recovered + result.chunks_failed, result.total_chunks);
}

TEST_F(S3RecoveryTest, RecoverHandlesNvmeWriteFailure) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);
    populate_s3_with_chunks(*m_s3_store, sb);

    // Make NVMe write fail for data chunk 10
    m_nvme_writer->set_chunk_fail(10);

    auto mgr = make_manager(2, 0, 0);
    mgr->needs_recovery();
    auto result = mgr->recover_from_s3();

    ASSERT_FALSE(result.success);
    ASSERT_EQ(result.chunks_failed, 1u);
    // Other chunks should still succeed
    ASSERT_EQ(result.chunks_recovered, 5u);
}

TEST_F(S3RecoveryTest, RecoverWithEmptySuperblockSucceeds) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    PdevS3Superblock sb;
    sb.set_pdev_id(1);
    sb.set_generation(1);
    sb.write_to_s3(*m_s3_store, m_volume_id);

    auto mgr = make_manager();
    mgr->needs_recovery();
    auto result = mgr->recover_from_s3();

    ASSERT_TRUE(result.success);
    ASSERT_EQ(result.total_chunks, 0u);
    ASSERT_EQ(result.chunks_recovered, 0u);
}

TEST_F(S3RecoveryTest, RecoverWithoutPriorNeedsRecoveryCheck) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);
    populate_s3_with_chunks(*m_s3_store, sb);

    // Call recover_from_s3() directly without needs_recovery()
    auto mgr = make_manager();
    auto result = mgr->recover_from_s3();

    ASSERT_TRUE(result.success);
    ASSERT_EQ(result.chunks_recovered, 6u);
}

TEST_F(S3RecoveryTest, RecoverFailsWithoutS3Superblock) {
    auto mgr = make_manager();
    auto result = mgr->recover_from_s3();

    ASSERT_FALSE(result.success);
    ASSERT_FALSE(result.error_message.empty());
}

TEST_F(S3RecoveryTest, RecoverWithHighConcurrency) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    PdevS3Superblock sb;
    sb.set_pdev_id(1);
    sb.set_generation(5);

    S3KeyMapper mapper{m_volume_id};
    for (uint64_t cid = 100; cid < 120; ++cid) {
        s3_chunk_entry e;
        e.chunk_id = cid;
        e.chunk_size = 2048;
        e.chunk_type = S3ChunkType::DATA;
        e.vdev_id = 1;
        e.set_s3_key(mapper.chunk_data_key(static_cast< chunk_id_t >(cid)));
        sb.add_chunk(e);
    }

    sb.write_to_s3(*m_s3_store, m_volume_id);
    populate_s3_with_chunks(*m_s3_store, sb);

    auto mgr = make_manager(/*concurrency=*/8, /*retry_count=*/1, /*retry_backoff_ms=*/0);
    auto result = mgr->recover_from_s3();

    ASSERT_TRUE(result.success) << result.error_message;
    ASSERT_EQ(result.chunks_recovered, 20u);
    ASSERT_EQ(m_nvme_writer->written_count(), 20u);
}

TEST_F(S3RecoveryTest, RecoverSizeMismatchTriggersRetry) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    PdevS3Superblock sb;
    sb.set_pdev_id(1);
    sb.set_generation(1);

    s3_chunk_entry entry;
    entry.chunk_id = 50;
    entry.chunk_size = 4096;
    entry.chunk_type = S3ChunkType::DATA;
    entry.vdev_id = 1;
    S3KeyMapper mapper{m_volume_id};
    entry.set_s3_key(mapper.chunk_data_key(50));
    sb.add_chunk(entry);

    sb.write_to_s3(*m_s3_store, m_volume_id);

    // Upload data with wrong size (smaller than expected)
    auto small_data = make_test_data(2048, 0xDD);
    sisl::io_blob_safe blob(2048, 0);
    std::memcpy(blob.bytes(), small_data->cbytes(), 2048);
    m_s3_store->put_object(mapper.chunk_data_key(50), std::move(blob)).get();

    auto mgr = make_manager(1, 1, 0);
    auto result = mgr->recover_from_s3();

    // Should fail — size mismatch is persistent
    ASSERT_FALSE(result.success);
    ASSERT_EQ(result.chunks_failed, 1u);
}

TEST_F(S3RecoveryTest, SuperblockAccessibleAfterRecovery) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);
    populate_s3_with_chunks(*m_s3_store, sb);

    auto mgr = make_manager();
    mgr->needs_recovery();

    ASSERT_EQ(mgr->superblock().generation(), 10u);
    ASSERT_EQ(mgr->superblock().num_chunks(), 6u);
    ASSERT_EQ(mgr->superblock().pdev_id(), 1u);
}

TEST_F(S3RecoveryTest, RecoverResultContainsPerChunkDetails) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);
    populate_s3_with_chunks(*m_s3_store, sb);

    auto mgr = make_manager();
    auto result = mgr->recover_from_s3();

    ASSERT_TRUE(result.success);
    ASSERT_EQ(result.chunk_results.size(), 6u);

    for (const auto& cr : result.chunk_results) {
        EXPECT_TRUE(cr.success) << "chunk_id=" << cr.chunk_id << " failed: " << cr.error_message;
        EXPECT_GT(cr.bytes_downloaded, 0u);
        EXPECT_TRUE(cr.error_message.empty());
    }
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_recovery");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
