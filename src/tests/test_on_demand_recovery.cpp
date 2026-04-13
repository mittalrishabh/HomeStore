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

#include <chrono>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>

#include <gtest/gtest.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/chunk_eviction_manager.h>
#include <homestore/s3/chunk_hydration_manager.h>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/on_demand_recovery.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/s3_recovery.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Test Helpers
///////////////////////////////////////////////////////////////////////////////
static sisl::byte_array make_patterned_data(uint32_t size, uint8_t seed = 0) {
    auto buf = sisl::make_byte_array(size, 0);
    for (uint32_t i = 0; i < size; ++i) {
        buf->bytes()[i] = static_cast< uint8_t >((seed + i) & 0xFF);
    }
    return buf;
}

static S3ObjectStoreConfig make_test_config() {
    S3ObjectStoreConfig cfg;
    cfg.bucket = "homestore-test-ondemand";
    cfg.region = "us-east-1";
    cfg.retry_count = 1;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

///////////////////////////////////////////////////////////////////////////////
// Mock NvmeRecoveryWriter
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
    bool has_chunk(chunk_id_t chunk_id) const { return m_written_chunks.count(chunk_id) > 0; }
    size_t written_count() const { return m_written_chunks.size(); }

private:
    NvmeState m_state{NvmeState::EMPTY};
    std::unordered_map< chunk_id_t, sisl::byte_array > m_written_chunks;
    std::set< chunk_id_t > m_fail_chunks;
};

///////////////////////////////////////////////////////////////////////////////
// Mock NvmeChunkReader (for FullChunkStore)
///////////////////////////////////////////////////////////////////////////////
class MockNvmeChunkReader : public NvmeChunkReader {
public:
    std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t, uint64_t) override {
        return {{.status_code = 404, .error_message = "Not on NVMe"}, {}};
    }
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
        m_on_nvme.erase(chunk_id);
        return {};
    }

private:
    std::set< chunk_id_t > m_on_nvme;
};

///////////////////////////////////////////////////////////////////////////////
// Mock NvmeDeviceIO (for ChunkHydrationManager)
///////////////////////////////////////////////////////////////////////////////
class MockNvmeDeviceIO : public NvmeDeviceIO {
public:
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
        auto key = std::to_string(chunk_id) + ":" + std::to_string(offset_in_chunk);
        auto b = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(b->bytes(), buf, size);
        m_storage[key] = std::move(b);
        m_on_nvme.insert(chunk_id);
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
// Mock NvmeChunkAllocator (for ChunkHydrationManager)
///////////////////////////////////////////////////////////////////////////////
class MockNvmeChunkAllocator : public NvmeChunkAllocator {
public:
    std::error_code allocate_nvme_chunk(chunk_id_t, uint64_t) override { return {}; }
    void release_nvme_chunk(chunk_id_t, uint64_t) override {}
    uint64_t free_nvme_space_bytes() const override { return 1ULL << 30; }
};

///////////////////////////////////////////////////////////////////////////////
// Helper: build a superblock with mixed chunk types and populate S3
///////////////////////////////////////////////////////////////////////////////
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

    add(1, 1024, S3ChunkType::METABLK);
    add(2, 2048, S3ChunkType::WAL);
    add(3, 1024, S3ChunkType::INDEX);
    add(10, 4096, S3ChunkType::DATA);
    add(11, 4096, S3ChunkType::DATA);
    add(12, 8192, S3ChunkType::DATA);

    return sb;
}

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

///////////////////////////////////////////////////////////////////////////////
// Test Fixture
///////////////////////////////////////////////////////////////////////////////
class OnDemandRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();
        m_nvme_writer = std::make_shared< MockNvmeRecoveryWriter >();
        m_key_mapper = S3KeyMapper{.volume_id = m_volume_id};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, m_key_mapper);

        m_nvme_mgr = std::make_shared< MockNvmeChunkManager >();
        m_nvme_io = std::make_shared< MockNvmeDeviceIO >();
        m_nvme_alloc = std::make_shared< MockNvmeChunkAllocator >();

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, m_volume_id, 1024, nullptr);

        m_eviction_mgr = std::make_shared< ChunkEvictionManager >(
            m_s3_pdev.get(), m_nvme_mgr);
    }

    std::unique_ptr< OnDemandRecoveryManager > make_manager(uint32_t retry_count = 2,
                                                             uint32_t retry_backoff_ms = 0) {
        return std::make_unique< OnDemandRecoveryManager >(
            m_chunk_store, m_s3_store, m_nvme_writer, m_eviction_mgr,
            m_volume_id, retry_count, retry_backoff_ms);
    }

    std::shared_ptr< ChunkHydrationManager > make_hydration_mgr() {
        return std::make_shared< ChunkHydrationManager >(
            m_chunk_store, m_nvme_io, m_nvme_alloc, m_eviction_mgr,
            nullptr, /*num_workers=*/1);
    }

    void setup_s3_data() {
        auto sb = make_test_superblock(m_volume_id);
        sb.write_to_s3(*m_s3_store, m_volume_id);
        populate_s3_with_chunks(*m_s3_store, sb);

        for (const auto& entry : sb.chunks()) {
            m_s3_pdev->create_chunk(entry.chunk_id, entry.chunk_size, entry.chunk_type, entry.vdev_id);
            m_nvme_mgr->set_on_nvme(entry.chunk_id, true);
        }
    }

    std::string m_volume_id{"vol-ondemand-test"};
    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< MockNvmeRecoveryWriter > m_nvme_writer;
    S3KeyMapper m_key_mapper;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::shared_ptr< MockNvmeChunkManager > m_nvme_mgr;
    std::shared_ptr< MockNvmeDeviceIO > m_nvme_io;
    std::shared_ptr< MockNvmeChunkAllocator > m_nvme_alloc;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;
};

///////////////////////////////////////////////////////////////////////////////
// Tests
///////////////////////////////////////////////////////////////////////////////

TEST_F(OnDemandRecoveryTest, NeedsRecoveryDelegatesToS3RecoveryManager) {
    m_nvme_writer->set_nvme_state(NvmeState::VALID);
    auto mgr = make_manager();
    ASSERT_FALSE(mgr->needs_recovery());
}

TEST_F(OnDemandRecoveryTest, NeedsRecoveryReturnsTrueWhenNvmeEmpty) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);

    auto mgr = make_manager();
    ASSERT_TRUE(mgr->needs_recovery());
}

TEST_F(OnDemandRecoveryTest, RecoverEssentialChunksSucceeds) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    setup_s3_data();

    auto mgr = make_manager();
    ASSERT_TRUE(mgr->needs_recovery());

    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success) << result.error_message;
    EXPECT_EQ(result.essential_chunks_total, 3u);
    EXPECT_EQ(result.essential_chunks_recovered, 3u);
    EXPECT_GT(result.essential_bytes, 0u);
    EXPECT_EQ(result.data_chunks_deferred, 3u);
    EXPECT_EQ(result.deferred_chunk_ids.size(), 3u);
    EXPECT_GT(result.elapsed.count(), 0);
}

TEST_F(OnDemandRecoveryTest, OnlyEssentialChunksWrittenToNvme) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    setup_s3_data();

    auto mgr = make_manager();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success);

    // Essential chunks written
    EXPECT_TRUE(m_nvme_writer->has_chunk(1));
    EXPECT_TRUE(m_nvme_writer->has_chunk(2));
    EXPECT_TRUE(m_nvme_writer->has_chunk(3));

    // Data chunks NOT written — this is the critical fix
    EXPECT_FALSE(m_nvme_writer->has_chunk(10));
    EXPECT_FALSE(m_nvme_writer->has_chunk(11));
    EXPECT_FALSE(m_nvme_writer->has_chunk(12));
    EXPECT_EQ(m_nvme_writer->written_count(), 3u);
}

TEST_F(OnDemandRecoveryTest, DataChunksMarkedAsEvicted) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    setup_s3_data();

    auto mgr = make_manager();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success);

    EXPECT_TRUE(m_eviction_mgr->is_chunk_evicted(10));
    EXPECT_TRUE(m_eviction_mgr->is_chunk_evicted(11));
    EXPECT_TRUE(m_eviction_mgr->is_chunk_evicted(12));
}

TEST_F(OnDemandRecoveryTest, DeferredRemainingTracksDataChunks) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    setup_s3_data();

    auto mgr = make_manager();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success);

    EXPECT_EQ(mgr->deferred_remaining(), 3u);
    EXPECT_EQ(mgr->hydrated_count(), 0u);
}

TEST_F(OnDemandRecoveryTest, EssentialChunkFailureAbortsRecovery) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    auto sb = make_test_superblock(m_volume_id);
    sb.write_to_s3(*m_s3_store, m_volume_id);

    // Populate only WAL and INDEX — METABLK missing will cause failure
    for (const auto& entry : sb.chunks()) {
        if (entry.chunk_type == S3ChunkType::METABLK) continue;
        auto data = make_patterned_data(static_cast< uint32_t >(entry.chunk_size));
        sisl::io_blob_safe blob(entry.chunk_size, 0);
        std::memcpy(blob.bytes(), data->cbytes(), entry.chunk_size);
        m_s3_store->put_object(entry.get_s3_key(), std::move(blob)).get();
    }

    auto mgr = make_manager(0, 0);
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();

    ASSERT_FALSE(result.success);
    EXPECT_FALSE(result.error_message.empty());
}

TEST_F(OnDemandRecoveryTest, EmptySuperblockRecoverySucceeds) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

    PdevS3Superblock sb;
    sb.set_pdev_id(1);
    sb.set_generation(1);
    sb.write_to_s3(*m_s3_store, m_volume_id);

    auto mgr = make_manager();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();

    ASSERT_TRUE(result.success);
    EXPECT_EQ(result.essential_chunks_total, 0u);
    EXPECT_EQ(result.data_chunks_deferred, 0u);
}

TEST_F(OnDemandRecoveryTest, SuperblockAccessibleAfterRecovery) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    setup_s3_data();

    auto mgr = make_manager();
    mgr->needs_recovery();

    EXPECT_EQ(mgr->superblock().generation(), 10u);
    EXPECT_EQ(mgr->superblock().num_chunks(), 6u);
}

TEST_F(OnDemandRecoveryTest, ProactiveHydrationHydratesDataChunks) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    setup_s3_data();

    auto mgr = make_manager();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success);
    ASSERT_EQ(mgr->deferred_remaining(), 3u);

    auto hydration_mgr = make_hydration_mgr();

    ProactiveHydrationConfig hydration_cfg{.interval_ms = 10, .batch_size = 2};
    mgr->start_proactive_hydration(hydration_mgr, hydration_cfg);

    for (int i = 0; i < 200 && mgr->deferred_remaining() > 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    mgr->stop_proactive_hydration();
    hydration_mgr->shutdown();

    EXPECT_EQ(mgr->deferred_remaining(), 0u);
    EXPECT_EQ(mgr->hydrated_count(), 3u);
    EXPECT_FALSE(mgr->is_hydration_running());
}

TEST_F(OnDemandRecoveryTest, ProactiveHydrationStopsCleanly) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    setup_s3_data();

    auto mgr = make_manager();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success);

    auto hydration_mgr = make_hydration_mgr();

    ProactiveHydrationConfig hydration_cfg{.interval_ms = 60000, .batch_size = 1};
    mgr->start_proactive_hydration(hydration_mgr, hydration_cfg);

    EXPECT_TRUE(mgr->is_hydration_running());

    mgr->stop_proactive_hydration();
    hydration_mgr->shutdown();
    EXPECT_FALSE(mgr->is_hydration_running());
}

TEST_F(OnDemandRecoveryTest, DoubleStartHydrationIsIdempotent) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    setup_s3_data();

    auto mgr = make_manager();
    mgr->needs_recovery();
    mgr->recover_essential_chunks();

    auto hydration_mgr = make_hydration_mgr();

    ProactiveHydrationConfig cfg{.interval_ms = 60000, .batch_size = 1};
    mgr->start_proactive_hydration(hydration_mgr, cfg);
    mgr->start_proactive_hydration(hydration_mgr, cfg); // no-op

    EXPECT_TRUE(mgr->is_hydration_running());
    mgr->stop_proactive_hydration();
    hydration_mgr->shutdown();
}

TEST_F(OnDemandRecoveryTest, RecoverWithoutPriorNeedsRecoveryCheck) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    setup_s3_data();

    auto mgr = make_manager();
    auto result = mgr->recover_essential_chunks();

    ASSERT_TRUE(result.success) << result.error_message;
}

TEST_F(OnDemandRecoveryTest, DeferredChunkIdsContainOnlyDataChunks) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    setup_s3_data();

    auto mgr = make_manager();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success);

    std::set< chunk_id_t > deferred(result.deferred_chunk_ids.begin(),
                                     result.deferred_chunk_ids.end());
    EXPECT_TRUE(deferred.count(10));
    EXPECT_TRUE(deferred.count(11));
    EXPECT_TRUE(deferred.count(12));
    EXPECT_FALSE(deferred.count(1));
    EXPECT_FALSE(deferred.count(2));
    EXPECT_FALSE(deferred.count(3));
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_on_demand_recovery");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
