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

/**
 * @file test_s3_integ_ondemand_recovery.cpp
 * @brief Integration tests — Group 4: On-Demand Recovery + Hydration
 *
 * Wires up REAL: OnDemandRecoveryManager, ChunkHydrationManager,
 *               ChunkEvictionManager, TieredReadHandler, FullChunkStore,
 *               S3PhysicalDev, S3RecoveryManager
 * Mocked:        S3 transport (MockS3ObjectStore), NVMe I/O
 */

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <set>
#include <thread>
#include <vector>

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
#include <homestore/s3/tiered_read_handler.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Mocks
///////////////////////////////////////////////////////////////////////////////

class MockNvmeRecoveryWriter : public NvmeRecoveryWriter {
public:
    void set_nvme_state(NvmeState state) { m_state = state; }

    NvmeState check_nvme_state() override { return m_state; }

    std::error_code write_chunk(chunk_id_t chunk_id, const sisl::byte_array& data) override {
        std::lock_guard lock{m_mtx};
        if (m_fail_chunks.count(chunk_id)) {
            return std::make_error_code(std::errc::io_error);
        }
        m_written_chunks[chunk_id] = data;
        return {};
    }

    void set_chunk_fail(chunk_id_t chunk_id) { m_fail_chunks.insert(chunk_id); }
    bool has_chunk(chunk_id_t chunk_id) const {
        std::lock_guard lock{m_mtx};
        return m_written_chunks.count(chunk_id) > 0;
    }
    size_t written_count() const {
        std::lock_guard lock{m_mtx};
        return m_written_chunks.size();
    }

private:
    NvmeState m_state{NvmeState::EMPTY};
    mutable std::mutex m_mtx;
    std::unordered_map< chunk_id_t, sisl::byte_array > m_written_chunks;
    std::set< chunk_id_t > m_fail_chunks;
};

class MockNvmeChunkReader : public NvmeChunkReader {
public:
    std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t, uint64_t) override {
        return {{.status_code = 404, .error_message = "Not on NVMe"}, {}};
    }
};

class MockNvmeChunkManager : public NvmeChunkManager {
public:
    void set_on_nvme(chunk_id_t chunk_id, bool on) {
        std::lock_guard lock{m_mtx};
        if (on) m_on_nvme.insert(chunk_id);
        else m_on_nvme.erase(chunk_id);
    }

    bool is_chunk_on_nvme(chunk_id_t chunk_id) const override {
        std::lock_guard lock{m_mtx};
        return m_on_nvme.count(chunk_id) > 0;
    }

    std::error_code release_nvme_chunk(chunk_id_t chunk_id) override {
        std::lock_guard lock{m_mtx};
        m_on_nvme.erase(chunk_id);
        return {};
    }

private:
    mutable std::mutex m_mtx;
    std::set< chunk_id_t > m_on_nvme;
};

class MockNvmeDeviceIO : public NvmeDeviceIO {
public:
    void set_on_nvme(chunk_id_t chunk_id, bool on) {
        std::lock_guard lock{m_mtx};
        if (on) m_on_nvme.insert(chunk_id);
        else m_on_nvme.erase(chunk_id);
    }

    std::error_code nvme_read(chunk_id_t chunk_id, uint64_t offset,
                              char* buf, uint64_t size) override {
        std::lock_guard lock{m_mtx};
        if (m_on_nvme.find(chunk_id) == m_on_nvme.end()) {
            return std::make_error_code(std::errc::no_such_device);
        }
        auto key = std::to_string(chunk_id) + ":" + std::to_string(offset);
        auto it = m_storage.find(key);
        if (it != m_storage.end()) {
            auto to_copy = std::min(size, static_cast< uint64_t >(it->second->size()));
            std::memcpy(buf, it->second->cbytes(), to_copy);
            return {};
        }
        return std::make_error_code(std::errc::no_such_file_or_directory);
    }

    std::error_code nvme_write(chunk_id_t chunk_id, uint64_t offset,
                               const char* buf, uint64_t size) override {
        std::lock_guard lock{m_mtx};
        auto key = std::to_string(chunk_id) + ":" + std::to_string(offset);
        auto b = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(b->bytes(), buf, size);
        m_storage[key] = std::move(b);
        m_on_nvme.insert(chunk_id);
        return {};
    }

    bool is_chunk_on_nvme(chunk_id_t chunk_id) const override {
        std::lock_guard lock{m_mtx};
        return m_on_nvme.count(chunk_id) > 0;
    }

private:
    mutable std::mutex m_mtx;
    std::unordered_map< std::string, sisl::byte_array > m_storage;
    std::set< chunk_id_t > m_on_nvme;
};

class MockNvmeChunkAllocator : public NvmeChunkAllocator {
public:
    std::error_code allocate_nvme_chunk(chunk_id_t, uint64_t) override { return {}; }
    void release_nvme_chunk(chunk_id_t, uint64_t) override {}
    uint64_t free_nvme_space_bytes() const override { return 1ULL << 30; }
};

///////////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////////

static sisl::byte_array make_patterned_data(uint32_t size, uint8_t seed) {
    auto buf = sisl::make_byte_array(size, 0);
    for (uint32_t i = 0; i < size; ++i) {
        buf->bytes()[i] = static_cast< uint8_t >((seed + i) & 0xFF);
    }
    return buf;
}

static S3ObjectStoreConfig make_test_config() {
    S3ObjectStoreConfig cfg;
    cfg.bucket = "homestore-integ-ondemand";
    cfg.region = "us-east-1";
    cfg.retry_count = 2;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

static void wait_for_condition(std::function< bool() > cond, int max_ms = 3000) {
    for (int elapsed = 0; elapsed < max_ms; elapsed += 10) {
        if (cond()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

///////////////////////////////////////////////////////////////////////////////
// Test Fixture
///////////////////////////////////////////////////////////////////////////////
class OnDemandRecoveryIntegTest : public ::testing::Test {
protected:
    static constexpr char VOLUME_ID[] = "vol-ondemand-integ";

    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();
        m_nvme_writer = std::make_shared< MockNvmeRecoveryWriter >();

        S3KeyMapper key_mapper{.volume_id = VOLUME_ID};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, key_mapper);

        m_nvme_mgr = std::make_shared< MockNvmeChunkManager >();
        m_nvme_io = std::make_shared< MockNvmeDeviceIO >();
        m_nvme_alloc = std::make_shared< MockNvmeChunkAllocator >();

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, VOLUME_ID, 1024, nullptr);

        m_eviction_mgr = std::make_shared< ChunkEvictionManager >(m_s3_pdev.get(), m_nvme_mgr);
    }

    PdevS3Superblock build_superblock() {
        PdevS3Superblock sb;
        sb.set_pdev_id(1);
        sb.set_generation(5);

        auto add = [&](uint64_t cid, uint64_t size, S3ChunkType type) {
            s3_chunk_entry e;
            e.chunk_id = cid;
            e.chunk_size = size;
            e.chunk_type = type;
            e.vdev_id = 1;
            S3KeyMapper mapper{VOLUME_ID};
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

    void populate_s3(const PdevS3Superblock& sb) {
        sb.write_to_s3(*m_s3_store, VOLUME_ID);

        for (const auto& entry : sb.chunks()) {
            auto data = make_patterned_data(static_cast< uint32_t >(entry.chunk_size),
                                            static_cast< uint8_t >(entry.chunk_id));
            sisl::io_blob_safe blob(entry.chunk_size, 0);
            std::memcpy(blob.bytes(), data->cbytes(), entry.chunk_size);
            m_s3_store->put_object(entry.get_s3_key(), std::move(blob)).get();

            m_s3_pdev->create_chunk(entry.chunk_id, entry.chunk_size, entry.chunk_type, entry.vdev_id);
            m_nvme_mgr->set_on_nvme(entry.chunk_id, true);
        }
    }

    std::unique_ptr< OnDemandRecoveryManager > make_recovery_mgr() {
        return std::make_unique< OnDemandRecoveryManager >(
            m_chunk_store, m_s3_store, m_nvme_writer, m_eviction_mgr,
            VOLUME_ID, 2, 0);
    }

    std::shared_ptr< ChunkHydrationManager > make_hydration_mgr() {
        return std::make_shared< ChunkHydrationManager >(
            m_chunk_store, m_nvme_io, m_nvme_alloc, m_eviction_mgr,
            nullptr, 2);
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< MockNvmeRecoveryWriter > m_nvme_writer;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::shared_ptr< MockNvmeChunkManager > m_nvme_mgr;
    std::shared_ptr< MockNvmeDeviceIO > m_nvme_io;
    std::shared_ptr< MockNvmeChunkAllocator > m_nvme_alloc;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;
};

///////////////////////////////////////////////////////////////////////////////
// Test 4.1 — Essential-only recovery: only METABLK/WAL/INDEX downloaded,
//             DATA chunks marked as evicted (S3-only)
///////////////////////////////////////////////////////////////////////////////
TEST_F(OnDemandRecoveryIntegTest, EssentialOnlyRecovery) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    auto sb = build_superblock();
    populate_s3(sb);

    auto mgr = make_recovery_mgr();
    ASSERT_TRUE(mgr->needs_recovery());

    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success) << result.error_message;
    EXPECT_EQ(result.essential_chunks_total, 3u);
    EXPECT_EQ(result.essential_chunks_recovered, 3u);
    EXPECT_EQ(result.data_chunks_deferred, 3u);

    EXPECT_TRUE(m_nvme_writer->has_chunk(1));
    EXPECT_TRUE(m_nvme_writer->has_chunk(2));
    EXPECT_TRUE(m_nvme_writer->has_chunk(3));
    EXPECT_FALSE(m_nvme_writer->has_chunk(10));
    EXPECT_FALSE(m_nvme_writer->has_chunk(11));
    EXPECT_FALSE(m_nvme_writer->has_chunk(12));

    EXPECT_TRUE(m_eviction_mgr->is_chunk_evicted(10));
    EXPECT_TRUE(m_eviction_mgr->is_chunk_evicted(11));
    EXPECT_TRUE(m_eviction_mgr->is_chunk_evicted(12));
}

///////////////////////////////////////////////////////////////////////////////
// Test 4.2 — Tiered read serves deferred data chunks from S3
//   After on-demand recovery, data chunks are S3-only.
//   TieredReadHandler should serve them from S3 via FullChunkStore.
///////////////////////////////////////////////////////////////////////////////
TEST_F(OnDemandRecoveryIntegTest, TieredReadServesDeferred) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    auto sb = build_superblock();
    populate_s3(sb);

    auto mgr = make_recovery_mgr();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success);

    TieredReadHandler handler{m_s3_pdev.get(), m_nvme_io,
                              {.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false}};

    for (auto cid : result.deferred_chunk_ids) {
        auto [ec, data] = handler.async_read(cid, 0, 64).get();
        ASSERT_FALSE(ec) << "Failed to read deferred chunk " << cid << ": " << ec.message();
        ASSERT_NE(data, nullptr);
        ASSERT_EQ(data->cbytes()[0], static_cast< uint8_t >(cid & 0xFF));
    }
}

///////////////////////////////////////////////////////////////////////////////
// Test 4.3 — Proactive hydration warms all data chunks to NVMe
///////////////////////////////////////////////////////////////////////////////
TEST_F(OnDemandRecoveryIntegTest, ProactiveHydrationWarmsAll) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    auto sb = build_superblock();
    populate_s3(sb);

    auto mgr = make_recovery_mgr();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success);
    ASSERT_EQ(mgr->deferred_remaining(), 3u);

    auto hydration_mgr = make_hydration_mgr();

    ProactiveHydrationConfig hydration_cfg{.interval_ms = 10, .batch_size = 4};
    mgr->start_proactive_hydration(hydration_mgr, hydration_cfg);

    wait_for_condition([&]() { return mgr->deferred_remaining() == 0; }, 5000);

    mgr->stop_proactive_hydration();
    hydration_mgr->shutdown();

    EXPECT_EQ(mgr->deferred_remaining(), 0u);
    EXPECT_EQ(mgr->hydrated_count(), 3u);

    for (auto cid : {10u, 11u, 12u}) {
        EXPECT_TRUE(m_nvme_io->is_chunk_on_nvme(cid))
            << "Chunk " << cid << " should be on NVMe after proactive hydration";
    }
}

///////////////////////////////////////////////////////////////////////////////
// Test 4.4 — Concurrent reads during on-demand recovery
//   While proactive hydration is running, tiered reads should still work.
///////////////////////////////////////////////////////////////////////////////
TEST_F(OnDemandRecoveryIntegTest, ConcurrentReadsDuringHydration) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    auto sb = build_superblock();
    populate_s3(sb);

    auto mgr = make_recovery_mgr();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success);

    auto hydration_mgr = make_hydration_mgr();

    TieredReadHandler handler{m_s3_pdev.get(), m_nvme_io,
                              {.s3_read_fallback_enabled = true, .s3_hydrate_on_read = true},
                              hydration_mgr};

    ProactiveHydrationConfig hydration_cfg{.interval_ms = 10, .batch_size = 1};
    mgr->start_proactive_hydration(hydration_mgr, hydration_cfg);

    std::atomic< int > success_count{0};
    std::vector< std::thread > threads;

    for (int i = 0; i < 6; ++i) {
        chunk_id_t cid = result.deferred_chunk_ids[i % result.deferred_chunk_ids.size()];
        threads.emplace_back([&handler, cid, &success_count]() {
            for (int j = 0; j < 3; ++j) {
                auto [ec, data] = handler.async_read(cid, 0, 64).get();
                if (!ec && data) success_count++;
            }
        });
    }

    for (auto& t : threads) { t.join(); }

    mgr->stop_proactive_hydration();
    hydration_mgr->shutdown();

    ASSERT_EQ(success_count.load(), 18) << "All concurrent reads should succeed";
}

///////////////////////////////////////////////////////////////////////////////
// Test 4.5 — After proactive hydration completes, reads go NVMe fast path
///////////////////////////////////////////////////////////////////////////////
TEST_F(OnDemandRecoveryIntegTest, ReadsGoNvmeAfterHydration) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    auto sb = build_superblock();
    populate_s3(sb);

    auto mgr = make_recovery_mgr();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();
    ASSERT_TRUE(result.success);

    auto hydration_mgr = make_hydration_mgr();
    ProactiveHydrationConfig hydration_cfg{.interval_ms = 10, .batch_size = 4};
    mgr->start_proactive_hydration(hydration_mgr, hydration_cfg);

    wait_for_condition([&]() { return mgr->deferred_remaining() == 0; }, 5000);
    mgr->stop_proactive_hydration();
    hydration_mgr->shutdown();

    TieredReadHandler handler{m_s3_pdev.get(), m_nvme_io,
                              {.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false}};

    auto gets_before = m_s3_store->total_gets();

    for (auto cid : {10u, 11u, 12u}) {
        auto [ec, data] = handler.async_read(cid, 0, 64).get();
        ASSERT_FALSE(ec);
        ASSERT_NE(data, nullptr);
    }

    ASSERT_EQ(m_s3_store->total_gets(), gets_before)
        << "After full hydration, all reads should hit NVMe";
}

///////////////////////////////////////////////////////////////////////////////
// Test 4.6 — Essential chunk failure aborts recovery before DATA download
///////////////////////////////////////////////////////////////////////////////
TEST_F(OnDemandRecoveryIntegTest, EssentialFailureAborts) {
    m_nvme_writer->set_nvme_state(NvmeState::EMPTY);
    auto sb = build_superblock();

    sb.write_to_s3(*m_s3_store, VOLUME_ID);
    for (const auto& entry : sb.chunks()) {
        if (entry.chunk_type == S3ChunkType::METABLK) continue;
        auto data = make_patterned_data(static_cast< uint32_t >(entry.chunk_size),
                                        static_cast< uint8_t >(entry.chunk_id));
        sisl::io_blob_safe blob(entry.chunk_size, 0);
        std::memcpy(blob.bytes(), data->cbytes(), entry.chunk_size);
        m_s3_store->put_object(entry.get_s3_key(), std::move(blob)).get();
    }

    auto mgr = make_recovery_mgr();
    mgr->needs_recovery();
    auto result = mgr->recover_essential_chunks();

    ASSERT_FALSE(result.success);
    EXPECT_FALSE(result.error_message.empty());
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_integ_ondemand_recovery");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
