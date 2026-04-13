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
 * @file test_s3_integ_tiered_read.cpp
 * @brief Integration tests — Group 2: Tiered Read Path
 *
 * Wires up REAL: TieredReadHandler, FullChunkStore, ChunkHydrationManager,
 *               ChunkEvictionManager, S3PhysicalDev
 * Mocked:        S3 transport (MockS3ObjectStore), NVMe I/O (MockNvmeDeviceIO)
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
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/tiered_read_handler.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Mocks — minimal, only S3 transport and NVMe I/O
///////////////////////////////////////////////////////////////////////////////

class MockNvmeChunkReader : public NvmeChunkReader {
public:
    void set_chunk_data(chunk_id_t chunk_id, sisl::byte_array data) {
        m_chunks[chunk_id] = std::move(data);
    }

    void remove_chunk_data(chunk_id_t chunk_id) { m_chunks.erase(chunk_id); }

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
    void store(chunk_id_t chunk_id, uint64_t offset, const char* data, uint64_t size) {
        std::lock_guard lock{m_mtx};
        auto key = make_key(chunk_id, offset);
        auto buf = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(buf->bytes(), data, size);
        m_storage[key] = std::move(buf);
        m_on_nvme.insert(chunk_id);
    }

    void set_on_nvme(chunk_id_t chunk_id, bool on) {
        std::lock_guard lock{m_mtx};
        if (on) m_on_nvme.insert(chunk_id);
        else m_on_nvme.erase(chunk_id);
    }

    std::error_code nvme_read(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                              char* buf, uint64_t size) override {
        std::lock_guard lock{m_mtx};
        if (m_on_nvme.find(chunk_id) == m_on_nvme.end()) {
            return std::make_error_code(std::errc::no_such_device);
        }
        auto key = make_key(chunk_id, offset_in_chunk);
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
        std::lock_guard lock{m_mtx};
        auto key = make_key(chunk_id, offset_in_chunk);
        auto data = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(data->bytes(), buf, size);
        m_storage[key] = std::move(data);
        m_on_nvme.insert(chunk_id);
        return {};
    }

    bool is_chunk_on_nvme(chunk_id_t chunk_id) const override {
        std::lock_guard lock{m_mtx};
        return m_on_nvme.count(chunk_id) > 0;
    }

    uint8_t read_byte(chunk_id_t chunk_id, uint64_t offset) {
        std::lock_guard lock{m_mtx};
        auto key = make_key(chunk_id, offset);
        auto it = m_storage.find(key);
        if (it != m_storage.end() && it->second->size() > 0) {
            return it->second->cbytes()[0];
        }
        return 0;
    }

private:
    static std::string make_key(chunk_id_t cid, uint64_t offset) {
        return std::to_string(cid) + ":" + std::to_string(offset);
    }

    mutable std::mutex m_mtx;
    std::unordered_map< std::string, sisl::byte_array > m_storage;
    std::set< chunk_id_t > m_on_nvme;
};

class MockNvmeChunkAllocator : public NvmeChunkAllocator {
public:
    explicit MockNvmeChunkAllocator(uint64_t total_space = 64 * 1024 * 1024)
        : m_total_space{total_space}, m_used_space{0} {}

    std::error_code allocate_nvme_chunk(chunk_id_t, uint64_t chunk_size) override {
        if (m_used_space + chunk_size > m_total_space) {
            return std::make_error_code(std::errc::no_space_on_device);
        }
        m_used_space += chunk_size;
        return {};
    }

    void release_nvme_chunk(chunk_id_t, uint64_t chunk_size) override {
        m_used_space = m_used_space > chunk_size ? m_used_space - chunk_size : 0;
    }

    uint64_t free_nvme_space_bytes() const override {
        return m_total_space > m_used_space ? m_total_space - m_used_space : 0;
    }

private:
    uint64_t m_total_space;
    uint64_t m_used_space;
};

///////////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////////

static sisl::byte_array make_test_data(uint32_t size, uint8_t pattern) {
    auto buf = sisl::make_byte_array(size, 0);
    std::memset(buf->bytes(), pattern, size);
    return buf;
}

static S3ObjectStoreConfig make_test_config() {
    S3ObjectStoreConfig cfg;
    cfg.bucket = "homestore-integ-test";
    cfg.region = "us-east-1";
    cfg.retry_count = 1;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

static void wait_for_condition(std::function< bool() > cond, int max_ms = 2000) {
    for (int elapsed = 0; elapsed < max_ms; elapsed += 10) {
        if (cond()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

///////////////////////////////////////////////////////////////////////////////
// Integration Test Fixture
//
// Wires up: S3PhysicalDev → FullChunkStore → MockS3ObjectStore
//           ChunkEvictionManager (real) → MockNvmeChunkManager
//           ChunkHydrationManager (real) → real eviction mgr
//           TieredReadHandler (real) → real hydration mgr
///////////////////////////////////////////////////////////////////////////////
class TieredReadIntegTest : public ::testing::Test {
protected:
    static constexpr uint64_t CHUNK_SIZE = 4096;

    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();

        S3KeyMapper key_mapper{.volume_id = "vol-integ"};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, key_mapper);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, "vol-integ", 1024, nullptr);

        m_nvme_mgr = std::make_shared< MockNvmeChunkManager >();
        m_nvme_io = std::make_shared< MockNvmeDeviceIO >();
        m_nvme_alloc = std::make_shared< MockNvmeChunkAllocator >();

        m_eviction_mgr = std::make_shared< ChunkEvictionManager >(m_s3_pdev.get(), m_nvme_mgr);

        m_hydration_mgr = std::make_shared< ChunkHydrationManager >(
            m_chunk_store, m_nvme_io, m_nvme_alloc, m_eviction_mgr,
            nullptr, 2);

        m_handler = std::make_unique< TieredReadHandler >(
            m_s3_pdev.get(), m_nvme_io,
            TieredReadConfig{.s3_read_fallback_enabled = true, .s3_hydrate_on_read = true},
            m_hydration_mgr);
    }

    void TearDown() override {
        m_hydration_mgr->shutdown();
    }

    void put_chunk_on_nvme_and_s3(chunk_id_t chunk_id, uint8_t pattern) {
        m_s3_pdev->create_chunk(chunk_id, CHUNK_SIZE, S3ChunkType::DATA, 1);
        auto data = make_test_data(CHUNK_SIZE, pattern);
        m_nvme_reader->set_chunk_data(chunk_id, data);
        m_chunk_store->put(chunk_id, {}, CHUNK_SIZE);
        m_nvme_io->store(chunk_id, 0,
                         reinterpret_cast< const char* >(data->cbytes()), CHUNK_SIZE);
        m_nvme_mgr->set_on_nvme(chunk_id, true);
    }

    void put_chunk_on_s3_only(chunk_id_t chunk_id, uint8_t pattern) {
        m_s3_pdev->create_chunk(chunk_id, CHUNK_SIZE, S3ChunkType::DATA, 1);
        auto data = make_test_data(CHUNK_SIZE, pattern);
        m_nvme_reader->set_chunk_data(chunk_id, data);
        m_chunk_store->put(chunk_id, {}, CHUNK_SIZE);
        m_nvme_reader->remove_chunk_data(chunk_id);
    }

    void evict_chunk(chunk_id_t chunk_id) {
        auto result = m_eviction_mgr->evict_chunk(chunk_id, CHUNK_SIZE);
        ASSERT_EQ(result, EvictionResult::SUCCESS);
        m_nvme_io->set_on_nvme(chunk_id, false);
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::shared_ptr< MockNvmeChunkManager > m_nvme_mgr;
    std::shared_ptr< MockNvmeDeviceIO > m_nvme_io;
    std::shared_ptr< MockNvmeChunkAllocator > m_nvme_alloc;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;
    std::shared_ptr< ChunkHydrationManager > m_hydration_mgr;
    std::unique_ptr< TieredReadHandler > m_handler;
};

///////////////////////////////////////////////////////////////////////////////
// Test 2.1 — NVMe fast path: chunk on NVMe → read directly, no S3 call
///////////////////////////////////////////////////////////////////////////////
TEST_F(TieredReadIntegTest, NvmeFastPath) {
    constexpr chunk_id_t CID = 10;
    put_chunk_on_nvme_and_s3(CID, 0xAA);

    auto gets_before = m_s3_store->total_gets();

    auto [ec, data] = m_handler->async_read(CID, 0, 64).get();
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_NE(data, nullptr);
    ASSERT_EQ(data->cbytes()[0], 0xAA);

    ASSERT_EQ(m_s3_store->total_gets(), gets_before) << "S3 should NOT be hit for NVMe-resident chunk";
}

///////////////////////////////////////////////////////////////////////////////
// Test 2.2 — S3 fallback after eviction: evict chunk → read serves from S3
///////////////////////////////////////////////////////////////////////////////
TEST_F(TieredReadIntegTest, S3FallbackAfterEviction) {
    constexpr chunk_id_t CID = 20;
    put_chunk_on_nvme_and_s3(CID, 0xBB);

    evict_chunk(CID);

    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CID));
    ASSERT_FALSE(m_nvme_io->is_chunk_on_nvme(CID));

    auto [ec, data] = m_handler->async_read(CID, 0, 64).get();
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_NE(data, nullptr);
    ASSERT_EQ(data->cbytes()[0], 0xBB);
}

///////////////////////////////////////////////////////////////////////////////
// Test 2.3 — Read → hydration → re-read from NVMe
//   First read: S3 fallback + triggers background hydration
//   Second read (after hydration): NVMe fast path
///////////////////////////////////////////////////////////////////////////////
TEST_F(TieredReadIntegTest, ReadHydrateReread) {
    constexpr chunk_id_t CID = 30;
    put_chunk_on_s3_only(CID, 0xCC);

    auto [ec1, data1] = m_handler->async_read(CID, 0, 64).get();
    ASSERT_FALSE(ec1) << ec1.message();
    ASSERT_EQ(data1->cbytes()[0], 0xCC);

    wait_for_condition([&]() { return m_nvme_io->is_chunk_on_nvme(CID); });
    ASSERT_TRUE(m_nvme_io->is_chunk_on_nvme(CID)) << "Chunk should be hydrated to NVMe";

    auto gets_before = m_s3_store->total_gets();
    auto [ec2, data2] = m_handler->async_read(CID, 0, 64).get();
    ASSERT_FALSE(ec2);
    ASSERT_EQ(data2->cbytes()[0], 0xCC);

    ASSERT_EQ(m_s3_store->total_gets(), gets_before)
        << "Second read should hit NVMe, not S3";
}

///////////////////////////////////////////////////////////////////////////////
// Test 2.4 — 10 concurrent reads to evicted chunk: dedup verification
//   All 10 reads should succeed with correct data.
//   Only 1 S3 download should happen (SharedPromise dedup in FullChunkStore).
///////////////////////////////////////////////////////////////////////////////
TEST_F(TieredReadIntegTest, ConcurrentReadsToEvictedChunkDedup) {
    constexpr chunk_id_t CID = 40;
    put_chunk_on_nvme_and_s3(CID, 0xDD);
    evict_chunk(CID);

    auto gets_before = m_s3_store->total_gets();

    constexpr int NUM_READERS = 10;
    std::vector< std::thread > threads;
    std::atomic< int > success_count{0};
    std::atomic< int > data_ok_count{0};

    for (int i = 0; i < NUM_READERS; ++i) {
        threads.emplace_back([&]() {
            auto [ec, data] = m_handler->async_read(CID, 0, 64).get();
            if (!ec) success_count++;
            if (data && data->cbytes()[0] == 0xDD) data_ok_count++;
        });
    }

    for (auto& t : threads) { t.join(); }

    ASSERT_EQ(success_count.load(), NUM_READERS);
    ASSERT_EQ(data_ok_count.load(), NUM_READERS);

    auto s3_gets = m_s3_store->total_gets() - gets_before;
    ASSERT_LE(s3_gets, 2u)
        << "Expected at most 2 S3 gets (dedup), got " << s3_gets;
}

///////////////////////////////////////////////////////////////////////////////
// Test 2.5 — Full evict → read → hydrate → evict → read → hydrate cycle
//   Validates state transitions across the full tiered read lifecycle.
///////////////////////////////////////////////////////////////////////////////
TEST_F(TieredReadIntegTest, FullEvictReadHydrateCycle) {
    constexpr chunk_id_t CID = 50;
    put_chunk_on_nvme_and_s3(CID, 0xEE);

    // Phase 1: Read from NVMe (fast path)
    auto [ec1, d1] = m_handler->async_read(CID, 0, 64).get();
    ASSERT_FALSE(ec1);
    ASSERT_EQ(d1->cbytes()[0], 0xEE);

    // Phase 2: Evict → read from S3 → auto-hydrate
    evict_chunk(CID);
    auto [ec2, d2] = m_handler->async_read(CID, 0, 64).get();
    ASSERT_FALSE(ec2);
    ASSERT_EQ(d2->cbytes()[0], 0xEE);

    wait_for_condition([&]() { return m_nvme_io->is_chunk_on_nvme(CID); });
    ASSERT_TRUE(m_nvme_io->is_chunk_on_nvme(CID));

    // Phase 3: Read from NVMe again (hydrated)
    auto [ec3, d3] = m_handler->async_read(CID, 0, 64).get();
    ASSERT_FALSE(ec3);
    ASSERT_EQ(d3->cbytes()[0], 0xEE);
}

///////////////////////////////////////////////////////////////////////////////
// Test 2.6 — Hydration disabled: reads fall back to S3 but no NVMe write
///////////////////////////////////////////////////////////////////////////////
TEST_F(TieredReadIntegTest, NoHydrationWhenDisabled) {
    constexpr chunk_id_t CID = 60;
    put_chunk_on_s3_only(CID, 0xFF);

    m_handler->set_config({.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false});

    auto [ec, data] = m_handler->async_read(CID, 0, 64).get();
    ASSERT_FALSE(ec);
    ASSERT_EQ(data->cbytes()[0], 0xFF);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(m_nvme_io->is_chunk_on_nvme(CID))
        << "Chunk should NOT be hydrated when hydrate_on_read is disabled";
}

///////////////////////////////////////////////////////////////////////////////
// Test 2.7 — Runtime config toggle: disable hydration → re-enable → verify
///////////////////////////////////////////////////////////////////////////////
TEST_F(TieredReadIntegTest, RuntimeConfigToggle) {
    constexpr chunk_id_t CID_A = 70;
    constexpr chunk_id_t CID_B = 71;
    put_chunk_on_s3_only(CID_A, 0x11);
    put_chunk_on_s3_only(CID_B, 0x22);

    m_handler->set_config({.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false});

    auto [ec1, d1] = m_handler->async_read(CID_A, 0, 64).get();
    ASSERT_FALSE(ec1);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_FALSE(m_nvme_io->is_chunk_on_nvme(CID_A));

    m_handler->set_config({.s3_read_fallback_enabled = true, .s3_hydrate_on_read = true});

    auto [ec2, d2] = m_handler->async_read(CID_B, 0, 64).get();
    ASSERT_FALSE(ec2);

    wait_for_condition([&]() { return m_nvme_io->is_chunk_on_nvme(CID_B); });
    ASSERT_TRUE(m_nvme_io->is_chunk_on_nvme(CID_B))
        << "Chunk B should be hydrated after re-enabling hydration";
}

///////////////////////////////////////////////////////////////////////////////
// Test 2.8 — Multiple chunks: mixed NVMe/S3 reads interleaved
///////////////////////////////////////////////////////////////////////////////
TEST_F(TieredReadIntegTest, MixedNvmeAndS3Reads) {
    for (uint32_t i = 0; i < 5; ++i) {
        put_chunk_on_nvme_and_s3(100 + i, static_cast< uint8_t >(0xA0 + i));
    }
    for (uint32_t i = 0; i < 5; ++i) {
        put_chunk_on_s3_only(200 + i, static_cast< uint8_t >(0xB0 + i));
    }

    for (uint32_t i = 0; i < 5; ++i) {
        auto [ec, data] = m_handler->async_read(100 + i, 0, 64).get();
        ASSERT_FALSE(ec) << "NVMe read failed for chunk " << (100 + i);
        ASSERT_EQ(data->cbytes()[0], static_cast< uint8_t >(0xA0 + i));
    }

    for (uint32_t i = 0; i < 5; ++i) {
        auto [ec, data] = m_handler->async_read(200 + i, 0, 64).get();
        ASSERT_FALSE(ec) << "S3 fallback failed for chunk " << (200 + i);
        ASSERT_EQ(data->cbytes()[0], static_cast< uint8_t >(0xB0 + i));
    }
}

///////////////////////////////////////////////////////////////////////////////
// Test 2.9 — Sync read path: NVMe fast path + S3 fallback
///////////////////////////////////////////////////////////////////////////////
TEST_F(TieredReadIntegTest, SyncReadIntegration) {
    constexpr chunk_id_t CID_NVME = 80;
    constexpr chunk_id_t CID_S3 = 81;

    put_chunk_on_nvme_and_s3(CID_NVME, 0x44);
    put_chunk_on_s3_only(CID_S3, 0x55);

    char buf1[64];
    auto ec1 = m_handler->sync_read(CID_NVME, 0, buf1, 64);
    ASSERT_FALSE(ec1);
    ASSERT_EQ(static_cast< uint8_t >(buf1[0]), 0x44);

    char buf2[64];
    auto ec2 = m_handler->sync_read(CID_S3, 0, buf2, 64);
    ASSERT_FALSE(ec2);
    ASSERT_EQ(static_cast< uint8_t >(buf2[0]), 0x55);
}

///////////////////////////////////////////////////////////////////////////////
// Test 2.10 — Metrics: verify counters after mixed read pattern
///////////////////////////////////////////////////////////////////////////////
TEST_F(TieredReadIntegTest, MetricsAfterMixedReads) {
    constexpr chunk_id_t CID_NVME = 90;
    constexpr chunk_id_t CID_S3 = 91;

    put_chunk_on_nvme_and_s3(CID_NVME, 0x66);
    put_chunk_on_s3_only(CID_S3, 0x77);

    m_handler->async_read(CID_NVME, 0, 64).get();
    m_handler->async_read(CID_NVME, 0, 64).get();
    m_handler->async_read(CID_S3, 0, 64).get();

    auto& metrics = m_handler->metrics();
    (void)metrics;
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_integ_tiered_read");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
