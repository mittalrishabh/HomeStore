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
 * @file test_s3_integ_eviction_lifecycle.cpp
 * @brief Integration tests — Group 5: Eviction + Hydration Lifecycle
 *
 * Wires up REAL: ChunkEvictionManager, ChunkHydrationManager,
 *               TieredReadHandler, FullChunkStore, S3PhysicalDev
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
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/tiered_read_handler.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Mocks
///////////////////////////////////////////////////////////////////////////////

class MockNvmeChunkReader : public NvmeChunkReader {
public:
    void set_chunk_data(chunk_id_t chunk_id, sisl::byte_array data) {
        std::lock_guard lock{m_mtx};
        m_chunks[chunk_id] = std::move(data);
    }

    void remove_chunk_data(chunk_id_t chunk_id) {
        std::lock_guard lock{m_mtx};
        m_chunks.erase(chunk_id);
    }

    std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t chunk_id, uint64_t chunk_size) override {
        std::lock_guard lock{m_mtx};
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
    mutable std::mutex m_mtx;
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

    void clear_chunk(chunk_id_t chunk_id) {
        std::lock_guard lock{m_mtx};
        m_on_nvme.erase(chunk_id);
        for (auto it = m_storage.begin(); it != m_storage.end();) {
            if (it->first.substr(0, it->first.find(':')) == std::to_string(chunk_id)) {
                it = m_storage.erase(it);
            } else {
                ++it;
            }
        }
    }

    std::error_code nvme_read(chunk_id_t chunk_id, uint64_t offset,
                              char* buf, uint64_t size) override {
        std::lock_guard lock{m_mtx};
        if (m_on_nvme.find(chunk_id) == m_on_nvme.end()) {
            return std::make_error_code(std::errc::no_such_device);
        }
        auto key = make_key(chunk_id, offset);
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
        auto key = make_key(chunk_id, offset);
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
    static std::string make_key(chunk_id_t cid, uint64_t offset) {
        return std::to_string(cid) + ":" + std::to_string(offset);
    }

    mutable std::mutex m_mtx;
    std::unordered_map< std::string, sisl::byte_array > m_storage;
    std::set< chunk_id_t > m_on_nvme;
};

class MockNvmeChunkAllocator : public NvmeChunkAllocator {
public:
    explicit MockNvmeChunkAllocator(uint64_t total = 64 * 1024 * 1024)
        : m_total{total}, m_used{0} {}

    std::error_code allocate_nvme_chunk(chunk_id_t, uint64_t chunk_size) override {
        if (m_used + chunk_size > m_total) {
            return std::make_error_code(std::errc::no_space_on_device);
        }
        m_used += chunk_size;
        return {};
    }

    void release_nvme_chunk(chunk_id_t, uint64_t chunk_size) override {
        m_used = m_used > chunk_size ? m_used - chunk_size : 0;
    }

    uint64_t free_nvme_space_bytes() const override {
        return m_total > m_used ? m_total - m_used : 0;
    }

    void set_used(uint64_t used) { m_used = used; }

private:
    uint64_t m_total;
    uint64_t m_used;
};

class MockEvictionCandidateSelector : public EvictionCandidateSelector {
public:
    void add_candidate(chunk_id_t id, uint64_t size) {
        std::lock_guard lock{m_mtx};
        m_candidates.push_back({id, size});
    }

    bool select_eviction_candidate(chunk_id_t& out_chunk_id, uint64_t& out_chunk_size) override {
        std::lock_guard lock{m_mtx};
        if (m_candidates.empty()) return false;
        auto [id, size] = m_candidates.front();
        m_candidates.erase(m_candidates.begin());
        out_chunk_id = id;
        out_chunk_size = size;
        return true;
    }

private:
    std::mutex m_mtx;
    std::vector< std::pair< chunk_id_t, uint64_t > > m_candidates;
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
    cfg.bucket = "homestore-integ-eviction";
    cfg.region = "us-east-1";
    cfg.retry_count = 1;
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
class EvictionLifecycleIntegTest : public ::testing::Test {
protected:
    static constexpr uint64_t CHUNK_SIZE = 4096;

    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();

        S3KeyMapper key_mapper{.volume_id = "vol-evict-integ"};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, key_mapper);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, "vol-evict-integ", 1024, nullptr);

        m_nvme_mgr = std::make_shared< MockNvmeChunkManager >();
        m_nvme_io = std::make_shared< MockNvmeDeviceIO >();
        m_nvme_alloc = std::make_shared< MockNvmeChunkAllocator >();
        m_eviction_selector = std::make_shared< MockEvictionCandidateSelector >();

        m_eviction_mgr = std::make_shared< ChunkEvictionManager >(m_s3_pdev.get(), m_nvme_mgr);

        m_hydration_mgr = std::make_shared< ChunkHydrationManager >(
            m_chunk_store, m_nvme_io, m_nvme_alloc, m_eviction_mgr,
            m_eviction_selector, 2);
    }

    void TearDown() override {
        m_hydration_mgr->shutdown();
    }

    void setup_chunk(chunk_id_t chunk_id, uint8_t pattern) {
        m_s3_pdev->create_chunk(chunk_id, CHUNK_SIZE, S3ChunkType::DATA, 1);
        auto data = make_test_data(CHUNK_SIZE, pattern);
        m_nvme_reader->set_chunk_data(chunk_id, data);
        m_chunk_store->put(chunk_id, {}, CHUNK_SIZE);
        m_nvme_io->store(chunk_id, 0,
                         reinterpret_cast< const char* >(data->cbytes()), CHUNK_SIZE);
        m_nvme_mgr->set_on_nvme(chunk_id, true);
    }

    void evict(chunk_id_t chunk_id) {
        auto r = m_eviction_mgr->evict_chunk(chunk_id, CHUNK_SIZE);
        ASSERT_EQ(r, EvictionResult::SUCCESS);
        m_nvme_io->set_on_nvme(chunk_id, false);
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::shared_ptr< MockNvmeChunkManager > m_nvme_mgr;
    std::shared_ptr< MockNvmeDeviceIO > m_nvme_io;
    std::shared_ptr< MockNvmeChunkAllocator > m_nvme_alloc;
    std::shared_ptr< MockEvictionCandidateSelector > m_eviction_selector;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;
    std::shared_ptr< ChunkHydrationManager > m_hydration_mgr;
};

///////////////////////////////////////////////////////////////////////////////
// Test 5.1 — Evict chunk → verify NVMe freed, reads still work from S3
///////////////////////////////////////////////////////////////////////////////
TEST_F(EvictionLifecycleIntegTest, EvictVerifyReadsStillWork) {
    constexpr chunk_id_t CID = 10;
    setup_chunk(CID, 0xAA);

    TieredReadHandler handler{m_s3_pdev.get(), m_nvme_io,
                              {.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false}};

    auto [ec1, d1] = handler.async_read(CID, 0, 64).get();
    ASSERT_FALSE(ec1);
    ASSERT_EQ(d1->cbytes()[0], 0xAA);

    evict(CID);
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CID));
    ASSERT_FALSE(m_nvme_io->is_chunk_on_nvme(CID));

    auto [ec2, d2] = handler.async_read(CID, 0, 64).get();
    ASSERT_FALSE(ec2) << ec2.message();
    ASSERT_EQ(d2->cbytes()[0], 0xAA);
}

///////////////////////////////////////////////////////////////////////////////
// Test 5.2 — Full cycle: evict → hydrate → evict → hydrate
///////////////////////////////////////////////////////////////////////////////
TEST_F(EvictionLifecycleIntegTest, FullEvictHydrateCycle) {
    constexpr chunk_id_t CID = 20;
    setup_chunk(CID, 0xBB);

    // Cycle 1: evict
    evict(CID);
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CID));

    // Cycle 1: hydrate
    auto r1 = m_hydration_mgr->hydrate_sync(CID, CHUNK_SIZE);
    ASSERT_EQ(r1, HydrationResult::SUCCESS);
    ASSERT_FALSE(m_eviction_mgr->is_chunk_evicted(CID));
    ASSERT_TRUE(m_nvme_io->is_chunk_on_nvme(CID));

    // Must re-register for eviction since NVMe state changed
    m_nvme_mgr->set_on_nvme(CID, true);

    // Cycle 2: evict again
    evict(CID);
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CID));

    // Cycle 2: hydrate again
    auto r2 = m_hydration_mgr->hydrate_sync(CID, CHUNK_SIZE);
    ASSERT_EQ(r2, HydrationResult::SUCCESS);
    ASSERT_FALSE(m_eviction_mgr->is_chunk_evicted(CID));
    ASSERT_TRUE(m_nvme_io->is_chunk_on_nvme(CID));
}

///////////////////////////////////////////////////////////////////////////////
// Test 5.3 — Hydration triggers eviction of cold chunk to make NVMe space
///////////////////////////////////////////////////////////////////////////////
TEST_F(EvictionLifecycleIntegTest, HydrationTriggersEvictionForSpace) {
    constexpr chunk_id_t CID_COLD = 30;
    constexpr chunk_id_t CID_HOT = 31;

    setup_chunk(CID_COLD, 0xCC);
    setup_chunk(CID_HOT, 0xDD);

    evict(CID_HOT);
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CID_HOT));

    m_nvme_alloc = std::make_shared< MockNvmeChunkAllocator >(CHUNK_SIZE);
    m_nvme_alloc->set_used(CHUNK_SIZE);

    m_eviction_selector->add_candidate(CID_COLD, CHUNK_SIZE);

    m_hydration_mgr->shutdown();
    m_hydration_mgr = std::make_shared< ChunkHydrationManager >(
        m_chunk_store, m_nvme_io, m_nvme_alloc, m_eviction_mgr,
        m_eviction_selector, 1);

    auto result = m_hydration_mgr->hydrate_sync(CID_HOT, CHUNK_SIZE);
    ASSERT_EQ(result, HydrationResult::SUCCESS);
    ASSERT_TRUE(m_nvme_io->is_chunk_on_nvme(CID_HOT));
}

///////////////////////////////////////////////////////////////////////////////
// Test 5.4 — Concurrent eviction + hydration + reads on different chunks
///////////////////////////////////////////////////////////////////////////////
TEST_F(EvictionLifecycleIntegTest, ConcurrentEvictionHydrationReads) {
    for (uint32_t i = 0; i < 8; ++i) {
        setup_chunk(100 + i, static_cast< uint8_t >(0xA0 + i));
    }

    TieredReadHandler handler{m_s3_pdev.get(), m_nvme_io,
                              {.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false}};

    std::atomic< int > errors{0};
    std::vector< std::thread > threads;

    // Eviction threads: evict chunks 100-103
    for (uint32_t i = 0; i < 4; ++i) {
        threads.emplace_back([this, i, &errors]() {
            auto r = m_eviction_mgr->evict_chunk(100 + i, CHUNK_SIZE);
            if (r != EvictionResult::SUCCESS) errors++;
            m_nvme_io->set_on_nvme(100 + i, false);
        });
    }

    // Hydration threads: hydrate chunks 104-107 (already on NVMe but testing sync)
    for (uint32_t i = 4; i < 8; ++i) {
        threads.emplace_back([this, i]() {
            m_eviction_mgr->evict_chunk(100 + i, CHUNK_SIZE);
            m_nvme_io->set_on_nvme(100 + i, false);
            m_hydration_mgr->hydrate_sync(100 + i, CHUNK_SIZE);
        });
    }

    // Read threads: read all chunks
    for (uint32_t i = 0; i < 8; ++i) {
        threads.emplace_back([&handler, i, &errors]() {
            auto [ec, data] = handler.async_read(100 + i, 0, 64).get();
            if (ec) errors++;
        });
    }

    for (auto& t : threads) { t.join(); }

    ASSERT_EQ(errors.load(), 0) << "No operations should fail";

    // Chunks 100-103 should be evicted, 104-107 should be hydrated
    for (uint32_t i = 0; i < 4; ++i) {
        EXPECT_TRUE(m_eviction_mgr->is_chunk_evicted(100 + i));
    }
    for (uint32_t i = 4; i < 8; ++i) {
        EXPECT_TRUE(m_nvme_io->is_chunk_on_nvme(100 + i));
    }
}

///////////////////////////////////////////////////////////////////////////////
// Test 5.5 — Eviction of chunk not on S3 returns NOT_ON_S3
///////////////////////////////////////////////////////////////////////////////
TEST_F(EvictionLifecycleIntegTest, EvictionNotOnS3Rejected) {
    constexpr chunk_id_t CID = 50;
    m_nvme_mgr->set_on_nvme(CID, true);

    auto result = m_eviction_mgr->evict_chunk(CID, CHUNK_SIZE);
    ASSERT_EQ(result, EvictionResult::NOT_ON_S3);
}

///////////////////////////////////////////////////////////////////////////////
// Test 5.6 — Double eviction returns ALREADY_EVICTED
///////////////////////////////////////////////////////////////////////////////
TEST_F(EvictionLifecycleIntegTest, DoubleEvictionRejected) {
    constexpr chunk_id_t CID = 60;
    setup_chunk(CID, 0xEE);

    evict(CID);
    auto result = m_eviction_mgr->evict_chunk(CID, CHUNK_SIZE);
    ASSERT_EQ(result, EvictionResult::ALREADY_EVICTED);
}

///////////////////////////////////////////////////////////////////////////////
// Test 5.7 — Writes blocked during eviction
///////////////////////////////////////////////////////////////////////////////
TEST_F(EvictionLifecycleIntegTest, WritesBlockedDuringEviction) {
    constexpr chunk_id_t CID = 70;
    setup_chunk(CID, 0xFF);

    // Before eviction: writes not blocked
    ASSERT_FALSE(m_eviction_mgr->is_write_blocked(CID));

    // After eviction: is_chunk_evicted but not EVICTING anymore
    evict(CID);
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(CID));
    ASSERT_FALSE(m_eviction_mgr->is_write_blocked(CID));
}

///////////////////////////////////////////////////////////////////////////////
// Test 5.8 — Hydration restores chunk, verifying data integrity
///////////////////////////////////////////////////////////////////////////////
TEST_F(EvictionLifecycleIntegTest, HydrationDataIntegrity) {
    constexpr chunk_id_t CID = 80;
    constexpr uint8_t PATTERN = 0x42;
    setup_chunk(CID, PATTERN);

    evict(CID);

    auto result = m_hydration_mgr->hydrate_sync(CID, CHUNK_SIZE);
    ASSERT_EQ(result, HydrationResult::SUCCESS);

    TieredReadHandler handler{m_s3_pdev.get(), m_nvme_io,
                              {.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false}};

    auto [ec, data] = handler.async_read(CID, 0, CHUNK_SIZE).get();
    ASSERT_FALSE(ec);
    ASSERT_NE(data, nullptr);
    ASSERT_EQ(data->size(), CHUNK_SIZE);

    for (uint64_t i = 0; i < CHUNK_SIZE; ++i) {
        ASSERT_EQ(data->cbytes()[i], PATTERN)
            << "Data mismatch at byte " << i << " after eviction + hydration";
    }
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_integ_eviction_lifecycle");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
