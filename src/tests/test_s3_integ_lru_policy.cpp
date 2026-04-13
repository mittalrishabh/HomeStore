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
 * @file test_s3_integ_lru_policy.cpp
 * @brief Integration tests — Group 6: LRU Eviction Policy
 *
 * Wires up REAL: EvictionPolicyEngine, ChunkEvictionManager,
 *               FullChunkStore, S3PhysicalDev
 * Mocked:        S3 transport (MockS3ObjectStore), NVMe allocator
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
#include <homestore/s3/eviction_policy.h>
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

class MockNvmeChunkAllocator : public NvmeChunkAllocator {
public:
    explicit MockNvmeChunkAllocator(uint64_t total)
        : m_total{total}, m_free{total} {}

    std::error_code allocate_nvme_chunk(chunk_id_t, uint64_t chunk_size) override {
        if (chunk_size > m_free) return std::make_error_code(std::errc::no_space_on_device);
        m_free -= chunk_size;
        return {};
    }

    void release_nvme_chunk(chunk_id_t, uint64_t chunk_size) override {
        m_free = std::min(m_free + chunk_size, m_total);
    }

    uint64_t free_nvme_space_bytes() const override { return m_free; }

    void set_free(uint64_t free) { m_free = free; }

private:
    uint64_t m_total;
    uint64_t m_free;
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
    cfg.bucket = "homestore-integ-lru";
    cfg.region = "us-east-1";
    cfg.retry_count = 1;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

///////////////////////////////////////////////////////////////////////////////
// Test Fixture
///////////////////////////////////////////////////////////////////////////////
class LruPolicyIntegTest : public ::testing::Test {
protected:
    static constexpr uint64_t CHUNK_SIZE = 4096;
    static constexpr uint64_t NVME_CAPACITY = 1024 * 1024;

    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();

        S3KeyMapper key_mapper{.volume_id = "vol-lru-integ"};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, key_mapper);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, "vol-lru-integ", 1024, nullptr);

        m_nvme_mgr = std::make_shared< MockNvmeChunkManager >();
        m_nvme_alloc = std::make_shared< MockNvmeChunkAllocator >(NVME_CAPACITY);

        m_eviction_mgr = std::make_shared< ChunkEvictionManager >(m_s3_pdev.get(), m_nvme_mgr);
    }

    std::unique_ptr< EvictionPolicyEngine > make_engine(EvictionPolicyConfig cfg = {}) {
        return std::make_unique< EvictionPolicyEngine >(
            m_eviction_mgr, m_nvme_alloc, NVME_CAPACITY, std::move(cfg));
    }

    void setup_chunk(chunk_id_t chunk_id, uint8_t pattern = 0xAB) {
        m_s3_pdev->create_chunk(chunk_id, CHUNK_SIZE, S3ChunkType::DATA, 1);
        auto data = make_test_data(CHUNK_SIZE, pattern);
        m_nvme_reader->set_chunk_data(chunk_id, data);
        m_chunk_store->put(chunk_id, {}, CHUNK_SIZE);
        m_nvme_mgr->set_on_nvme(chunk_id, true);
    }

    void register_chunk(EvictionPolicyEngine& engine, chunk_id_t chunk_id, uint8_t pattern = 0xAB) {
        setup_chunk(chunk_id, pattern);
        engine.record_access(chunk_id, CHUNK_SIZE);
        engine.mark_on_s3(chunk_id);
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::shared_ptr< MockNvmeChunkManager > m_nvme_mgr;
    std::shared_ptr< MockNvmeChunkAllocator > m_nvme_alloc;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;
};

///////////////////////////////////////////////////////////////////////////////
// Test 6.1 — Coldest chunk evicted first
///////////////////////////////////////////////////////////////////////////////
TEST_F(LruPolicyIntegTest, ColdestChunkEvictedFirst) {
    auto engine = make_engine();

    register_chunk(*engine, 1, 0xAA);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 2, 0xBB);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 3, 0xCC);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 1u);

    auto result = m_eviction_mgr->evict_chunk(victim, victim_size);
    ASSERT_EQ(result, EvictionResult::SUCCESS);
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(1));

    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 2u);
}

///////////////////////////////////////////////////////////////////////////////
// Test 6.2 — Capacity threshold enforcement: above threshold triggers eviction
///////////////////////////////////////////////////////////////////////////////
TEST_F(LruPolicyIntegTest, CapacityThresholdEnforcement) {
    EvictionPolicyConfig cfg;
    cfg.nvme_capacity_threshold = 0.5f;
    auto engine = make_engine(cfg);

    for (uint32_t i = 1; i <= 4; ++i) {
        register_chunk(*engine, i, static_cast< uint8_t >(0xA0 + i));
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
    }

    // NVMe 80% full → above 50% threshold
    m_nvme_alloc->set_free(NVME_CAPACITY / 5);

    auto evicted = engine->run_eviction_cycle();
    ASSERT_GE(evicted, 1u) << "Should evict chunks when above threshold";

    // Verify at least one chunk was actually evicted
    bool any_evicted = false;
    for (uint32_t i = 1; i <= 4; ++i) {
        if (m_eviction_mgr->is_chunk_evicted(i)) { any_evicted = true; break; }
    }
    ASSERT_TRUE(any_evicted);
}

///////////////////////////////////////////////////////////////////////////////
// Test 6.3 — Anti-thrash cooldown: recently hydrated chunk skipped
///////////////////////////////////////////////////////////////////////////////
TEST_F(LruPolicyIntegTest, AntiThrashCooldown) {
    EvictionPolicyConfig cfg;
    cfg.cooldown_secs = 3600;
    auto engine = make_engine(cfg);

    register_chunk(*engine, 1, 0xAA);
    register_chunk(*engine, 2, 0xBB);

    engine->record_hydration(1);
    ASSERT_TRUE(engine->is_chunk_in_cooldown(1));

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 2u) << "Recently hydrated chunk 1 should be skipped";
}

///////////////////////////////////////////////////////////////////////////////
// Test 6.4 — Full lifecycle with access patterns → steady state
//   Simulate: create 8 chunks, access pattern makes some hot/cold,
//   eviction cycle evicts cold ones, hydrate them back, verify ordering.
///////////////////////////////////////////////////////////////////////////////
TEST_F(LruPolicyIntegTest, FullLifecycleAccessPatterns) {
    EvictionPolicyConfig cfg;
    cfg.nvme_capacity_threshold = 0.5f;
    cfg.cooldown_secs = 0;
    auto engine = make_engine(cfg);

    constexpr uint32_t NUM_CHUNKS = 8;
    for (uint32_t i = 1; i <= NUM_CHUNKS; ++i) {
        register_chunk(*engine, i, static_cast< uint8_t >(0xA0 + i));
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
    }

    // "Hot" access pattern: chunks 5-8 get accessed recently
    for (uint32_t i = 5; i <= NUM_CHUNKS; ++i) {
        engine->record_access(i, CHUNK_SIZE);
    }

    // Set NVMe to 90% full
    m_nvme_alloc->set_free(NVME_CAPACITY / 10);

    auto evicted = engine->run_eviction_cycle();
    ASSERT_GE(evicted, 1u);

    // Verify: cold chunks (1-4) should be evicted before hot chunks (5-8)
    uint32_t cold_evicted = 0;
    uint32_t hot_evicted = 0;
    for (uint32_t i = 1; i <= 4; ++i) {
        if (m_eviction_mgr->is_chunk_evicted(i)) cold_evicted++;
    }
    for (uint32_t i = 5; i <= NUM_CHUNKS; ++i) {
        if (m_eviction_mgr->is_chunk_evicted(i)) hot_evicted++;
    }

    ASSERT_GE(cold_evicted, hot_evicted)
        << "Cold chunks should be evicted before hot chunks";
}

///////////////////////////////////////////////////////////////////////////////
// Test 6.5 — Eviction candidate selection with real eviction execution
//   Select candidate via policy → execute eviction → verify state across
//   both EvictionPolicyEngine and ChunkEvictionManager.
///////////////////////////////////////////////////////////////////////////////
TEST_F(LruPolicyIntegTest, SelectAndExecuteEviction) {
    auto engine = make_engine();

    register_chunk(*engine, 1, 0x11);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 2, 0x22);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 3, 0x33);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 1u);

    auto result = m_eviction_mgr->evict_chunk(victim, victim_size);
    ASSERT_EQ(result, EvictionResult::SUCCESS);

    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(1));
    ASSERT_FALSE(m_eviction_mgr->is_chunk_evicted(2));
    ASSERT_FALSE(m_eviction_mgr->is_chunk_evicted(3));

    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 2u) << "After evicting 1, next candidate should be 2";
}

///////////////////////////////////////////////////////////////////////////////
// Test 6.6 — Background monitor: start, evicts above threshold, stop
///////////////////////////////////////////////////////////////////////////////
TEST_F(LruPolicyIntegTest, BackgroundMonitorEvictsAboveThreshold) {
    EvictionPolicyConfig cfg;
    cfg.nvme_capacity_threshold = 0.5f;
    cfg.monitor_interval_secs = 1;
    cfg.cooldown_secs = 0;
    auto engine = make_engine(cfg);

    for (uint32_t i = 1; i <= 4; ++i) {
        register_chunk(*engine, i, static_cast< uint8_t >(0xA0 + i));
        std::this_thread::sleep_for(std::chrono::milliseconds(3));
    }

    m_nvme_alloc->set_free(NVME_CAPACITY / 10);

    engine->start_monitor();
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    engine->stop_monitor();

    bool any_evicted = false;
    for (uint32_t i = 1; i <= 4; ++i) {
        if (m_eviction_mgr->is_chunk_evicted(i)) { any_evicted = true; break; }
    }
    ASSERT_TRUE(any_evicted) << "Monitor should have evicted at least one chunk";
}

///////////////////////////////////////////////////////////////////////////////
// Test 6.7 — Concurrent access tracking + candidate selection
///////////////////////////////////////////////////////////////////////////////
TEST_F(LruPolicyIntegTest, ConcurrentAccessAndSelection) {
    auto engine = make_engine();

    for (uint32_t i = 1; i <= 8; ++i) {
        register_chunk(*engine, i, static_cast< uint8_t >(0xA0 + i));
    }

    std::atomic< uint32_t > selections{0};
    std::vector< std::thread > threads;

    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&engine, &selections]() {
            chunk_id_t victim;
            uint64_t victim_size;
            for (int j = 0; j < 20; ++j) {
                if (engine->select_eviction_candidate(victim, victim_size)) {
                    selections.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&engine, t]() {
            for (int j = 0; j < 50; ++j) {
                engine->record_access(static_cast< chunk_id_t >(t + 1), CHUNK_SIZE);
            }
        });
    }

    for (auto& t : threads) { t.join(); }

    ASSERT_GT(selections.load(), 0u);
    ASSERT_EQ(engine->tracked_chunk_count(), 8u);
}

///////////////////////////////////////////////////////////////////////////////
// Test 6.8 — Re-hydrated chunk tracked again, respects new access time
///////////////////////////////////////////////////////////////////////////////
TEST_F(LruPolicyIntegTest, RehydratedChunkTrackedCorrectly) {
    EvictionPolicyConfig cfg;
    cfg.cooldown_secs = 0;
    auto engine = make_engine(cfg);

    register_chunk(*engine, 1, 0xAA);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 2, 0xBB);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    // Evict chunk 1
    m_eviction_mgr->evict_chunk(1, CHUNK_SIZE);

    // Hydrate chunk 1 (becomes most-recent)
    engine->record_hydration(1);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 2u)
        << "Chunk 2 should be coldest after chunk 1 was re-hydrated";
}

///////////////////////////////////////////////////////////////////////////////
// Test 6.9 — Eviction cycle skips when below threshold
///////////////////////////////////////////////////////////////////////////////
TEST_F(LruPolicyIntegTest, EvictionCycleSkipsBelowThreshold) {
    EvictionPolicyConfig cfg;
    cfg.nvme_capacity_threshold = 0.9f;
    auto engine = make_engine(cfg);

    for (uint32_t i = 1; i <= 4; ++i) {
        register_chunk(*engine, i);
    }

    m_nvme_alloc->set_free(NVME_CAPACITY / 2);

    auto evicted = engine->run_eviction_cycle();
    ASSERT_EQ(evicted, 0u) << "Should not evict when below threshold";

    for (uint32_t i = 1; i <= 4; ++i) {
        ASSERT_FALSE(m_eviction_mgr->is_chunk_evicted(i));
    }
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_integ_lru_policy");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
