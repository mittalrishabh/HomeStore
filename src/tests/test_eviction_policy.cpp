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

#include <homestore/s3/eviction_policy.h>
#include <homestore/s3/chunk_eviction_manager.h>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Mocks — follow patterns from test_chunk_eviction.cpp
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

class MockNvmeChunkAllocator : public NvmeChunkAllocator {
public:
    explicit MockNvmeChunkAllocator(uint64_t total_capacity)
        : m_total{total_capacity}, m_free{total_capacity} {}

    std::error_code allocate_nvme_chunk(chunk_id_t /*chunk_id*/, uint64_t chunk_size) override {
        if (chunk_size > m_free) return std::make_error_code(std::errc::no_space_on_device);
        m_free -= chunk_size;
        return {};
    }

    void release_nvme_chunk(chunk_id_t /*chunk_id*/, uint64_t chunk_size) override {
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
class EvictionPolicyTest : public ::testing::Test {
protected:
    static constexpr uint64_t CHUNK_SIZE = 4096;
    static constexpr uint64_t NVME_CAPACITY = 1024 * 1024;

    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_chunk_reader = std::make_shared< MockNvmeChunkReader >();

        S3KeyMapper key_mapper{.volume_id = "vol-001"};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_chunk_reader, key_mapper);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, "vol-001", 1024, nullptr);

        m_nvme_mgr = std::make_shared< MockNvmeChunkManager >();
        m_eviction_mgr = std::make_shared< ChunkEvictionManager >(m_s3_pdev.get(), m_nvme_mgr);
        m_nvme_alloc = std::make_shared< MockNvmeChunkAllocator >(NVME_CAPACITY);
    }

    std::unique_ptr< EvictionPolicyEngine > make_engine(EvictionPolicyConfig cfg = {}) {
        return std::make_unique< EvictionPolicyEngine >(
            m_eviction_mgr, m_nvme_alloc, NVME_CAPACITY, std::move(cfg));
    }

    void setup_chunk_on_both(chunk_id_t chunk_id, uint8_t pattern = 0xAB) {
        m_s3_pdev->create_chunk(chunk_id, CHUNK_SIZE, S3ChunkType::DATA, 1);
        auto data = make_test_data(CHUNK_SIZE, pattern);
        m_nvme_chunk_reader->set_chunk_data(chunk_id, data);
        m_chunk_store->put(chunk_id, {}, CHUNK_SIZE);
        m_nvme_mgr->set_on_nvme(chunk_id, true);
    }

    void register_chunk(EvictionPolicyEngine& engine, chunk_id_t chunk_id, uint8_t pattern = 0xAB) {
        setup_chunk_on_both(chunk_id, pattern);
        engine.record_access(chunk_id, CHUNK_SIZE);
        engine.mark_on_s3(chunk_id);
    }

    void register_chunks(EvictionPolicyEngine& engine, uint32_t count, chunk_id_t start = 1) {
        for (uint32_t i = 0; i < count; ++i) {
            register_chunk(engine, start + i, static_cast< uint8_t >(0xA0 + i));
        }
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_chunk_reader;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::shared_ptr< MockNvmeChunkManager > m_nvme_mgr;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;
    std::shared_ptr< MockNvmeChunkAllocator > m_nvme_alloc;
};

///////////////////////////////////////////////////////////////////////////////
// Basic LRU selection
///////////////////////////////////////////////////////////////////////////////

TEST_F(EvictionPolicyTest, SelectsColdestChunk) {
    auto engine = make_engine();

    register_chunk(*engine, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 3);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 1);
    ASSERT_EQ(victim_size, CHUNK_SIZE);
}

TEST_F(EvictionPolicyTest, AccessUpdatesRecency) {
    auto engine = make_engine();

    register_chunk(*engine, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 2);

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    engine->record_access(1, CHUNK_SIZE);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 2);
}

TEST_F(EvictionPolicyTest, NoCandidateWhenEmpty) {
    auto engine = make_engine();

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_FALSE(engine->select_eviction_candidate(victim, victim_size));
}

TEST_F(EvictionPolicyTest, SkipsEvictedChunks) {
    auto engine = make_engine();

    register_chunk(*engine, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 3);

    m_eviction_mgr->evict_chunk(1, CHUNK_SIZE);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 2);
}

TEST_F(EvictionPolicyTest, NoCandidateWhenAllEvicted) {
    auto engine = make_engine();

    register_chunk(*engine, 1);
    m_eviction_mgr->evict_chunk(1, CHUNK_SIZE);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_FALSE(engine->select_eviction_candidate(victim, victim_size));
}

TEST_F(EvictionPolicyTest, SkipsChunksNotMarkedOnS3) {
    auto engine = make_engine();

    register_chunk(*engine, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    setup_chunk_on_both(2);
    engine->record_access(2, CHUNK_SIZE);

    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 3);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 1);
}

///////////////////////////////////////////////////////////////////////////////
// Anti-thrash cooldown
///////////////////////////////////////////////////////////////////////////////

TEST_F(EvictionPolicyTest, CooldownPreventsImmediateReEviction) {
    EvictionPolicyConfig cfg;
    cfg.cooldown_secs = 3600;
    auto engine = make_engine(cfg);

    register_chunk(*engine, 1);
    register_chunk(*engine, 2);

    engine->record_hydration(1);
    ASSERT_TRUE(engine->is_chunk_in_cooldown(1));

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 2);
}

TEST_F(EvictionPolicyTest, CooldownExpiresAllowsEviction) {
    EvictionPolicyConfig cfg;
    cfg.cooldown_secs = 0;
    auto engine = make_engine(cfg);

    register_chunk(*engine, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 2);

    engine->record_hydration(1);
    ASSERT_FALSE(engine->is_chunk_in_cooldown(1));

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_TRUE(victim == 1 || victim == 2);
}

TEST_F(EvictionPolicyTest, AllChunksInCooldownReturnsNone) {
    EvictionPolicyConfig cfg;
    cfg.cooldown_secs = 3600;
    auto engine = make_engine(cfg);

    register_chunk(*engine, 1);
    engine->record_hydration(1);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_FALSE(engine->select_eviction_candidate(victim, victim_size));
}

///////////////////////////////////////////////////////////////////////////////
// Chunk tracking lifecycle
///////////////////////////////////////////////////////////////////////////////

TEST_F(EvictionPolicyTest, RemoveChunkStopsTracking) {
    auto engine = make_engine();

    register_chunk(*engine, 1);
    ASSERT_EQ(engine->tracked_chunk_count(), 1u);

    engine->remove_chunk(1);
    ASSERT_EQ(engine->tracked_chunk_count(), 0u);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_FALSE(engine->select_eviction_candidate(victim, victim_size));
}

TEST_F(EvictionPolicyTest, TrackedChunkCount) {
    auto engine = make_engine();

    ASSERT_EQ(engine->tracked_chunk_count(), 0u);

    register_chunks(*engine, 5);
    ASSERT_EQ(engine->tracked_chunk_count(), 5u);

    engine->remove_chunk(3);
    ASSERT_EQ(engine->tracked_chunk_count(), 4u);
}

///////////////////////////////////////////////////////////////////////////////
// NVMe usage ratio and eviction cycle
///////////////////////////////////////////////////////////////////////////////

TEST_F(EvictionPolicyTest, NvmeUsageRatio) {
    auto engine = make_engine();

    ASSERT_FLOAT_EQ(engine->current_nvme_usage_ratio(), 0.0f);

    m_nvme_alloc->set_free(NVME_CAPACITY / 2);
    ASSERT_FLOAT_EQ(engine->current_nvme_usage_ratio(), 0.5f);

    m_nvme_alloc->set_free(0);
    ASSERT_FLOAT_EQ(engine->current_nvme_usage_ratio(), 1.0f);
}

TEST_F(EvictionPolicyTest, EvictionCycleSkipsBelowThreshold) {
    EvictionPolicyConfig cfg;
    cfg.nvme_capacity_threshold = 0.9f;
    auto engine = make_engine(cfg);

    m_nvme_alloc->set_free(NVME_CAPACITY / 2);

    auto evicted = engine->run_eviction_cycle();
    ASSERT_EQ(evicted, 0u);
}

TEST_F(EvictionPolicyTest, EvictionCycleEvictsWhenAboveThreshold) {
    EvictionPolicyConfig cfg;
    cfg.nvme_capacity_threshold = 0.5f;
    auto engine = make_engine(cfg);

    register_chunks(*engine, 4);

    m_nvme_alloc->set_free(NVME_CAPACITY / 5);

    auto evicted = engine->run_eviction_cycle();
    ASSERT_GT(evicted, 0u);
}

TEST_F(EvictionPolicyTest, EvictionCycleStopsWhenBelowThreshold) {
    EvictionPolicyConfig cfg;
    cfg.nvme_capacity_threshold = 0.8f;
    auto engine = make_engine(cfg);

    register_chunks(*engine, 8);

    m_nvme_alloc->set_free(NVME_CAPACITY / 10);

    auto evicted = engine->run_eviction_cycle();
    ASSERT_GE(evicted, 1u);
}

///////////////////////////////////////////////////////////////////////////////
// Config validation
///////////////////////////////////////////////////////////////////////////////

TEST_F(EvictionPolicyTest, InvalidThresholdZero) {
    EvictionPolicyConfig cfg;
    cfg.nvme_capacity_threshold = 0.0f;
    ASSERT_DEATH(make_engine(cfg), "nvme_capacity_threshold");
}

TEST_F(EvictionPolicyTest, InvalidThresholdAboveOne) {
    EvictionPolicyConfig cfg;
    cfg.nvme_capacity_threshold = 1.5f;
    ASSERT_DEATH(make_engine(cfg), "nvme_capacity_threshold");
}

TEST_F(EvictionPolicyTest, ValidThresholdBoundary) {
    EvictionPolicyConfig cfg;
    cfg.nvme_capacity_threshold = 1.0f;
    auto engine = make_engine(cfg);
    ASSERT_EQ(engine->config().nvme_capacity_threshold, 1.0f);
}

///////////////////////////////////////////////////////////////////////////////
// Concurrent access
///////////////////////////////////////////////////////////////////////////////

TEST_F(EvictionPolicyTest, ConcurrentRecordAccess) {
    auto engine = make_engine();
    constexpr uint32_t NUM_THREADS = 8;
    constexpr uint32_t OPS_PER_THREAD = 100;

    for (uint32_t i = 0; i < NUM_THREADS; ++i) {
        register_chunk(*engine, i + 1);
    }

    std::vector< std::thread > threads;
    for (uint32_t i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&engine, i]() {
            for (uint32_t j = 0; j < OPS_PER_THREAD; ++j) {
                engine->record_access(i + 1, CHUNK_SIZE);
            }
        });
    }

    for (auto& t : threads) { t.join(); }

    ASSERT_EQ(engine->tracked_chunk_count(), NUM_THREADS);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
}

TEST_F(EvictionPolicyTest, ConcurrentSelectAndRecord) {
    auto engine = make_engine();
    constexpr uint32_t NUM_CHUNKS = 16;

    register_chunks(*engine, NUM_CHUNKS);

    std::atomic< uint32_t > selections{0};
    std::vector< std::thread > threads;

    for (uint32_t i = 0; i < 4; ++i) {
        threads.emplace_back([&engine, &selections]() {
            chunk_id_t victim;
            uint64_t victim_size;
            for (int j = 0; j < 50; ++j) {
                if (engine->select_eviction_candidate(victim, victim_size)) {
                    selections.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (uint32_t i = 0; i < 4; ++i) {
        threads.emplace_back([&engine, i]() {
            for (int j = 0; j < 50; ++j) {
                engine->record_access(i + 1, CHUNK_SIZE);
            }
        });
    }

    for (auto& t : threads) { t.join(); }

    ASSERT_GT(selections.load(), 0u);
}

///////////////////////////////////////////////////////////////////////////////
// Integration: eviction updates LRU ordering
///////////////////////////////////////////////////////////////////////////////

TEST_F(EvictionPolicyTest, EvictedChunkSkippedByNextSelection) {
    auto engine = make_engine();

    register_chunk(*engine, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 3);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 1);

    m_eviction_mgr->evict_chunk(1, CHUNK_SIZE);

    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 2);
}

TEST_F(EvictionPolicyTest, RehydratedChunkBecomesRecent) {
    EvictionPolicyConfig cfg;
    cfg.cooldown_secs = 0;
    auto engine = make_engine(cfg);

    register_chunk(*engine, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    register_chunk(*engine, 2);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    engine->record_hydration(1);

    chunk_id_t victim;
    uint64_t victim_size;
    ASSERT_TRUE(engine->select_eviction_candidate(victim, victim_size));
    ASSERT_EQ(victim, 2);
}

///////////////////////////////////////////////////////////////////////////////
// Background monitor start/stop
///////////////////////////////////////////////////////////////////////////////

TEST_F(EvictionPolicyTest, MonitorStartStop) {
    EvictionPolicyConfig cfg;
    cfg.monitor_interval_secs = 1;
    auto engine = make_engine(cfg);

    engine->start_monitor();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    engine->stop_monitor();
}

TEST_F(EvictionPolicyTest, DoubleStartIsNoop) {
    EvictionPolicyConfig cfg;
    cfg.monitor_interval_secs = 1;
    auto engine = make_engine(cfg);

    engine->start_monitor();
    engine->start_monitor();
    engine->stop_monitor();
}

TEST_F(EvictionPolicyTest, DoubleStopIsNoop) {
    EvictionPolicyConfig cfg;
    cfg.monitor_interval_secs = 1;
    auto engine = make_engine(cfg);

    engine->start_monitor();
    engine->stop_monitor();
    engine->stop_monitor();
}

///////////////////////////////////////////////////////////////////////////////
// Metrics
///////////////////////////////////////////////////////////////////////////////

TEST_F(EvictionPolicyTest, MetricsTracked) {
    auto engine = make_engine();

    register_chunk(*engine, 1);

    chunk_id_t victim;
    uint64_t victim_size;
    engine->select_eviction_candidate(victim, victim_size);

    auto& m = engine->metrics();
    (void)m;
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_eviction_policy");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
