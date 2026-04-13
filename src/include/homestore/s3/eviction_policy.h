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
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/s3/chunk_eviction_manager.h>
#include <homestore/s3/chunk_hydration_manager.h>

namespace homestore {

struct EvictionPolicyConfig {
    float nvme_capacity_threshold{0.9f};
    std::string policy{"lru"};
    uint32_t cooldown_secs{60};
    uint32_t monitor_interval_secs{10};
};

class EvictionPolicyMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit EvictionPolicyMetrics() : sisl::MetricsGroupWrapper{"EvictionPolicy", "eviction_policy"} {
        REGISTER_COUNTER(lru_candidates_scanned, "Chunks scanned during candidate selection");
        REGISTER_COUNTER(lru_candidates_skipped_cooldown, "Candidates skipped due to hydration cooldown");
        REGISTER_COUNTER(lru_candidates_skipped_not_on_nvme, "Candidates skipped: not on NVMe");
        REGISTER_COUNTER(lru_candidates_selected, "Candidates selected for eviction");
        REGISTER_COUNTER(lru_no_candidate_available, "Selection attempts with no eligible candidate");
        REGISTER_COUNTER(lru_access_records, "Total access records tracked");

        REGISTER_COUNTER(eviction_cycles_run, "Eviction monitor cycles executed");
        REGISTER_COUNTER(eviction_cycles_skipped, "Cycles skipped: below threshold");
        REGISTER_COUNTER(chunks_evicted_by_monitor, "Chunks evicted by background monitor");
        REGISTER_COUNTER(chunks_eviction_failed_by_monitor, "Evictions failed during monitor cycle");

        REGISTER_HISTOGRAM(eviction_cycle_latency_us, "Eviction cycle latency in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~EvictionPolicyMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief LRU-based eviction policy engine.
 *
 * Implements EvictionCandidateSelector to pick the coldest chunk when NVMe
 * space is needed. Tracks per-chunk access recency and enforces an
 * anti-thrash cooldown after hydration.
 *
 * Additionally provides a background eviction monitor that periodically
 * checks NVMe utilization and evicts cold chunks when the capacity
 * threshold is exceeded.
 *
 * ## Thread Safety:
 *
 * All public methods are protected by m_mutex. The lock is held briefly
 * (map lookups / iteration), so contention should be low.
 */
class EvictionPolicyEngine : public EvictionCandidateSelector {
public:
    EvictionPolicyEngine(std::shared_ptr< ChunkEvictionManager > eviction_mgr,
                         std::shared_ptr< NvmeChunkAllocator > nvme_alloc,
                         uint64_t nvme_total_capacity,
                         EvictionPolicyConfig config = {});

    ~EvictionPolicyEngine();

    EvictionPolicyEngine(const EvictionPolicyEngine&) = delete;
    EvictionPolicyEngine& operator=(const EvictionPolicyEngine&) = delete;

    // --- Access tracking (call on read/write hot path) ---

    void record_access(chunk_id_t chunk_id, uint64_t chunk_size);

    // --- Chunk lifecycle ---

    void record_hydration(chunk_id_t chunk_id);
    void remove_chunk(chunk_id_t chunk_id);
    void mark_on_s3(chunk_id_t chunk_id);

    // --- EvictionCandidateSelector interface ---

    bool select_eviction_candidate(chunk_id_t& out_chunk_id, uint64_t& out_chunk_size) override;

    // --- Background eviction monitor ---

    void start_monitor();
    void stop_monitor();

    /**
     * @brief Run one eviction cycle. Evicts coldest chunks until NVMe usage
     *        drops below the configured threshold.
     * @return Number of chunks evicted.
     */
    uint32_t run_eviction_cycle();

    // --- Queries ---

    uint32_t tracked_chunk_count() const;
    float current_nvme_usage_ratio() const;
    bool is_chunk_in_cooldown(chunk_id_t chunk_id) const;

    EvictionPolicyMetrics& metrics() { return m_metrics; }
    const EvictionPolicyConfig& config() const { return m_config; }

private:
    using clock_t = std::chrono::steady_clock;
    using time_point_t = clock_t::time_point;

    struct ChunkAccessInfo {
        uint64_t chunk_size{0};
        time_point_t last_access{clock_t::now()};
        time_point_t hydrated_at{time_point_t::min()};
        bool on_s3{false};
    };

    bool is_in_cooldown(const ChunkAccessInfo& info, time_point_t now) const;

    struct EvictionCandidate {
        chunk_id_t chunk_id;
        uint64_t chunk_size;
        time_point_t last_access;
    };
    std::vector< EvictionCandidate > select_coldest_candidates(uint32_t max_count);

    void monitor_loop();

    EvictionPolicyConfig m_config;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;
    std::shared_ptr< NvmeChunkAllocator > m_nvme_alloc;
    uint64_t m_nvme_total_capacity;

    mutable std::mutex m_mutex;
    std::unordered_map< chunk_id_t, ChunkAccessInfo > m_access_map;

    // Background monitor
    std::thread m_monitor_thread;
    std::atomic< bool > m_monitor_running{false};
    std::mutex m_monitor_mutex;
    std::condition_variable m_monitor_cv;

    mutable EvictionPolicyMetrics m_metrics;
};

} // namespace homestore
