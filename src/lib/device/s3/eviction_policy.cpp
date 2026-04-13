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

#include <algorithm>
#include <chrono>

#include <homestore/s3/eviction_policy.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

EvictionPolicyEngine::EvictionPolicyEngine(std::shared_ptr< ChunkEvictionManager > eviction_mgr,
                                           std::shared_ptr< NvmeChunkAllocator > nvme_alloc,
                                           uint64_t nvme_total_capacity,
                                           EvictionPolicyConfig config)
    : m_config{std::move(config)}
    , m_eviction_mgr{std::move(eviction_mgr)}
    , m_nvme_alloc{std::move(nvme_alloc)}
    , m_nvme_total_capacity{nvme_total_capacity} {
    RELEASE_ASSERT(m_eviction_mgr != nullptr, "ChunkEvictionManager must not be null");
    RELEASE_ASSERT(m_nvme_alloc != nullptr, "NvmeChunkAllocator must not be null");
    RELEASE_ASSERT(m_nvme_total_capacity > 0, "NVMe total capacity must be > 0");
    RELEASE_ASSERT(m_config.nvme_capacity_threshold > 0.0f && m_config.nvme_capacity_threshold <= 1.0f,
                   "nvme_capacity_threshold must be in (0.0, 1.0]");
    LOGDEBUGMOD(s3, "EvictionPolicyEngine created: policy={} threshold={:.2f} cooldown_secs={} monitor_interval={}s",
                m_config.policy, m_config.nvme_capacity_threshold, m_config.cooldown_secs,
                m_config.monitor_interval_secs);
}

EvictionPolicyEngine::~EvictionPolicyEngine() { stop_monitor(); }

void EvictionPolicyEngine::record_access(chunk_id_t chunk_id, uint64_t chunk_size) {
    std::lock_guard lock{m_mutex};
    auto& info = m_access_map[chunk_id];
    info.chunk_size = chunk_size;
    info.last_access = clock_t::now();
    COUNTER_INCREMENT(m_metrics, lru_access_records, 1);
}

void EvictionPolicyEngine::record_hydration(chunk_id_t chunk_id) {
    std::lock_guard lock{m_mutex};
    auto it = m_access_map.find(chunk_id);
    if (it != m_access_map.end()) {
        it->second.hydrated_at = clock_t::now();
        it->second.last_access = clock_t::now();
    }
}

void EvictionPolicyEngine::remove_chunk(chunk_id_t chunk_id) {
    std::lock_guard lock{m_mutex};
    m_access_map.erase(chunk_id);
}

void EvictionPolicyEngine::mark_on_s3(chunk_id_t chunk_id) {
    std::lock_guard lock{m_mutex};
    auto it = m_access_map.find(chunk_id);
    if (it != m_access_map.end()) {
        it->second.on_s3 = true;
    }
}

bool EvictionPolicyEngine::select_eviction_candidate(chunk_id_t& out_chunk_id, uint64_t& out_chunk_size) {
    auto candidates = select_coldest_candidates(1);
    if (candidates.empty()) {
        COUNTER_INCREMENT(m_metrics, lru_no_candidate_available, 1);
        return false;
    }
    out_chunk_id = candidates[0].chunk_id;
    out_chunk_size = candidates[0].chunk_size;
    COUNTER_INCREMENT(m_metrics, lru_candidates_selected, 1);
    LOGDEBUGMOD(s3, "select_eviction_candidate: selected chunk_id={} size={}", out_chunk_id, out_chunk_size);
    return true;
}

void EvictionPolicyEngine::start_monitor() {
    bool expected = false;
    if (!m_monitor_running.compare_exchange_strong(expected, true)) {
        return;
    }

    m_monitor_thread = std::thread([this] { monitor_loop(); });
    LOGDEBUGMOD(s3, "EvictionPolicyEngine monitor started (interval={}s)", m_config.monitor_interval_secs);
}

void EvictionPolicyEngine::stop_monitor() {
    bool expected = true;
    if (!m_monitor_running.compare_exchange_strong(expected, false)) {
        return;
    }

    m_monitor_cv.notify_all();
    if (m_monitor_thread.joinable()) {
        m_monitor_thread.join();
    }
    LOGDEBUGMOD(s3, "EvictionPolicyEngine monitor stopped");
}

uint32_t EvictionPolicyEngine::run_eviction_cycle() {
    auto cycle_start = clock_t::now();

    float usage = current_nvme_usage_ratio();
    if (usage < m_config.nvme_capacity_threshold) {
        COUNTER_INCREMENT(m_metrics, eviction_cycles_skipped, 1);
        return 0;
    }

    COUNTER_INCREMENT(m_metrics, eviction_cycles_run, 1);
    LOGDEBUGMOD(s3, "run_eviction_cycle: NVMe usage {:.2f} >= threshold {:.2f}",
                usage, m_config.nvme_capacity_threshold);

    uint32_t evicted = 0;
    constexpr uint32_t MAX_EVICTIONS_PER_CYCLE = 32;

    auto candidates = select_coldest_candidates(MAX_EVICTIONS_PER_CYCLE);

    for (const auto& c : candidates) {
        if (current_nvme_usage_ratio() < m_config.nvme_capacity_threshold) {
            break;
        }

        auto result = m_eviction_mgr->evict_chunk(c.chunk_id, c.chunk_size);
        if (result == EvictionResult::SUCCESS) {
            ++evicted;
            COUNTER_INCREMENT(m_metrics, chunks_evicted_by_monitor, 1);
            LOGDEBUGMOD(s3, "run_eviction_cycle: evicted chunk_id={}", c.chunk_id);
        } else {
            COUNTER_INCREMENT(m_metrics, chunks_eviction_failed_by_monitor, 1);
            LOGDEBUGMOD(s3, "run_eviction_cycle: eviction failed for chunk_id={}: {}",
                        c.chunk_id, to_string(result));
        }
    }

    auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
        clock_t::now() - cycle_start).count();
    HISTOGRAM_OBSERVE(m_metrics, eviction_cycle_latency_us, elapsed_us);

    LOGDEBUGMOD(s3, "run_eviction_cycle: evicted {} chunks in {}us", evicted, elapsed_us);
    return evicted;
}

uint32_t EvictionPolicyEngine::tracked_chunk_count() const {
    std::lock_guard lock{m_mutex};
    return static_cast< uint32_t >(m_access_map.size());
}

float EvictionPolicyEngine::current_nvme_usage_ratio() const {
    if (m_nvme_total_capacity == 0) return 0.0f;
    auto free = m_nvme_alloc->free_nvme_space_bytes();
    auto used = (free >= m_nvme_total_capacity) ? 0UL : m_nvme_total_capacity - free;
    return static_cast< float >(used) / static_cast< float >(m_nvme_total_capacity);
}

bool EvictionPolicyEngine::is_chunk_in_cooldown(chunk_id_t chunk_id) const {
    std::lock_guard lock{m_mutex};
    auto it = m_access_map.find(chunk_id);
    if (it == m_access_map.end()) return false;
    return is_in_cooldown(it->second, clock_t::now());
}

std::vector< EvictionPolicyEngine::EvictionCandidate >
EvictionPolicyEngine::select_coldest_candidates(uint32_t max_count) {
    std::vector< EvictionCandidate > eligible;

    std::lock_guard lock{m_mutex};
    auto now = clock_t::now();

    for (const auto& [chunk_id, info] : m_access_map) {
        COUNTER_INCREMENT(m_metrics, lru_candidates_scanned, 1);

        if (!info.on_s3) {
            continue;
        }

        if (m_eviction_mgr->is_chunk_evicted(chunk_id)) {
            COUNTER_INCREMENT(m_metrics, lru_candidates_skipped_not_on_nvme, 1);
            continue;
        }

        if (m_eviction_mgr->is_write_blocked(chunk_id)) {
            continue;
        }

        if (is_in_cooldown(info, now)) {
            COUNTER_INCREMENT(m_metrics, lru_candidates_skipped_cooldown, 1);
            continue;
        }

        eligible.push_back(EvictionCandidate{chunk_id, info.chunk_size, info.last_access});
    }

    std::sort(eligible.begin(), eligible.end(),
              [](const EvictionCandidate& a, const EvictionCandidate& b) {
                  return a.last_access < b.last_access;
              });

    if (eligible.size() > max_count) {
        eligible.resize(max_count);
    }

    return eligible;
}

bool EvictionPolicyEngine::is_in_cooldown(const ChunkAccessInfo& info, time_point_t now) const {
    if (info.hydrated_at == time_point_t::min()) return false;

    auto elapsed_secs = std::chrono::duration_cast< std::chrono::seconds >(now - info.hydrated_at).count();
    return elapsed_secs < static_cast< int64_t >(m_config.cooldown_secs);
}

void EvictionPolicyEngine::monitor_loop() {
    while (m_monitor_running.load(std::memory_order_relaxed)) {
        {
            std::unique_lock lock{m_monitor_mutex};
            m_monitor_cv.wait_for(lock, std::chrono::seconds{m_config.monitor_interval_secs}, [this] {
                return !m_monitor_running.load(std::memory_order_relaxed);
            });
        }

        if (!m_monitor_running.load(std::memory_order_relaxed)) break;

        run_eviction_cycle();
    }
}

} // namespace homestore
