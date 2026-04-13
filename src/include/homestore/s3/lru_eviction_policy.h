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

#include <chrono>
#include <cstdint>
#include <list>
#include <mutex>
#include <unordered_map>

#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/s3/chunk_hydration_manager.h>

namespace homestore {

class LruEvictionPolicyMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit LruEvictionPolicyMetrics() : sisl::MetricsGroupWrapper{"LruEvictionPolicy", "lru_eviction_policy"} {
        REGISTER_COUNTER(eviction_policy_candidates_selected, "Eviction candidates selected by LRU policy");
        REGISTER_COUNTER(eviction_policy_no_candidate, "Times no eviction candidate was available");
        REGISTER_COUNTER(eviction_policy_chunks_tracked, "Total chunks currently tracked");
        REGISTER_COUNTER(eviction_policy_access_updates, "Access timestamp updates");

        register_me_to_farm();
    }

    ~LruEvictionPolicyMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief LRU eviction policy for selecting chunks to evict from NVMe.
 *
 * Tracks per-chunk access recency using a doubly-linked list (most-recent
 * at back, least-recent at front). On select_eviction_candidate(), returns
 * the chunk at the front of the list (oldest access).
 *
 * Thread-safe: all operations are protected by a single mutex. The mutex is
 * not contended on the hot read path because record_access() is called
 * after the read completes, not inline.
 *
 * Chunks must be explicitly added via add_chunk() and removed via
 * remove_chunk(). Only chunks that are on NVMe and eligible for eviction
 * (i.e., backed by S3) should be tracked.
 */
class LruEvictionPolicy : public EvictionCandidateSelector {
public:
    LruEvictionPolicy() = default;
    ~LruEvictionPolicy() override = default;

    LruEvictionPolicy(const LruEvictionPolicy&) = delete;
    LruEvictionPolicy& operator=(const LruEvictionPolicy&) = delete;

    /**
     * @brief Start tracking a chunk in the LRU list.
     *
     * Call when a chunk becomes eligible for eviction (on NVMe + on S3).
     * If the chunk is already tracked, this is a no-op.
     */
    void add_chunk(chunk_id_t chunk_id, uint64_t chunk_size);

    /**
     * @brief Stop tracking a chunk (e.g., after eviction or removal).
     */
    void remove_chunk(chunk_id_t chunk_id);

    /**
     * @brief Record an access to a chunk, moving it to the back of the LRU list.
     *
     * Call on every read or write to a tracked chunk.
     */
    void record_access(chunk_id_t chunk_id);

    /**
     * @brief Select the least-recently-used chunk for eviction.
     *
     * @param[out] out_chunk_id   The chunk to evict
     * @param[out] out_chunk_size Size of the chunk
     * @return true if a candidate was found, false if no chunks are tracked
     */
    bool select_eviction_candidate(chunk_id_t& out_chunk_id, uint64_t& out_chunk_size) override;

    /**
     * @brief Get the number of chunks currently tracked.
     */
    uint32_t tracked_count() const;

    /**
     * @brief Check if a chunk is being tracked.
     */
    bool is_tracked(chunk_id_t chunk_id) const;

    LruEvictionPolicyMetrics& metrics() { return m_metrics; }

private:
    struct ChunkEntry {
        chunk_id_t chunk_id;
        uint64_t chunk_size;
    };

    using LruList = std::list< ChunkEntry >;
    using LruIterator = LruList::iterator;

    mutable std::mutex m_mutex;
    LruList m_lru_list;
    std::unordered_map< chunk_id_t, LruIterator > m_chunk_map;

    LruEvictionPolicyMetrics m_metrics;
};

} // namespace homestore
