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

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/s3/chunk_store.h>
#include <homestore/s3/s3_physical_dev.h>

namespace homestore {

enum class EvictionResult : uint8_t {
    SUCCESS,
    NOT_ON_S3,
    ALREADY_EVICTED,
    EVICTION_IN_PROGRESS,
    HAS_DIRTY_DATA,
    FLUSH_FAILED,
    INTERNAL_ERROR
};

std::string to_string(EvictionResult r);

/**
 * @brief Callback interface for NVMe chunk lifecycle operations.
 *
 * Isolates ChunkEvictionManager from direct PhysicalDev/VirtualDev coupling
 * so it can be tested independently.
 */
class NvmeChunkManager {
public:
    virtual ~NvmeChunkManager() = default;

    /// Check if a chunk currently resides on NVMe
    virtual bool is_chunk_on_nvme(chunk_id_t chunk_id) const = 0;

    /// Release NVMe space for a chunk (free the allocation, mark unavailable)
    virtual std::error_code release_nvme_chunk(chunk_id_t chunk_id) = 0;
};

class ChunkEvictionMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit ChunkEvictionMetrics() : sisl::MetricsGroupWrapper{"ChunkEviction", "chunk_eviction"} {
        REGISTER_COUNTER(eviction_success_count, "Successful chunk evictions");
        REGISTER_COUNTER(eviction_rejected_not_on_s3, "Evictions rejected: chunk not on S3");
        REGISTER_COUNTER(eviction_rejected_already_evicted, "Evictions rejected: already evicted");
        REGISTER_COUNTER(eviction_rejected_in_progress, "Evictions rejected: eviction in progress");
        REGISTER_COUNTER(eviction_rejected_dirty, "Evictions rejected: has dirty data");
        REGISTER_COUNTER(eviction_flush_failures, "Evictions failed due to S3 flush failure");
        REGISTER_COUNTER(eviction_errors, "Eviction internal errors");
        REGISTER_COUNTER(eviction_nvme_bytes_freed, "NVMe bytes freed by eviction");
        REGISTER_COUNTER(writes_blocked_by_eviction, "Writes blocked/rejected during eviction");

        REGISTER_HISTOGRAM(eviction_latency_us, "Chunk eviction latency in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~ChunkEvictionMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief ChunkEvictionManager — removes cold chunks from NVMe while keeping
 *        them on S3.
 *
 * ## Eviction Flow:
 *
 * 1. Verify chunk.on_s3 == true (precondition)
 * 2. Mark chunk as evicting (blocks new NVMe writes)
 * 3. Flush any pending dirty data to S3 via ChunkStore::put()
 * 4. Release NVMe space via NvmeChunkManager::release_nvme_chunk()
 * 5. Mark chunk as evicted (on_nvme = false)
 * 6. Clear evicting flag
 *
 * After eviction:
 * - Reads go through S3 via TieredReadHandler (already handles S3 fallback)
 * - Bitmap stays valid (unchanged — stays in memory / MetaSvc)
 * - New writes to evicted chunk's blocks should trigger hydration first
 *
 * ## Thread Safety:
 *
 * Per-chunk state is protected by a shared_mutex:
 * - evict_chunk() takes exclusive lock on the chunk
 * - is_write_blocked() takes shared lock (called on write hot path)
 * - Multiple chunks can be evicted concurrently
 */
class ChunkEvictionManager {
public:
    ChunkEvictionManager(S3PhysicalDev* s3_pdev,
                         std::shared_ptr< NvmeChunkManager > nvme_mgr);

    ~ChunkEvictionManager() = default;

    ChunkEvictionManager(const ChunkEvictionManager&) = delete;
    ChunkEvictionManager& operator=(const ChunkEvictionManager&) = delete;

    /**
     * @brief Evict a chunk from NVMe. Chunk must already be durable on S3.
     *
     * This is a blocking operation — meant to be called from a background
     * thread, not the hot write path.
     *
     * @param chunk_id  Chunk to evict
     * @param chunk_size  Size of the chunk (for dirty flush)
     * @return EvictionResult indicating success or reason for failure
     */
    EvictionResult evict_chunk(chunk_id_t chunk_id, uint64_t chunk_size);

    /**
     * @brief Check if writes to a chunk are currently blocked (eviction in progress).
     *
     * Called on the write path to prevent new NVMe writes during eviction.
     */
    bool is_write_blocked(chunk_id_t chunk_id) const;

    /**
     * @brief Check if a chunk has been evicted from NVMe.
     */
    bool is_chunk_evicted(chunk_id_t chunk_id) const;

    /**
     * @brief Mark a chunk as no longer evicted (e.g., after hydration restores it to NVMe).
     */
    void mark_hydrated(chunk_id_t chunk_id);

    ChunkEvictionMetrics& metrics() { return m_metrics; }

private:
    enum class ChunkEvictionState : uint8_t {
        NORMAL,     // chunk is on NVMe, not being evicted
        EVICTING,   // eviction in progress — writes blocked
        EVICTED     // chunk removed from NVMe, S3 only
    };

    struct ChunkState {
        mutable std::shared_mutex mtx;
        ChunkEvictionState state{ChunkEvictionState::NORMAL};
    };

    ChunkState& get_or_create_state(chunk_id_t chunk_id);
    const ChunkState* find_state(chunk_id_t chunk_id) const;
    ChunkState* find_state_mut(chunk_id_t chunk_id);

    S3PhysicalDev* m_s3_pdev;
    std::shared_ptr< NvmeChunkManager > m_nvme_mgr;

    mutable std::mutex m_states_mutex;
    std::unordered_map< chunk_id_t, std::unique_ptr< ChunkState > > m_chunk_states;

    mutable ChunkEvictionMetrics m_metrics;
};

} // namespace homestore
