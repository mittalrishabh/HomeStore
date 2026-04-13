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
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/s3/chunk_eviction_manager.h>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/tiered_read_handler.h>

namespace homestore {

enum class HydrationResult : uint8_t {
    SUCCESS,
    ALREADY_ON_NVME,
    ALREADY_HYDRATING,
    S3_READ_FAILED,
    NVME_WRITE_FAILED,
    NVME_FULL,
    EVICTION_FOR_SPACE_FAILED,
    CHUNK_NOT_ON_S3,
    SHUTTING_DOWN
};

std::string to_string(HydrationResult r);

class ChunkHydrationMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit ChunkHydrationMetrics() : sisl::MetricsGroupWrapper{"ChunkHydration", "chunk_hydration"} {
        REGISTER_COUNTER(hydration_scheduled_count, "Hydration requests scheduled");
        REGISTER_COUNTER(hydration_success_count, "Successful chunk hydrations");
        REGISTER_COUNTER(hydration_skipped_already_on_nvme, "Hydrations skipped: already on NVMe");
        REGISTER_COUNTER(hydration_skipped_already_hydrating, "Hydrations skipped: already in progress");
        REGISTER_COUNTER(hydration_s3_read_failures, "Hydrations failed: S3 read error");
        REGISTER_COUNTER(hydration_nvme_write_failures, "Hydrations failed: NVMe write error");
        REGISTER_COUNTER(hydration_eviction_for_space_count, "Evictions triggered to make space for hydration");
        REGISTER_COUNTER(hydration_eviction_for_space_failures, "Failed evictions for hydration space");
        REGISTER_COUNTER(hydration_nvme_bytes_written, "NVMe bytes written by hydration");

        REGISTER_HISTOGRAM(hydration_latency_us, "Chunk hydration latency in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~ChunkHydrationMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief Callback interface for NVMe space allocation during hydration.
 *
 * Separates ChunkHydrationManager from direct PhysicalDev coupling.
 */
class NvmeChunkAllocator {
public:
    virtual ~NvmeChunkAllocator() = default;

    /// Allocate NVMe space for a chunk. Returns error if NVMe is full.
    virtual std::error_code allocate_nvme_chunk(chunk_id_t chunk_id, uint64_t chunk_size) = 0;

    /// Get the total free NVMe space in bytes.
    virtual uint64_t free_nvme_space_bytes() const = 0;
};

/**
 * @brief Selects eviction candidates when NVMe is full during hydration.
 *
 * Default implementation uses simple LRU-like selection from the set of
 * evicted-eligible chunks.
 */
class EvictionCandidateSelector {
public:
    virtual ~EvictionCandidateSelector() = default;

    /// Select a chunk to evict to make room. Returns false if none available.
    virtual bool select_eviction_candidate(chunk_id_t& out_chunk_id, uint64_t& out_chunk_size) = 0;
};

/**
 * @brief ChunkHydrationManager — async, chunk-granularity cache-on-read.
 *
 * When a read hits a chunk on S3 only, the triggering read returns
 * immediately from S3. In the background, the hydration manager:
 *
 * 1. Downloads the full chunk from S3 via ChunkStore::recover()
 * 2. Allocates NVMe space (evicting a cold chunk if needed)
 * 3. Writes the chunk data to NVMe
 * 4. Updates chunk state: on_nvme = true (via ChunkEvictionManager::mark_hydrated)
 *
 * After hydration, future reads go through the NVMe fast path.
 *
 * ## Deduplication:
 *
 * Concurrent hydration requests for the same chunk are deduplicated — only
 * the first request triggers the actual download. Subsequent requests are
 * no-ops (ALREADY_HYDRATING).
 *
 * ## Thread Safety:
 *
 * schedule_hydration() is lock-free on the hot path (just enqueues work).
 * Background worker(s) process the queue. Safe to call from any thread.
 */
class ChunkHydrationManager {
public:
    /**
     * @param chunk_store      ChunkStore for S3 data recovery
     * @param nvme_io          NVMe device I/O for writing hydrated data
     * @param nvme_alloc       NVMe space allocator
     * @param eviction_mgr     Eviction manager for marking hydrated + evicting for space
     * @param eviction_selector  Selects chunks to evict when NVMe is full (optional)
     * @param num_workers      Number of background hydration threads
     */
    ChunkHydrationManager(std::shared_ptr< ChunkStore > chunk_store,
                          std::shared_ptr< NvmeDeviceIO > nvme_io,
                          std::shared_ptr< NvmeChunkAllocator > nvme_alloc,
                          std::shared_ptr< ChunkEvictionManager > eviction_mgr,
                          std::shared_ptr< EvictionCandidateSelector > eviction_selector = nullptr,
                          uint32_t num_workers = 1);

    ~ChunkHydrationManager();

    ChunkHydrationManager(const ChunkHydrationManager&) = delete;
    ChunkHydrationManager& operator=(const ChunkHydrationManager&) = delete;

    /**
     * @brief Schedule async hydration of a chunk from S3 to NVMe.
     *
     * Non-blocking. The triggering read should return S3 data immediately;
     * this method enqueues the chunk for background hydration.
     *
     * @param chunk_id    Chunk to hydrate
     * @param chunk_size  Size of the chunk (for NVMe allocation)
     * @return ALREADY_ON_NVME if chunk is on NVMe, ALREADY_HYDRATING if
     *         hydration is in progress, or SUCCESS if enqueued.
     *         SHUTTING_DOWN if the manager is stopping.
     */
    HydrationResult schedule_hydration(chunk_id_t chunk_id, uint64_t chunk_size);

    /**
     * @brief Synchronously hydrate a chunk (blocking).
     *
     * Downloads from S3, allocates NVMe, writes, marks hydrated.
     * Used for pre-warming or on-demand recovery.
     */
    HydrationResult hydrate_sync(chunk_id_t chunk_id, uint64_t chunk_size);

    /**
     * @brief Check if a chunk is currently being hydrated.
     */
    bool is_hydrating(chunk_id_t chunk_id) const;

    /**
     * @brief Get the number of pending hydration requests.
     */
    uint32_t pending_count() const;

    /**
     * @brief Shutdown the hydration manager. Waits for in-flight work.
     */
    void shutdown();

    ChunkHydrationMetrics& metrics() { return m_metrics; }

private:
    struct HydrationRequest {
        chunk_id_t chunk_id;
        uint64_t chunk_size;
    };

    HydrationResult do_hydrate(chunk_id_t chunk_id, uint64_t chunk_size);
    bool ensure_nvme_space(uint64_t needed_bytes);
    void worker_loop();

    std::shared_ptr< ChunkStore > m_chunk_store;
    std::shared_ptr< NvmeDeviceIO > m_nvme_io;
    std::shared_ptr< NvmeChunkAllocator > m_nvme_alloc;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;
    std::shared_ptr< EvictionCandidateSelector > m_eviction_selector;

    // Work queue
    mutable std::mutex m_queue_mutex;
    std::condition_variable m_queue_cv;
    std::queue< HydrationRequest > m_queue;

    // In-flight dedup set
    mutable std::mutex m_inflight_mutex;
    std::unordered_set< chunk_id_t > m_inflight;

    // Workers
    std::vector< std::thread > m_workers;
    std::atomic< bool > m_shutdown{false};

    ChunkHydrationMetrics m_metrics;
};

} // namespace homestore
