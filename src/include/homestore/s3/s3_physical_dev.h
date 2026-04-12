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
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <folly/futures/Future.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/homestore_decl.hpp>
#include <homestore/s3/s3_object_store.h>

namespace homestore {

// Forward declarations — ChunkStore defined by code-dev-2 (Task 1.3)
class ChunkStore;

using chunk_id_t = uint32_t;
using offset_t = uint64_t;

/// A dirty block cached in memory from a write to the S3 pdev.
/// The data is held as sisl::byte_array (shared_ptr<io_blob_safe>),
/// so caching is a pointer bump, not a data copy.
struct DirtyBlock {
    offset_t offset{0};       ///< Offset within the chunk
    sisl::byte_array data;    ///< Shared ptr to aligned, RAII-managed buffer
};

/// Metrics for the S3 physical device.
class S3PhysicalDevMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit S3PhysicalDevMetrics() : sisl::MetricsGroupWrapper{"S3PhysicalDev", "s3_pdev"} {
        REGISTER_COUNTER(s3_pdev_writes, "Total writes to S3 pdev (dirty cache inserts)");
        REGISTER_COUNTER(s3_pdev_reads, "Total reads from S3 pdev (via ChunkStore)");
        REGISTER_COUNTER(s3_pdev_dirty_cache_bytes, "Current dirty cache size in bytes");
        REGISTER_COUNTER(s3_pdev_chunks_registered, "Number of chunks registered with S3 pdev");
        REGISTER_COUNTER(s3_pdev_early_cp_flushes, "Early CP flushes triggered by dirty cache pressure");

        REGISTER_HISTOGRAM(s3_pdev_write_latency_us, "S3 pdev write (cache insert) latency in us",
                           HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(s3_pdev_read_latency_us, "S3 pdev read (S3 fetch) latency in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~S3PhysicalDevMetrics() { deregister_me_from_farm(); }
};

/// Callback type for early CP flush requests.
using early_cp_flush_cb_t = std::function< void() >;

/**
 * @brief S3 Physical Device — models S3 as a native pdev in HomeStore's
 *        device hierarchy.
 *
 * On write: stashes dirty blocks in an in-memory cache (near-instant,
 *           shared_ptr bump). No S3 I/O on the write path.
 *
 * On read:  delegates to ChunkStore::get() which handles full-chunk
 *           download (v1) or SST lookup (v3) transparently.
 *
 * At CP:    drain_dirty_cache() returns all dirty blocks for a chunk,
 *           which the CP hook uploads via ChunkStore::put().
 *
 * Memory bound: configurable dirty_cache_max_mb. If exceeded, triggers
 *               an early CP flush via the registered callback.
 *
 * This class does NOT inherit from PhysicalDev — it is a separate pdev
 * type with its own interface, registered alongside NVMe pdevs in
 * DeviceManager.
 */
class S3PhysicalDev {
public:
    /**
     * @brief Construct an S3 physical device.
     *
     * @param s3_store       S3 object store for reads/writes
     * @param chunk_store    ChunkStore for read-path abstraction (can be set later)
     * @param dirty_cache_max_mb  Max dirty cache size before triggering early CP
     */
    S3PhysicalDev(shared< S3ObjectStore > s3_store, uint64_t dirty_cache_max_mb);

    S3PhysicalDev(const S3PhysicalDev&) = delete;
    S3PhysicalDev(S3PhysicalDev&&) = delete;
    S3PhysicalDev& operator=(const S3PhysicalDev&) = delete;
    S3PhysicalDev& operator=(S3PhysicalDev&&) = delete;
    ~S3PhysicalDev() = default;

    // ─── ChunkStore lifecycle ──────────────────────────────────────────

    /// Set the ChunkStore implementation (called after construction,
    /// once ChunkStore is available).
    void set_chunk_store(shared< ChunkStore > cs);

    /// Register an early CP flush callback (called when dirty cache
    /// exceeds the configured limit).
    void set_early_cp_flush_cb(early_cp_flush_cb_t cb);

    // ─── Write path ────────────────────────────────────────────────────

    /**
     * @brief Cache a write in the dirty block map.
     *
     * This is the S3 pdev's "write" — it stashes the data as a DirtyBlock
     * in memory. Near-instant (shared_ptr bump). Called by VirtualDev's
     * mirrored write alongside the NVMe pdev's synchronous write.
     *
     * @param chunk_id  Chunk being written to
     * @param offset    Byte offset within the chunk
     * @param data      Data to cache (shared_ptr — no copy)
     * @return folly::Future<std::error_code> — always succeeds immediately
     */
    folly::Future< std::error_code > write(chunk_id_t chunk_id, offset_t offset, sisl::byte_array data);

    // ─── Read path ─────────────────────────────────────────────────────

    /**
     * @brief Read data from S3 via ChunkStore.
     *
     * Delegates to ChunkStore::get() which handles v1 (full chunk download)
     * or v3 (SST lookup) transparently.
     *
     * @param chunk_id  Chunk to read from
     * @param offset    Byte offset within the chunk
     * @param size      Number of bytes to read
     * @return Future resolving to the data or error
     */
    folly::Future< std::pair< std::error_code, sisl::byte_array > >
    read(chunk_id_t chunk_id, offset_t offset, uint64_t size);

    // ─── Chunk management ──────────────────────────────────────────────

    /// Register a chunk for S3 backing.
    void create_chunk(chunk_id_t chunk_id, uint64_t chunk_size);

    /// Remove a chunk's S3 data and deregister it.
    void remove_chunk(chunk_id_t chunk_id);

    /// Check if a chunk is registered.
    bool has_chunk(chunk_id_t chunk_id) const;

    // ─── CP interface ──────────────────────────────────────────────────

    /**
     * @brief Drain the dirty cache for a specific chunk.
     *
     * Returns and clears all dirty blocks accumulated since the last CP
     * for the given chunk. Called by the CP hook before uploading to S3.
     *
     * @param chunk_id  Chunk to drain
     * @return Vector of dirty blocks (moved out — cache is cleared)
     */
    std::vector< DirtyBlock > drain_dirty_cache(chunk_id_t chunk_id);

    /**
     * @brief Get all chunk IDs that have dirty data.
     *
     * @return Set of chunk IDs with pending dirty blocks
     */
    std::unordered_set< chunk_id_t > get_dirty_chunk_ids() const;

    /**
     * @brief Check if any chunks have dirty data.
     */
    bool has_dirty_data() const;

    // ─── Stats ─────────────────────────────────────────────────────────

    /// Current dirty cache size in bytes.
    uint64_t dirty_cache_bytes() const { return m_dirty_cache_bytes.load(std::memory_order_relaxed); }

    /// Max dirty cache size in bytes.
    uint64_t dirty_cache_max_bytes() const { return m_dirty_cache_max_bytes; }

    /// Number of registered chunks.
    uint32_t num_chunks() const;

    /// Metrics accessor.
    S3PhysicalDevMetrics& metrics() { return m_metrics; }

private:
    /// Check dirty cache size and trigger early CP if needed.
    void check_dirty_cache_pressure();

    shared< S3ObjectStore > m_s3_store;
    shared< ChunkStore > m_chunk_store;
    uint64_t m_dirty_cache_max_bytes;

    /// Per-chunk dirty block cache, protected by m_dirty_mtx.
    mutable std::mutex m_dirty_mtx;
    std::unordered_map< chunk_id_t, std::vector< DirtyBlock > > m_dirty_cache;
    std::atomic< uint64_t > m_dirty_cache_bytes{0};

    /// Registered chunks and their sizes.
    mutable std::mutex m_chunk_mtx;
    std::unordered_map< chunk_id_t, uint64_t > m_registered_chunks; // chunk_id -> chunk_size

    early_cp_flush_cb_t m_early_cp_flush_cb;
    S3PhysicalDevMetrics m_metrics;
};

} // namespace homestore
