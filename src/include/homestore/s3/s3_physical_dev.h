/*********************************************************************************
 * Copyright 2024 eBay Inc.
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
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/homestore_decl.hpp>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/pdev_s3_superblock.h>

namespace homestore {

class S3PhysicalDevMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit S3PhysicalDevMetrics(const std::string& name) : sisl::MetricsGroupWrapper{"S3PhysicalDev", name} {
        REGISTER_COUNTER(s3_write_count, "S3 pdev write count (cache inserts)");
        REGISTER_COUNTER(s3_read_count, "S3 pdev read count (via ChunkStore)");
        REGISTER_COUNTER(s3_dirty_cache_inserts, "Dirty cache insert count");
        REGISTER_COUNTER(s3_dirty_cache_drains, "Dirty cache drain count");
        REGISTER_COUNTER(s3_dirty_cache_bytes, "Current dirty cache size in bytes");
        REGISTER_COUNTER(s3_early_cp_flushes, "Forced early CP flush due to dirty cache overflow");
        REGISTER_COUNTER(s3_chunk_create_count, "S3 chunk create count");
        REGISTER_COUNTER(s3_chunk_remove_count, "S3 chunk remove count");

        REGISTER_HISTOGRAM(s3_write_latency, "S3 pdev write latency (cache insert) in us",
                           HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(s3_read_latency, "S3 pdev read latency (S3 fetch) in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~S3PhysicalDevMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief Callback for triggering an early CP flush when dirty cache exceeds threshold.
 */
class S3CpFlushCallback {
public:
    virtual ~S3CpFlushCallback() = default;
    virtual void trigger_early_cp_flush() = 0;
};

/**
 * @brief S3PhysicalDev — S3 as a native physical device in HomeStore.
 *
 * This is the core integration that makes S3 a native pdev. It implements
 * the pdev interface for S3 with an in-memory dirty cache:
 *
 * - write(): Stashes dirty block in memory cache, marks chunk dirty.
 *            No immediate S3 upload — ~instant since it's a cache insert.
 * - read():  Reads from S3 via ChunkStore::get()
 * - create_chunk(): Registers a chunk for S3 backing
 * - remove_chunk(): Removes a chunk's S3 data
 * - drain_dirty_cache(): Returns and clears dirty blocks for CP upload
 *
 * At CP time: dirty cache is drained into chunk_store->put(chunk_id, dirty_blocks)
 *
 * Memory bound: configurable dirty_cache_max_mb. If exceeded, triggers
 * early CP flush via callback.
 */
class S3PhysicalDev {
public:
    /**
     * @brief Construct an S3PhysicalDev.
     *
     * @param pdev_id         Unique pdev identifier
     * @param chunk_store     ChunkStore for S3 data access
     * @param s3_store        S3ObjectStore for superblock operations
     * @param volume_id       Volume ID for S3 key construction
     * @param dirty_cache_max_mb  Max dirty cache size before forcing early CP
     * @param cp_flush_cb     Callback to trigger early CP flush (optional)
     */
    S3PhysicalDev(uint32_t pdev_id,
                  std::shared_ptr< ChunkStore > chunk_store,
                  std::shared_ptr< S3ObjectStore > s3_store,
                  const std::string& volume_id,
                  uint64_t dirty_cache_max_mb = 4096,
                  std::shared_ptr< S3CpFlushCallback > cp_flush_cb = nullptr);

    ~S3PhysicalDev() = default;

    // Non-copyable, non-movable
    S3PhysicalDev(const S3PhysicalDev&) = delete;
    S3PhysicalDev& operator=(const S3PhysicalDev&) = delete;
    S3PhysicalDev(S3PhysicalDev&&) = delete;
    S3PhysicalDev& operator=(S3PhysicalDev&&) = delete;

    ///////////////////////// Write/Read Operations /////////////////////////

    /**
     * @brief Write data to the S3 pdev (cache insert, not S3 upload).
     *
     * Stashes a DirtyBlock in the in-memory cache. The data is a
     * sisl::byte_array (shared_ptr bump, not a data copy). At CP time
     * the cache is drained into chunk_store->put().
     *
     * This is ~instant since it's just a map insert + shared_ptr bump.
     *
     * @param chunk_id  Target chunk
     * @param offset    Offset within the chunk
     * @param data      Data to write (sisl::byte_array, shared_ptr)
     */
    void write(chunk_id_t chunk_id, offset_t offset, const sisl::byte_array& data);

    /**
     * @brief Read data from S3 via ChunkStore::get().
     *
     * @param chunk_id  Source chunk
     * @param offset    Offset within the chunk
     * @param size      Number of bytes to read
     * @return Future resolving to the data, or error
     */
    folly::Future< std::pair< S3Result, sisl::byte_array > >
    read(chunk_id_t chunk_id, offset_t offset, uint64_t size);

    ///////////////////////// Chunk Management /////////////////////////

    /**
     * @brief Register a chunk for S3 backing.
     *
     * @param chunk_id    Chunk ID
     * @param chunk_size  Size of the chunk
     * @param chunk_type  Type of chunk (DATA, INDEX, WAL, METABLK)
     * @param vdev_id     VDev this chunk belongs to
     */
    void create_chunk(chunk_id_t chunk_id, uint64_t chunk_size, S3ChunkType chunk_type, uint64_t vdev_id);

    /**
     * @brief Remove a chunk's S3 data and metadata.
     *
     * @param chunk_id  Chunk to remove
     */
    void remove_chunk(chunk_id_t chunk_id);

    /**
     * @brief Check if a chunk is registered on this pdev.
     */
    bool has_chunk(chunk_id_t chunk_id) const;

    ///////////////////////// Dirty Cache Management /////////////////////////

    /**
     * @brief Drain dirty blocks for a specific chunk.
     *
     * Returns all accumulated dirty blocks for the chunk since last drain
     * and clears them from the cache. Called at CP time.
     *
     * @param chunk_id  Chunk to drain
     * @return Vector of DirtyBlocks (empty if chunk has no dirty data)
     */
    std::vector< DirtyBlock > drain_dirty_cache(chunk_id_t chunk_id);

    /**
     * @brief Drain dirty blocks for ALL dirty chunks.
     *
     * Returns a map of chunk_id → dirty_blocks for all chunks with dirty data.
     * Called at CP time to flush everything.
     *
     * @return Map of chunk_id to vector of dirty blocks
     */
    std::map< chunk_id_t, std::vector< DirtyBlock > > drain_all_dirty_cache();

    /**
     * @brief Get the set of chunk IDs that have dirty data.
     */
    std::vector< chunk_id_t > get_dirty_chunks() const;

    /**
     * @brief Check if a chunk has dirty data pending.
     */
    bool is_chunk_dirty(chunk_id_t chunk_id) const;

    /**
     * @brief Get the current dirty cache size in bytes.
     */
    uint64_t dirty_cache_size_bytes() const { return m_dirty_cache_bytes.load(); }

    ///////////////////////// Superblock Operations /////////////////////////

    /**
     * @brief Write the pdev_s3 superblock to S3.
     *
     * Called last during CP — this is the S3 commit point.
     */
    S3Result write_superblock();

    /**
     * @brief Read and load the pdev_s3 superblock from S3.
     *
     * Used during startup/recovery.
     */
    S3Result load_superblock();

    /**
     * @brief Get a const reference to the current superblock.
     */
    const PdevS3Superblock& superblock() const { return m_superblock; }

    /**
     * @brief Get a mutable reference to the superblock (for updates).
     */
    PdevS3Superblock& superblock_mutable() { return m_superblock; }

    ///////////////////////// Getters /////////////////////////

    uint32_t pdev_id() const { return m_pdev_id; }
    const std::string& volume_id() const { return m_volume_id; }
    ChunkStore* chunk_store() { return m_chunk_store.get(); }
    S3PhysicalDevMetrics& metrics() { return m_metrics; }

private:
    /// Check if dirty cache exceeds threshold and trigger early CP if needed
    void check_dirty_cache_threshold();

    uint32_t m_pdev_id;
    std::shared_ptr< ChunkStore > m_chunk_store;
    std::shared_ptr< S3ObjectStore > m_s3_store;
    std::string m_volume_id;
    uint64_t m_dirty_cache_max_bytes;
    std::shared_ptr< S3CpFlushCallback > m_cp_flush_cb;

    /// Dirty cache: chunk_id → vector<DirtyBlock>
    mutable std::mutex m_dirty_cache_mutex;
    std::map< chunk_id_t, std::vector< DirtyBlock > > m_dirty_cache;
    std::atomic< uint64_t > m_dirty_cache_bytes{0};

    /// pdev_s3 superblock (in-memory)
    PdevS3Superblock m_superblock;

    S3PhysicalDevMetrics m_metrics;
};

} // namespace homestore
