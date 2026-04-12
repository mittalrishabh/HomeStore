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
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <folly/futures/Future.h>
#include <folly/futures/SharedPromise.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>

#include <homestore/s3/chunk_store.h>
#include <homestore/s3/s3_object_store.h>

namespace homestore {

/**
 * @brief Configuration for S3 chunk-to-key mapping.
 *
 * Bucket per cluster: homestore-<cluster_id>
 * Volume prefix: <volume_id>/
 * S3 key format:
 *   - No snapshots: <volume_id>/chunks/<chunk_id>/data.dat
 *   - With snapshots: <volume_id>/chunks/<chunk_id>/data_gen<N>.dat
 */
struct S3KeyMapper {
    std::string volume_id;

    /// Build the S3 key for a chunk's data object
    std::string chunk_data_key(chunk_id_t chunk_id, uint64_t generation = 0) const {
        if (generation == 0) {
            return volume_id + "/chunks/" + std::to_string(chunk_id) + "/data.dat";
        }
        return volume_id + "/chunks/" + std::to_string(chunk_id) + "/data_gen" + std::to_string(generation) + ".dat";
    }

    /// Build the S3 key prefix for listing all objects under a chunk
    std::string chunk_prefix(chunk_id_t chunk_id) const {
        return volume_id + "/chunks/" + std::to_string(chunk_id) + "/";
    }
};

/**
 * @brief Callback interface for reading chunk data from NVMe.
 *
 * FullChunkStore needs to read the full chunk from NVMe during put()
 * since v1 uploads the entire chunk. This callback isolates the
 * FullChunkStore from direct pdev/vdev coupling.
 */
class NvmeChunkReader {
public:
    virtual ~NvmeChunkReader() = default;

    /// Read the entire chunk data from NVMe
    virtual std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t chunk_id, uint64_t chunk_size) = 0;
};

/**
 * @brief v1 ChunkStore implementation — full chunk upload/download.
 *
 * put():     Reads full chunk from NVMe, uploads as single S3 object.
 *            Ignores dirty_blocks granularity (v3 will use them).
 * get():     Downloads full S3 object, caches locally, returns data at
 *            requested offset.
 * compact(): No-op (already a single object).
 * recover(): Downloads single S3 object for NVMe restore.
 * describe(): Returns the S3 key for the chunk.
 */
class FullChunkStore : public ChunkStore {
public:
    /// Default max cache size: 512 MB
    static constexpr uint64_t DEFAULT_MAX_CACHE_BYTES = 512ULL * 1024 * 1024;

    /**
     * @brief Construct a FullChunkStore.
     *
     * @param s3_store          S3 object store for actual S3 I/O
     * @param nvme_reader       Callback to read full chunks from NVMe (for put)
     * @param key_mapper        S3 key mapping configuration
     * @param max_cache_bytes   Max LRU cache size in bytes (0 = unlimited)
     */
    FullChunkStore(std::shared_ptr< S3ObjectStore > s3_store,
                   std::shared_ptr< NvmeChunkReader > nvme_reader,
                   S3KeyMapper key_mapper,
                   uint64_t max_cache_bytes = DEFAULT_MAX_CACHE_BYTES);

    ~FullChunkStore() override = default;

    // ChunkStore interface
    S3Result put(chunk_id_t chunk_id, const std::vector< DirtyBlock >& dirty_blocks,
                 uint64_t chunk_size) override;

    folly::Future< std::pair< S3Result, sisl::byte_array > >
    get(chunk_id_t chunk_id, offset_t offset, uint64_t size) override;

    S3Result compact(chunk_id_t chunk_id) override;
    ChunkState recover(chunk_id_t chunk_id) override;
    ChunkMetadata describe(chunk_id_t chunk_id) override;

    /// Invalidate the local cache for a specific chunk
    void invalidate_cache(chunk_id_t chunk_id);

    /// Invalidate all cached chunks
    void invalidate_all_cache();

    /// Get current cache size in bytes
    uint64_t cache_size_bytes() const;

    /// Get max cache size in bytes
    uint64_t max_cache_bytes() const { return m_max_cache_bytes; }

private:
    /// Fetch the full chunk from S3 and cache it locally (with dedup)
    folly::Future< std::pair< S3Result, sisl::byte_array > > fetch_and_cache(chunk_id_t chunk_id);

    /// Look up chunk in local cache (returns nullptr if not cached); promotes to MRU
    sisl::byte_array lookup_cache(chunk_id_t chunk_id);

    /// Insert into LRU cache, evicting oldest entries if over budget
    void insert_cache(chunk_id_t chunk_id, const sisl::byte_array& data);

    /// Evict oldest entries until cache is within budget
    void evict_cache_locked();

    std::shared_ptr< S3ObjectStore > m_s3_store;
    std::shared_ptr< NvmeChunkReader > m_nvme_reader;
    S3KeyMapper m_key_mapper;
    uint64_t m_max_cache_bytes;

    /// LRU cache: chunk_id → data, with LRU eviction.
    /// m_lru_list is ordered most-recent-first; m_cache_map points into it.
    mutable std::mutex m_cache_mutex;
    using LruEntry = std::pair< chunk_id_t, sisl::byte_array >;
    std::list< LruEntry > m_lru_list;
    std::unordered_map< chunk_id_t, std::list< LruEntry >::iterator > m_cache_map;
    uint64_t m_cache_bytes{0};

    /// In-flight dedup: prevents thundering herd on concurrent cache misses
    /// for the same chunk_id. Second caller waits on the first's download.
    mutable std::mutex m_inflight_mutex;
    std::unordered_map< chunk_id_t, std::shared_ptr< folly::SharedPromise< std::pair< S3Result, sisl::byte_array > > > >
        m_inflight_fetches;
};

} // namespace homestore
