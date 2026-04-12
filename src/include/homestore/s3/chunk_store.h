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
#include <string>
#include <vector>

#include <folly/futures/Future.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>

#include <homestore/s3/s3_object_store.h>

namespace homestore {

using chunk_id_t = uint32_t;
using offset_t = uint64_t;

/// A dirty block represents a write that has been cached in memory and needs
/// to be flushed to S3 at checkpoint time. The data is held as a
/// sisl::byte_array (shared_ptr<io_blob_safe>) so caching is a pointer bump,
/// not a data copy.
struct DirtyBlock {
    offset_t offset{0};       ///< Offset within the chunk
    sisl::byte_array data;    ///< Shared ptr to aligned, RAII-managed buffer
};

/// Metadata about a chunk's S3 representation
struct ChunkMetadata {
    chunk_id_t chunk_id{0};
    std::string s3_key;
    uint64_t s3_object_size{0};
    uint64_t generation{0};
};

/// State of a chunk recovered from S3
struct ChunkState {
    chunk_id_t chunk_id{0};
    sisl::byte_array data;     ///< Full chunk data downloaded from S3
    uint64_t chunk_size{0};
    uint64_t generation{0};
    bool valid{false};
};

/**
 * @brief Abstract interface for S3 chunk storage.
 *
 * This is the v3-forward abstraction that ALL S3 data access goes through.
 * The interface captures dirty block info so that future implementations
 * (e.g. SSTChunkStore for v3) can use fine-grained dirty block data for
 * efficient SST file construction.
 *
 * v1 implementation (FullChunkStore) ignores dirty_blocks granularity and
 * uploads/downloads full chunks. The interface is the contract; v1 is just
 * the first backend.
 */
class ChunkStore {
public:
    virtual ~ChunkStore() = default;

    /**
     * @brief Upload a chunk's dirty data to S3.
     *
     * Called at CP time with the dirty blocks accumulated since last CP.
     * v1 (FullChunkStore) ignores dirty_blocks and uploads the full chunk
     * from NVMe. v3 (SSTChunkStore) will use dirty_blocks to build SST files.
     *
     * @param chunk_id      The chunk being flushed
     * @param dirty_blocks  Dirty blocks since last CP (offset + data)
     * @param chunk_size    Total size of the chunk
     * @return S3Result indicating success or error
     */
    virtual S3Result put(chunk_id_t chunk_id,
                         const std::vector< DirtyBlock >& dirty_blocks,
                         uint64_t chunk_size) = 0;

    /**
     * @brief Read data from a chunk on S3.
     *
     * v1 downloads the full S3 object and seeks to the requested offset.
     * Implementations should cache locally to avoid repeated full downloads.
     *
     * @param chunk_id  The chunk to read from
     * @param offset    Byte offset within the chunk
     * @param size      Number of bytes to read
     * @return Future resolving to the requested data, or error
     */
    virtual folly::Future< std::pair< S3Result, sisl::byte_array > >
    get(chunk_id_t chunk_id, offset_t offset, uint64_t size) = 0;

    /**
     * @brief Compact a chunk's S3 representation.
     *
     * v1: no-op (already a single object).
     * v3: merge SST files into fewer/larger objects.
     *
     * @param chunk_id  The chunk to compact
     * @return S3Result indicating success or error
     */
    virtual S3Result compact(chunk_id_t chunk_id) = 0;

    /**
     * @brief Recover a chunk from S3 for NVMe restore.
     *
     * Downloads the chunk data from S3. Used during recovery when NVMe
     * is empty/lost.
     *
     * @param chunk_id  The chunk to recover
     * @return ChunkState with full chunk data and metadata
     */
    virtual ChunkState recover(chunk_id_t chunk_id) = 0;

    /**
     * @brief Describe a chunk's S3 representation.
     *
     * Returns metadata about the chunk's S3 objects (keys, sizes, etc.).
     *
     * @param chunk_id  The chunk to describe
     * @return ChunkMetadata with S3 key and size info
     */
    virtual ChunkMetadata describe(chunk_id_t chunk_id) = 0;
};

} // namespace homestore
