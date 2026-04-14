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
#include <string>
#include <unordered_map>

#include <folly/futures/Future.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/blk.h>
#include <homestore/btree/detail/btree_node.hpp>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_object_store.h>

namespace homestore {

enum class SnapshotReadStatus : uint8_t {
    SUCCESS,
    SNAPSHOT_NOT_FOUND,
    S3_READ_FAILED,
    BTREE_NODE_CORRUPT,
    KEY_NOT_FOUND,
    INVALID_BLKID,
    INTERNAL_ERROR,
};

std::string to_string(SnapshotReadStatus s);

struct SnapshotReadResult {
    SnapshotReadStatus status{SnapshotReadStatus::INTERNAL_ERROR};
    sisl::byte_array data;
    uint64_t data_size{0};
};

class SnapshotReadMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit SnapshotReadMetrics() : sisl::MetricsGroupWrapper{"SnapshotRead", "snapshot_read"} {
        REGISTER_COUNTER(snapshot_reads_total, "Total snapshot read requests");
        REGISTER_COUNTER(snapshot_reads_success, "Successful snapshot reads");
        REGISTER_COUNTER(snapshot_reads_failed, "Failed snapshot reads");
        REGISTER_COUNTER(snapshot_s3_gets, "S3 GET requests issued for snapshot reads");
        REGISTER_COUNTER(snapshot_btree_nodes_read, "B+tree nodes read during snapshot traversal");

        REGISTER_HISTOGRAM(snapshot_read_latency_us, "End-to-end snapshot read latency in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~SnapshotReadMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief SnapshotReader — read data from an S3-native snapshot.
 *
 * Reads traverse the B+tree stored on S3 at the time of snapshot creation:
 * 1. Look up snapshot in pdev_s3 superblock → btree_root_blkid + chunk keys
 * 2. Build chunk_id → S3 key map from snapshot's pinned keys
 * 3. Resolve btree_root_blkid to (chunk_id, offset), read node from S3
 * 4. Binary search node for target LBA
 * 5. Interior node → follow child pointer, repeat from step 3
 * 6. Leaf node → extract data BlkId → read data from S3
 *
 * Each B+tree level requires one S3 GET (range read), plus one final
 * GET for the data itself.
 */
class SnapshotReader {
public:
    SnapshotReader(const PdevS3Superblock* superblock,
                   std::shared_ptr< S3ObjectStore > s3_store,
                   uint32_t block_size,
                   uint32_t node_size = 0);

    ~SnapshotReader() = default;

    SnapshotReader(const SnapshotReader&) = delete;
    SnapshotReader& operator=(const SnapshotReader&) = delete;

    /**
     * @brief Read data at a given LBA from a snapshot.
     *
     * @param snap_id   Snapshot to read from
     * @param lba       Logical block address to look up
     * @param size      Number of bytes to read (0 = one block)
     * @return SnapshotReadResult with status and data
     */
    SnapshotReadResult snapshot_read(uint64_t snap_id, uint64_t lba, uint64_t size = 0);

    /**
     * @brief Async version of snapshot_read.
     */
    folly::Future< SnapshotReadResult > snapshot_read_async(uint64_t snap_id, uint64_t lba, uint64_t size = 0);

    SnapshotReadMetrics& metrics() { return m_metrics; }

private:
    using ChunkKeyMap = std::unordered_map< uint64_t, std::string >;

    struct NodeReadResult {
        SnapshotReadStatus status{SnapshotReadStatus::INTERNAL_ERROR};
        sisl::byte_array raw_data;
    };

    ChunkKeyMap build_chunk_key_map(const PdevS3Superblock::SnapshotRecord& snap) const;

    NodeReadResult read_btree_node(bnodeid_t node_id, const ChunkKeyMap& chunk_keys) const;

    SnapshotReadResult read_data_block(const BlkId& blk_id, const ChunkKeyMap& chunk_keys,
                                       uint64_t read_size) const;

    SnapshotReadResult traverse_btree(bnodeid_t root_id, uint64_t lba, uint64_t read_size,
                                      const ChunkKeyMap& chunk_keys) const;

    bool validate_node(const uint8_t* buf, uint64_t buf_size) const;

    const PdevS3Superblock* m_superblock;
    std::shared_ptr< S3ObjectStore > m_s3_store;
    uint32_t m_block_size;
    uint32_t m_node_size;
    SnapshotReadMetrics m_metrics;
};

} // namespace homestore
