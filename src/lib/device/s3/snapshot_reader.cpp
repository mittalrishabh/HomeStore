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
#include <chrono>
#include <cstring>

#include <homestore/s3/snapshot_reader.h>
#include <homestore/btree/detail/btree_crc32c.hpp>

SISL_LOGGING_DECL(s3)

namespace homestore {

static constexpr uint32_t DEFAULT_NODE_SIZE = 8192;

std::string to_string(SnapshotReadStatus s) {
    switch (s) {
    case SnapshotReadStatus::SUCCESS:
        return "SUCCESS";
    case SnapshotReadStatus::SNAPSHOT_NOT_FOUND:
        return "SNAPSHOT_NOT_FOUND";
    case SnapshotReadStatus::S3_READ_FAILED:
        return "S3_READ_FAILED";
    case SnapshotReadStatus::BTREE_NODE_CORRUPT:
        return "BTREE_NODE_CORRUPT";
    case SnapshotReadStatus::KEY_NOT_FOUND:
        return "KEY_NOT_FOUND";
    case SnapshotReadStatus::INVALID_BLKID:
        return "INVALID_BLKID";
    case SnapshotReadStatus::INTERNAL_ERROR:
        return "INTERNAL_ERROR";
    }
    return "UNKNOWN";
}

SnapshotReader::SnapshotReader(const PdevS3Superblock* superblock,
                               std::shared_ptr< S3ObjectStore > s3_store,
                               uint32_t block_size,
                               uint32_t node_size)
    : m_superblock{superblock}
    , m_s3_store{std::move(s3_store)}
    , m_block_size{block_size}
    , m_node_size{node_size == 0 ? DEFAULT_NODE_SIZE : node_size} {}

SnapshotReader::ChunkKeyMap
SnapshotReader::build_chunk_key_map(const PdevS3Superblock::SnapshotRecord& snap) const {
    ChunkKeyMap map;
    map.reserve(snap.chunk_keys.size());
    for (const auto& ck : snap.chunk_keys) {
        map[ck.chunk_id] = ck.get_s3_key();
    }
    return map;
}

bool SnapshotReader::validate_node(const uint8_t* buf, uint64_t buf_size) const {
    if (buf_size < sizeof(persistent_hdr_t)) { return false; }

    auto* hdr = reinterpret_cast< const persistent_hdr_t* >(buf);
    if (hdr->magic != BTREE_NODE_MAGIC || hdr->version != BTREE_NODE_VERSION) { return false; }

    auto actual_size = static_cast< uint64_t >(hdr->node_size) + 1;
    if (actual_size > buf_size) { return false; }

    auto exp_checksum = bt_crc32c(bt_init_crc32c,
                                  buf + sizeof(persistent_hdr_t),
                                  actual_size - sizeof(persistent_hdr_t));
    return hdr->checksum == exp_checksum;
}

SnapshotReader::NodeReadResult
SnapshotReader::read_btree_node(bnodeid_t node_id, const ChunkKeyMap& chunk_keys) const {
    BlkId blk_id{node_id};
    if (!blk_id.is_valid()) {
        return {SnapshotReadStatus::INVALID_BLKID, nullptr};
    }

    auto chunk_num = blk_id.chunk_num();
    auto it = chunk_keys.find(chunk_num);
    if (it == chunk_keys.end()) {
        LOGERROR("Snapshot read: chunk {} not found in snapshot chunk key map", chunk_num);
        return {SnapshotReadStatus::S3_READ_FAILED, nullptr};
    }

    uint64_t offset = static_cast< uint64_t >(blk_id.blk_num()) * m_block_size;
    uint64_t length = m_node_size;

    COUNTER_INCREMENT(m_metrics, snapshot_s3_gets, 1);

    auto result = m_s3_store->get_object_range(it->second, offset, length).get();
    if (!result.first.ok()) {
        LOGERROR("Snapshot read: S3 GET failed for key={} offset={} len={}: {}",
                 it->second, offset, length, result.first.error_message);
        return {SnapshotReadStatus::S3_READ_FAILED, nullptr};
    }

    auto ba = sisl::make_byte_array(static_cast< uint32_t >(result.second.size()));
    std::memcpy(ba->bytes(), result.second.cbytes(), result.second.size());

    COUNTER_INCREMENT(m_metrics, snapshot_btree_nodes_read, 1);

    if (!validate_node(ba->cbytes(), ba->size())) {
        LOGERROR("Snapshot read: B+tree node corrupt at node_id={} chunk={} offset={}",
                 node_id, chunk_num, offset);
        return {SnapshotReadStatus::BTREE_NODE_CORRUPT, nullptr};
    }

    return {SnapshotReadStatus::SUCCESS, ba};
}

SnapshotReadResult
SnapshotReader::read_data_block(const BlkId& blk_id, const ChunkKeyMap& chunk_keys,
                                uint64_t read_size) const {
    if (!blk_id.is_valid()) {
        return {SnapshotReadStatus::INVALID_BLKID, nullptr, 0};
    }

    auto chunk_num = blk_id.chunk_num();
    auto it = chunk_keys.find(chunk_num);
    if (it == chunk_keys.end()) {
        LOGERROR("Snapshot read: data chunk {} not found in snapshot chunk key map", chunk_num);
        return {SnapshotReadStatus::S3_READ_FAILED, nullptr, 0};
    }

    uint64_t offset = static_cast< uint64_t >(blk_id.blk_num()) * m_block_size;
    if (read_size == 0) {
        read_size = static_cast< uint64_t >(blk_id.blk_count()) * m_block_size;
    }

    COUNTER_INCREMENT(m_metrics, snapshot_s3_gets, 1);

    auto result = m_s3_store->get_object_range(it->second, offset, read_size).get();
    if (!result.first.ok()) {
        LOGERROR("Snapshot read: S3 data GET failed for key={} offset={} len={}: {}",
                 it->second, offset, read_size, result.first.error_message);
        return {SnapshotReadStatus::S3_READ_FAILED, nullptr, 0};
    }

    auto ba = sisl::make_byte_array(static_cast< uint32_t >(result.second.size()));
    std::memcpy(ba->bytes(), result.second.cbytes(), result.second.size());

    return {SnapshotReadStatus::SUCCESS, ba, ba->size()};
}

SnapshotReadResult
SnapshotReader::traverse_btree(bnodeid_t root_id, uint64_t lba, uint64_t read_size,
                               const ChunkKeyMap& chunk_keys) const {
    bnodeid_t current_id = root_id;
    static constexpr uint32_t MAX_TREE_DEPTH = 64;

    for (uint32_t depth = 0; depth < MAX_TREE_DEPTH; ++depth) {
        auto node_result = read_btree_node(current_id, chunk_keys);
        if (node_result.status != SnapshotReadStatus::SUCCESS) {
            return {node_result.status, nullptr, 0};
        }

        auto* buf = node_result.raw_data->cbytes();
        auto* hdr = reinterpret_cast< const persistent_hdr_t* >(buf);
        auto nentries = static_cast< uint32_t >(hdr->nentries);
        bool is_leaf = hdr->leaf != 0;
        auto* data_area = buf + sizeof(persistent_hdr_t);

        // Entry layout in SimpleNode: [key (8 bytes) | value] * nentries
        // Interior value = BtreeLinkInfo::bnode_link_info (16 bytes: bnodeid_t + uint64_t)
        // Leaf value = BlkId (8 bytes)
        static constexpr uint32_t KEY_SIZE = sizeof(uint64_t);
        uint32_t value_size = is_leaf ? sizeof(uint64_t) : sizeof(BtreeLinkInfo::bnode_link_info);
        uint32_t entry_size = KEY_SIZE + value_size;

        // Binary search matching HomeStore's bsearch semantics:
        // Returns (found, idx) where idx is the first entry with key >= target.
        // If target > all keys, idx == nentries.
        int32_t start = -1;
        int32_t end = static_cast< int32_t >(nentries);
        bool found = false;

        while ((end - start) > 1) {
            int32_t mid = start + (end - start) / 2;
            uint64_t mid_key;
            std::memcpy(&mid_key, data_area + (mid * entry_size), KEY_SIZE);

            if (mid_key == lba) {
                found = true;
                end = mid;
                break;
            } else if (mid_key > lba) {
                end = mid;
            } else {
                start = mid;
            }
        }
        uint32_t idx = static_cast< uint32_t >(end);

        if (is_leaf) {
            if (!found || idx >= nentries) {
                return {SnapshotReadStatus::KEY_NOT_FOUND, nullptr, 0};
            }
            uint64_t blkid_int;
            std::memcpy(&blkid_int, data_area + (idx * entry_size) + KEY_SIZE, sizeof(uint64_t));
            BlkId data_blk{blkid_int};
            return read_data_block(data_blk, chunk_keys, read_size);
        }

        // Interior node: follow child pointer.
        // In HomeStore's B+tree, value[i] is the child for keys <= key[i].
        // Edge pointer is the child for keys > key[n-1].
        if (idx == nentries) {
            if (hdr->edge_info.m_bnodeid != empty_bnodeid) {
                current_id = hdr->edge_info.m_bnodeid;
            } else {
                return {SnapshotReadStatus::KEY_NOT_FOUND, nullptr, 0};
            }
        } else {
            BtreeLinkInfo::bnode_link_info link;
            std::memcpy(&link, data_area + (idx * entry_size) + KEY_SIZE,
                       sizeof(BtreeLinkInfo::bnode_link_info));
            current_id = link.m_bnodeid;
        }
    }

    LOGERROR("Snapshot read: B+tree traversal exceeded max depth {}", MAX_TREE_DEPTH);
    return {SnapshotReadStatus::INTERNAL_ERROR, nullptr, 0};
}

SnapshotReadResult
SnapshotReader::snapshot_read(uint64_t snap_id, uint64_t lba, uint64_t size) {
    auto start = std::chrono::steady_clock::now();
    COUNTER_INCREMENT(m_metrics, snapshot_reads_total, 1);

    auto* snap = m_superblock->find_snapshot(snap_id);
    if (!snap) {
        LOGWARN("Snapshot read: snapshot {} not found", snap_id);
        COUNTER_INCREMENT(m_metrics, snapshot_reads_failed, 1);
        return {SnapshotReadStatus::SNAPSHOT_NOT_FOUND, nullptr, 0};
    }

    auto chunk_keys = build_chunk_key_map(*snap);
    auto root_id = static_cast< bnodeid_t >(snap->header.btree_root_blkid);

    auto result = traverse_btree(root_id, lba, size, chunk_keys);

    auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
                          std::chrono::steady_clock::now() - start)
                          .count();
    HISTOGRAM_OBSERVE(m_metrics, snapshot_read_latency_us, elapsed_us);

    if (result.status == SnapshotReadStatus::SUCCESS) {
        COUNTER_INCREMENT(m_metrics, snapshot_reads_success, 1);
        LOGDEBUG("Snapshot read: snap_id={} lba={} read {} bytes in {} us",
                 snap_id, lba, result.data_size, elapsed_us);
    } else {
        COUNTER_INCREMENT(m_metrics, snapshot_reads_failed, 1);
        LOGWARN("Snapshot read: snap_id={} lba={} failed: {}", snap_id, lba, to_string(result.status));
    }

    return result;
}

folly::Future< SnapshotReadResult >
SnapshotReader::snapshot_read_async(uint64_t snap_id, uint64_t lba, uint64_t size) {
    return folly::makeFuture(snapshot_read(snap_id, lba, size));
}

} // namespace homestore
