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
#include <unordered_set>

#include <homestore/s3/snapshot_manager.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

std::string to_string(SnapshotResult r) {
    switch (r) {
    case SnapshotResult::SUCCESS: return "SUCCESS";
    case SnapshotResult::DUPLICATE_SNAP_ID: return "DUPLICATE_SNAP_ID";
    case SnapshotResult::CP_FLUSH_FAILED: return "CP_FLUSH_FAILED";
    case SnapshotResult::SUPERBLOCK_WRITE_FAILED: return "SUPERBLOCK_WRITE_FAILED";
    case SnapshotResult::INTERNAL_ERROR: return "INTERNAL_ERROR";
    }
    return "UNKNOWN";
}

SnapshotManager::SnapshotManager(S3PhysicalDev* s3_pdev,
                                 FullChunkStore* chunk_store,
                                 CpFlushCallback cp_flush_cb,
                                 std::shared_ptr< S3ObjectStore > s3_store)
    : m_s3_pdev{s3_pdev},
      m_chunk_store{chunk_store},
      m_cp_flush_cb{std::move(cp_flush_cb)},
      m_s3_store{std::move(s3_store)} {}

CreateSnapshotResult SnapshotManager::create_snapshot(uint64_t snap_id, uint64_t btree_root_blkid) {
    auto start = std::chrono::steady_clock::now();
    CreateSnapshotResult result;
    result.snap_id = snap_id;

    auto& sb = m_s3_pdev->superblock_mutable();

    // Check for duplicate snap_id
    if (sb.find_snapshot(snap_id) != nullptr) {
        LOGWARNMOD(s3, "Snapshot {} already exists", snap_id);
        result.status = SnapshotResult::DUPLICATE_SNAP_ID;
        return result;
    }

    // Step 1: Flush checkpoint to ensure S3 is fully up to date
    LOGINFO("Snapshot {}: triggering CP flush", snap_id);
    if (!m_cp_flush_cb()) {
        LOGERRORMOD(s3, "Snapshot {}: CP flush failed", snap_id);
        result.status = SnapshotResult::CP_FLUSH_FAILED;
        return result;
    }

    // Step 2: Record current S3 keys for all chunks — these are now pinned
    auto snapshot_gen = sb.generation();
    result.generation = snapshot_gen;

    PdevS3Superblock::SnapshotRecord snap_record;
    snap_record.header.snap_id = snap_id;
    snap_record.header.generation = snapshot_gen;
    snap_record.header.btree_root_blkid = btree_root_blkid;

    for (const auto& chunk : sb.chunks()) {
        s3_snapshot_chunk_key ck;
        ck.chunk_id = chunk.chunk_id;
        ck.set_s3_key(chunk.get_s3_key());
        snap_record.chunk_keys.push_back(ck);
    }

    result.num_chunks_pinned = snap_record.chunk_keys.size();
    snap_record.header.num_chunk_keys = static_cast< uint32_t >(result.num_chunks_pinned);

    // Step 3: Add snapshot to superblock
    sb.add_snapshot(snap_record);

    // Step 4: Switch to generation-stamped naming for future CPs.
    // Next generation = current + 1; write_superblock() will increment it.
    auto next_gen = snapshot_gen + 1;
    m_chunk_store->set_active_generation(next_gen);

    // Update chunk entries to point to generation-stamped keys for next CP
    S3KeyMapper mapper{m_s3_pdev->volume_id()};
    for (auto& chunk : sb.chunks_mutable()) {
        chunk.set_s3_key(mapper.chunk_data_key(chunk.chunk_id, next_gen));
        chunk.generation = next_gen;
    }

    // Step 5: Write updated superblock to S3
    auto sb_result = m_s3_pdev->write_superblock();
    if (!sb_result.ok()) {
        LOGERRORMOD(s3, "Snapshot {}: superblock write failed: {}", snap_id, sb_result.error_message);
        sb.remove_snapshot(snap_id);
        result.status = SnapshotResult::SUPERBLOCK_WRITE_FAILED;
        return result;
    }

    auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
        std::chrono::steady_clock::now() - start).count();
    HISTOGRAM_OBSERVE(m_metrics, snapshot_create_latency_us, elapsed_us);
    COUNTER_INCREMENT(m_metrics, snapshots_created, 1);
    COUNTER_INCREMENT(m_metrics, snapshot_chunks_pinned, result.num_chunks_pinned);

    result.status = SnapshotResult::SUCCESS;

    LOGINFO("Snapshot {} created: gen={} chunks_pinned={} btree_root={} latency={}us",
            snap_id, snapshot_gen, result.num_chunks_pinned, btree_root_blkid, elapsed_us);

    return result;
}

DeleteSnapshotResult SnapshotManager::delete_snapshot(uint64_t snap_id) {
    DeleteSnapshotResult result;
    auto& sb = m_s3_pdev->superblock_mutable();

    // Step 1: Collect the snapshot's pinned S3 keys before removing
    auto* snap = sb.find_snapshot(snap_id);
    if (!snap) {
        LOGWARNMOD(s3, "Snapshot {} not found for deletion", snap_id);
        return result;
    }

    std::vector< std::string > snap_keys;
    snap_keys.reserve(snap->chunk_keys.size());
    for (const auto& ck : snap->chunk_keys) {
        snap_keys.push_back(ck.get_s3_key());
    }

    // Step 2: Remove snapshot from superblock
    sb.remove_snapshot(snap_id);

    // Step 3: Determine which of the snapshot's keys are no longer pinned
    // by any remaining snapshot and delete them from S3
    if (m_s3_store && !snap_keys.empty()) {
        // Build pinned set from remaining snapshots
        std::unordered_set< std::string > still_pinned;
        for (const auto& remaining_snap : sb.snapshots()) {
            for (const auto& ck : remaining_snap.chunk_keys) {
                still_pinned.insert(ck.get_s3_key());
            }
        }

        // Also keep keys that are currently live (in chunk entries)
        for (const auto& chunk : sb.chunks()) {
            still_pinned.insert(chunk.get_s3_key());
        }

        std::vector< std::string > to_delete;
        for (const auto& key : snap_keys) {
            if (still_pinned.count(key) == 0) {
                to_delete.push_back(key);
            }
        }

        if (!to_delete.empty()) {
            LOGINFO("Snapshot {}: cleaning up {} unreferenced S3 keys", snap_id, to_delete.size());

            auto del_result = m_s3_store->delete_objects(to_delete).get();
            if (del_result.ok()) {
                result.s3_keys_cleaned = to_delete.size();
            } else {
                LOGWARNMOD(s3, "Snapshot {}: batch delete failed, falling back to individual deletes: {}",
                           snap_id, del_result.error_message);
                for (const auto& key : to_delete) {
                    auto r = m_s3_store->delete_object(key).get();
                    if (r.ok()) {
                        ++result.s3_keys_cleaned;
                    } else {
                        LOGERRORMOD(s3, "Snapshot {}: failed to delete S3 key {}: {}",
                                    snap_id, key, r.error_message);
                    }
                }
            }
            COUNTER_INCREMENT(m_metrics, snapshot_s3_keys_cleaned, result.s3_keys_cleaned);
        }
    }

    // Step 4: If no snapshots remain, revert to overwrite naming
    if (!sb.has_snapshots()) {
        m_chunk_store->set_active_generation(0);

        S3KeyMapper mapper{m_s3_pdev->volume_id()};
        for (auto& chunk : sb.chunks_mutable()) {
            chunk.set_s3_key(mapper.chunk_data_key(chunk.chunk_id, 0));
            chunk.generation = 0;
        }

        LOGINFO("Snapshot {}: no snapshots remain, reverted to overwrite naming", snap_id);
    }

    // Step 5: Write updated superblock to S3
    auto sb_result = m_s3_pdev->write_superblock();
    if (!sb_result.ok()) {
        LOGERRORMOD(s3, "Snapshot {}: superblock write failed during deletion: {}",
                    snap_id, sb_result.error_message);
    }

    COUNTER_INCREMENT(m_metrics, snapshots_deleted, 1);
    LOGINFO("Snapshot {} deleted: {} S3 keys cleaned", snap_id, result.s3_keys_cleaned);

    result.success = true;
    return result;
}

} // namespace homestore
