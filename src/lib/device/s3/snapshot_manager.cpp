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
                                 CpFlushCallback cp_flush_cb)
    : m_s3_pdev{s3_pdev},
      m_chunk_store{chunk_store},
      m_cp_flush_cb{std::move(cp_flush_cb)} {}

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

bool SnapshotManager::delete_snapshot(uint64_t snap_id) {
    auto& sb = m_s3_pdev->superblock_mutable();

    if (!sb.remove_snapshot(snap_id)) {
        LOGWARNMOD(s3, "Snapshot {} not found for deletion", snap_id);
        return false;
    }

    // If no snapshots remain, revert to overwrite naming
    if (!sb.has_snapshots()) {
        m_chunk_store->set_active_generation(0);

        S3KeyMapper mapper{m_s3_pdev->volume_id()};
        for (auto& chunk : sb.chunks_mutable()) {
            chunk.set_s3_key(mapper.chunk_data_key(chunk.chunk_id, 0));
            chunk.generation = 0;
        }
    }

    auto sb_result = m_s3_pdev->write_superblock();
    if (!sb_result.ok()) {
        LOGERRORMOD(s3, "Snapshot {}: superblock write failed during deletion: {}",
                    snap_id, sb_result.error_message);
    }

    COUNTER_INCREMENT(m_metrics, snapshots_deleted, 1);
    LOGINFO("Snapshot {} deleted", snap_id);
    return true;
}

} // namespace homestore
