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
#include <functional>
#include <memory>
#include <string>

#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_physical_dev.h>

namespace homestore {

enum class SnapshotResult : uint8_t {
    SUCCESS,
    DUPLICATE_SNAP_ID,
    CP_FLUSH_FAILED,
    SUPERBLOCK_WRITE_FAILED,
    INTERNAL_ERROR,
};

std::string to_string(SnapshotResult r);

struct CreateSnapshotResult {
    SnapshotResult status{SnapshotResult::INTERNAL_ERROR};
    uint64_t snap_id{0};
    uint64_t generation{0};
    uint64_t num_chunks_pinned{0};
};

using CpFlushCallback = std::function< bool() >;

class SnapshotMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit SnapshotMetrics() : sisl::MetricsGroupWrapper{"Snapshot", "snapshot"} {
        REGISTER_COUNTER(snapshots_created, "Total snapshots created");
        REGISTER_COUNTER(snapshots_deleted, "Total snapshots deleted");
        REGISTER_COUNTER(snapshot_chunks_pinned, "Total chunk keys pinned by snapshots");
        REGISTER_COUNTER(snapshot_s3_keys_cleaned, "S3 keys cleaned up during snapshot deletion");

        REGISTER_HISTOGRAM(snapshot_create_latency_us, "Snapshot creation latency in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~SnapshotMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief SnapshotManager — create and delete S3-native snapshots.
 *
 * Snapshot creation:
 * 1. Trigger a CP flush to ensure all dirty data is on S3
 * 2. Record the current S3 keys for all chunks (the "pinned" state)
 * 3. Store the snapshot entry in the pdev_s3 superblock
 * 4. Switch FullChunkStore to generation-stamped naming so future CPs
 *    write to new S3 keys, preserving the snapshot's objects
 * 5. Write the updated superblock to S3
 *
 * After snapshot creation, the pinned S3 objects are immutable — new
 * writes go to generation-stamped keys (data_gen<N>.dat).
 */
struct DeleteSnapshotResult {
    bool success{false};
    uint64_t s3_keys_cleaned{0};
};

class SnapshotManager {
public:
    SnapshotManager(S3PhysicalDev* s3_pdev,
                    FullChunkStore* chunk_store,
                    CpFlushCallback cp_flush_cb,
                    std::shared_ptr< S3ObjectStore > s3_store = nullptr);

    ~SnapshotManager() = default;

    SnapshotManager(const SnapshotManager&) = delete;
    SnapshotManager& operator=(const SnapshotManager&) = delete;

    /**
     * @brief Create a snapshot.
     *
     * @param snap_id           Unique snapshot ID (caller-assigned)
     * @param btree_root_blkid  B+tree root block ID at this point in time
     * @return CreateSnapshotResult with status and metadata
     */
    CreateSnapshotResult create_snapshot(uint64_t snap_id, uint64_t btree_root_blkid = 0);

    /**
     * @brief Delete a snapshot and clean up its unreferenced S3 objects.
     *
     * 1. Collect the snapshot's pinned S3 keys
     * 2. Remove the snapshot from the superblock
     * 3. For each pinned key: if no remaining snapshot references it, delete from S3
     * 4. If no snapshots remain: switch back to overwrite naming
     * 5. Write updated superblock to S3
     *
     * S3 key cleanup requires an S3ObjectStore (set via constructor). If not
     * set, deletion still removes metadata but skips S3 cleanup.
     *
     * @param snap_id  Snapshot to delete
     * @return DeleteSnapshotResult with success status and cleanup count
     */
    DeleteSnapshotResult delete_snapshot(uint64_t snap_id);

    SnapshotMetrics& metrics() { return m_metrics; }

private:
    S3PhysicalDev* m_s3_pdev;
    FullChunkStore* m_chunk_store;
    CpFlushCallback m_cp_flush_cb;
    std::shared_ptr< S3ObjectStore > m_s3_store;
    SnapshotMetrics m_metrics;
};

} // namespace homestore
