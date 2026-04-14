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
#include <mutex>
#include <set>
#include <string>
#include <unordered_set>

#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_object_store.h>

namespace homestore {

class S3GcMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit S3GcMetrics() : sisl::MetricsGroupWrapper{"S3GC", "s3_gc"} {
        REGISTER_COUNTER(gc_runs_total, "Total GC invocations");
        REGISTER_COUNTER(gc_objects_deleted, "Total S3 objects deleted by GC");
        REGISTER_COUNTER(gc_objects_retained, "S3 objects retained (pinned by snapshots)");
        REGISTER_COUNTER(gc_objects_pending, "Current pending GC candidates");
        REGISTER_COUNTER(gc_delete_errors, "S3 delete failures during GC");

        REGISTER_HISTOGRAM(gc_run_latency_us, "GC run latency in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~S3GcMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief S3GarbageCollector — cleans up superseded S3 objects after CP commits.
 *
 * When snapshots exist, each new generation writes to new S3 keys (CoW naming).
 * After a CP commit, the previous generation's S3 objects are superseded. If no
 * snapshot pins them, they should be deleted from S3 to keep storage minimal.
 *
 * ## Lifecycle:
 *
 * 1. **on_generation_change()**: Called when a snapshot bumps the active generation.
 *    Records the old S3 keys as GC candidates (they were just superseded).
 *
 * 2. **run_gc()**: Called after CP commit. For each GC candidate:
 *    - Check if any snapshot's chunk_snapshot_keys references it
 *    - If not pinned: delete from S3
 *    - If pinned: retain (do not delete)
 *
 * 3. **collect_pinned_keys()**: Scans all snapshots in the superblock and returns
 *    the union of all pinned S3 keys. Used during GC to determine what to keep.
 *
 * GC is non-blocking — runs asynchronously after CP commit. Deletion failures
 * are logged but do not fail the CP.
 */
class S3GarbageCollector {
public:
    S3GarbageCollector(const PdevS3Superblock* superblock,
                       std::shared_ptr< S3ObjectStore > s3_store);

    ~S3GarbageCollector() = default;

    S3GarbageCollector(const S3GarbageCollector&) = delete;
    S3GarbageCollector& operator=(const S3GarbageCollector&) = delete;

    /**
     * @brief Record superseded S3 keys as GC candidates.
     *
     * Called when a snapshot creation bumps the active generation, making
     * the previous generation's keys superseded. The old keys are added
     * to the pending GC set.
     *
     * @param superseded_keys  S3 keys from the previous generation
     */
    void on_generation_change(const std::vector< std::string >& superseded_keys);

    /**
     * @brief Add a single S3 key as a GC candidate.
     */
    void add_gc_candidate(const std::string& s3_key);

    /**
     * @brief Run garbage collection — delete unreferenced S3 objects.
     *
     * Scans the pending GC set, checks each key against all snapshot
     * pinned keys, and deletes any that are not referenced. Runs
     * synchronously but is designed to be called asynchronously after CP.
     *
     * @return Number of objects deleted
     */
    uint64_t run_gc();

    /**
     * @brief Collect all S3 keys pinned by any snapshot.
     *
     * Scans all snapshots in the superblock and returns the union of
     * all chunk_snapshot_keys. Used to determine which S3 objects must
     * be retained during GC.
     */
    std::unordered_set< std::string > collect_pinned_keys() const;

    /**
     * @brief Check if a specific S3 key is pinned by any snapshot.
     */
    bool is_key_pinned(const std::string& s3_key) const;

    /**
     * @brief Get the number of pending GC candidates.
     */
    uint64_t pending_count() const;

    /**
     * @brief Clear all pending GC candidates without deleting.
     */
    void clear_pending();

    S3GcMetrics& metrics() { return m_metrics; }

private:
    const PdevS3Superblock* m_superblock;
    std::shared_ptr< S3ObjectStore > m_s3_store;

    mutable std::mutex m_pending_mutex;
    std::set< std::string > m_pending_keys;

    S3GcMetrics m_metrics;
};

} // namespace homestore
