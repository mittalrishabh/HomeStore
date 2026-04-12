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
#include <memory>
#include <mutex>
#include <set>
#include <vector>

#include <folly/futures/Future.h>
#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/checkpoint/cp_mgr.hpp>
#include <homestore/checkpoint/cp.hpp>
#include <homestore/s3/s3_physical_dev.h>

namespace homestore {

/**
 * @brief Metrics for S3 CP flush operations.
 */
class S3CpMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit S3CpMetrics() : sisl::MetricsGroupWrapper{"S3CpFlush", "s3_cp"} {
        REGISTER_COUNTER(s3_cp_flush_count, "Total S3 CP flush invocations");
        REGISTER_COUNTER(s3_cp_chunks_flushed, "Total chunks flushed to S3 across all CPs");
        REGISTER_COUNTER(s3_cp_chunks_failed, "Total chunk upload failures");
        REGISTER_COUNTER(s3_cp_bytes_flushed, "Total bytes flushed to S3 across all CPs");
        REGISTER_COUNTER(s3_cp_flush_errors, "S3 CP flush error count");
        REGISTER_COUNTER(s3_cp_superblock_writes, "S3 superblock write count");
        REGISTER_COUNTER(s3_cp_superblock_failures, "Failed superblock writes");
        REGISTER_COUNTER(s3_cp_retry_chunks, "Chunks retried from previous CP");

        REGISTER_HISTOGRAM(s3_cp_flush_latency_us, "S3 CP flush latency (all pdevs) in us",
                           HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(s3_cp_chunk_put_latency_us, "Per-chunk S3 put latency in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~S3CpMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief CPContext for S3 service — tracks dirty S3 pdevs that need flushing.
 *
 * Created during on_switchover_cp. At switchover time, we snapshot which
 * S3PhysicalDevs have dirty data. During cp_flush, we drain their dirty
 * caches, upload via ChunkStore, and write the S3 superblock.
 */
class S3CpContext : public CPContext {
public:
    explicit S3CpContext(CP* cp) : CPContext{cp} {}
    ~S3CpContext() override = default;

    /// S3PhysicalDevs with dirty data at switchover time
    std::vector< S3PhysicalDev* > m_dirty_pdevs;

    /// Per-pdev: chunks that need to be retried from previous CP failures
    std::map< uint32_t /*pdev_id*/, std::set< chunk_id_t > > m_retry_chunks;

    /// Flush progress tracking
    std::atomic< uint64_t > m_total_chunks{0};
    std::atomic< uint64_t > m_flushed_chunks{0};
};

/**
 * @brief S3 CP Callbacks — integrates S3 pdev flushing into HomeStore's
 *        checkpoint pipeline.
 *
 * ## CP Flush Flow (S3 side):
 *
 * 1. on_switchover_cp:
 *    - Snapshot which S3PhysicalDevs have dirty data
 *    - Include chunks that failed in the previous CP for retry
 *    - Return S3CpContext with the dirty pdev list
 *
 * 2. cp_flush (runs after NVMe flush is done):
 *    For each dirty S3PhysicalDev:
 *      a. drain_all_dirty_cache() → get all dirty blocks per chunk
 *      b. Sort chunks by type: DATA → WAL → INDEX → METABLK
 *      c. Upload chunks in parallel batches (bounded concurrency)
 *      d. If ALL uploads succeed: write_superblock() (S3 commit point)
 *      e. If any fail: skip superblock, mark failed for retry next CP
 *
 * 3. cp_cleanup:
 *    - Reset progress counters
 *
 * ## Error handling:
 *
 * - S3 flush failure is NON-FATAL — NVMe is already durable
 * - Failed chunks are carried to next CP for automatic retry
 * - Superblock only written if ALL chunks uploaded successfully
 *   (previous generation remains valid if skipped)
 */
class S3CpCallbacks : public CPCallbacks {
public:
    /**
     * @brief Construct S3CpCallbacks.
     *
     * @param s3_pdevs            All S3PhysicalDev instances managed by DeviceManager
     * @param upload_concurrency  Max parallel chunk uploads per pdev (from s3.upload_concurrency)
     */
    S3CpCallbacks(std::vector< S3PhysicalDev* > s3_pdevs, uint32_t upload_concurrency = 4);
    ~S3CpCallbacks() override = default;

    // CPCallbacks interface
    std::unique_ptr< CPContext > on_switchover_cp(CP* cur_cp, CP* new_cp) override;
    folly::Future< bool > cp_flush(CP* cp) override;
    void cp_cleanup(CP* cp) override;
    int cp_progress_percent() override;

    /// Get the metrics
    S3CpMetrics& metrics() { return m_metrics; }

    /// Chunks that failed upload in the last CP (will be retried next CP).
    /// Keyed by pdev_id.
    std::map< uint32_t, std::set< chunk_id_t > > failed_chunks() const;

private:
    /// Flush a single S3PhysicalDev: drain dirty cache, upload chunks in
    /// parallel batches, write superblock. Returns true on full success.
    bool flush_pdev(S3PhysicalDev* pdev, S3CpContext* ctx);

    /// Upload a single chunk. Returns (chunk_id, S3Result).
    std::pair< chunk_id_t, S3Result >
    upload_one_chunk(S3PhysicalDev* pdev, chunk_id_t chunk_id,
                     const std::vector< DirtyBlock >& dirty_blocks);

    /// Sort chunks by flush order: DATA → WAL → INDEX → METABLK.
    std::vector< std::pair< chunk_id_t, S3ChunkType > >
    sort_by_flush_order(const PdevS3Superblock& sb,
                        const std::map< chunk_id_t, std::vector< DirtyBlock > >& dirty_map);

    std::vector< S3PhysicalDev* > m_s3_pdevs;
    uint32_t m_upload_concurrency;
    S3CpMetrics m_metrics;

    /// Per-pdev failed chunks — carried over to next CP for retry
    mutable std::mutex m_failed_mutex;
    std::map< uint32_t /*pdev_id*/, std::set< chunk_id_t > > m_failed_chunks;

    /// Progress tracking across CPs
    std::atomic< uint64_t > m_total_chunks_this_cp{0};
    std::atomic< uint64_t > m_flushed_chunks_this_cp{0};
};

} // namespace homestore
