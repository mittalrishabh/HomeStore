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
        REGISTER_COUNTER(s3_cp_chunks_failed, "Total chunk uploads that failed");
        REGISTER_COUNTER(s3_cp_bytes_flushed, "Total bytes flushed to S3 across all CPs");
        REGISTER_COUNTER(s3_cp_flush_errors, "S3 CP flush error count");
        REGISTER_COUNTER(s3_cp_superblock_writes, "S3 superblock write count");
        REGISTER_COUNTER(s3_cp_early_triggers, "Early CP triggers from dirty cache threshold");
        REGISTER_COUNTER(s3_cp_retry_chunks, "Chunks retried from previous failed CP");

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
 * Created during on_switchover_cp. At switchover time, we drain dirty caches
 * and capture them here. During cp_flush, we upload the drained data.
 */
class S3CpContext : public CPContext {
public:
    explicit S3CpContext(CP* cp) : CPContext{cp} {}
    ~S3CpContext() override = default;

    /// S3PhysicalDevs with dirty data at switchover time
    std::vector< S3PhysicalDev* > m_dirty_pdevs;

    /// Pre-drained dirty blocks per pdev (index matches m_dirty_pdevs)
    std::vector< std::map< chunk_id_t, std::vector< DirtyBlock > > > m_drained_data;

    /// Flush progress tracking
    std::atomic< uint64_t > m_total_chunks{0};
    std::atomic< uint64_t > m_flushed_chunks{0};
};

/**
 * @brief S3 CP Callbacks — integrates S3 pdev flushing into HomeStore's
 *        checkpoint pipeline. Also implements S3CpFlushCallback so that
 *        S3PhysicalDev's dirty cache threshold can trigger an early CP.
 *
 * ## CP Flush Flow:
 *
 * 1. on_switchover_cp:
 *    - drain_all_dirty_cache() from each dirty pdev (atomic handoff)
 *    - Include failed chunks from previous CP for retry
 *
 * 2. cp_flush (async via folly::Future):
 *    For each dirty S3PhysicalDev:
 *      a. Group chunks by type: DATA → WAL → INDEX → METABLK
 *      b. Upload each type group with bounded concurrency (folly::collectAll)
 *      c. write_superblock() → S3 commit point (written last, only on full success)
 *
 * 3. cp_cleanup: no-op
 *
 * ## Error handling:
 *
 * If any chunk put fails:
 *   - Log + continue uploading remaining chunks (best effort)
 *   - Failed chunk IDs are saved and retried in the next CP
 *   - Superblock is NOT written for that pdev (previous gen stays valid)
 *   - cp_flush returns false
 */
class S3CpCallbacks : public CPCallbacks, public S3CpFlushCallback {
public:
    static constexpr uint32_t DEFAULT_UPLOAD_CONCURRENCY = 4;

    /**
     * @param s3_pdevs             All S3PhysicalDev instances
     * @param upload_concurrency   Max parallel chunk uploads per type group
     */
    explicit S3CpCallbacks(std::vector< S3PhysicalDev* > s3_pdevs,
                           uint32_t upload_concurrency = DEFAULT_UPLOAD_CONCURRENCY);
    ~S3CpCallbacks() override = default;

    // CPCallbacks interface
    std::unique_ptr< CPContext > on_switchover_cp(CP* cur_cp, CP* new_cp) override;
    folly::Future< bool > cp_flush(CP* cp) override;
    void cp_cleanup(CP* cp) override;
    int cp_progress_percent() override;

    // S3CpFlushCallback interface
    void trigger_early_cp_flush() override;

    S3CpMetrics& metrics() { return m_metrics; }

    /// Chunks that failed in the last CP (will be retried)
    std::set< chunk_id_t > failed_chunks(uint32_t pdev_id) const;

private:
    /// Flush a single S3PhysicalDev with concurrent uploads.
    folly::Future< bool > flush_pdev(S3PhysicalDev* pdev,
                                      std::map< chunk_id_t, std::vector< DirtyBlock > >& drained,
                                      S3CpContext* ctx);

    /// Sort chunks by flush order: DATA → WAL → INDEX → METABLK
    std::vector< chunk_id_t > sort_chunks_by_flush_order(
        const std::map< chunk_id_t, std::vector< DirtyBlock > >& drained,
        const PdevS3Superblock& superblock) const;

    /// Upload a batch of chunks concurrently with bounded parallelism
    std::set< chunk_id_t > upload_batch(
        ChunkStore* chunk_store,
        const PdevS3Superblock& superblock,
        const std::vector< chunk_id_t >& chunk_ids,
        std::map< chunk_id_t, std::vector< DirtyBlock > >& drained,
        S3CpContext* ctx);

    std::vector< S3PhysicalDev* > m_s3_pdevs;
    uint32_t m_upload_concurrency;
    S3CpMetrics m_metrics;

    /// Failed chunks per pdev — retried in the next CP
    mutable std::mutex m_failed_mutex;
    std::map< uint32_t, std::set< chunk_id_t > > m_failed_chunks; // pdev_id → failed chunk_ids

    /// Progress tracking
    std::atomic< uint64_t > m_total_chunks_this_cp{0};
    std::atomic< uint64_t > m_flushed_chunks_this_cp{0};
};

} // namespace homestore
