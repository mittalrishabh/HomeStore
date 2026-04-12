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
#include <string>
#include <vector>

#include <folly/futures/Future.h>
#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/checkpoint/cp_mgr.hpp>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_physical_dev.h>

namespace homestore {

// ─── Metrics ─────────────────────────────────────────────────────────────────

class S3CpMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit S3CpMetrics() : sisl::MetricsGroupWrapper{"S3CpHandler", "s3_cp"} {
        REGISTER_COUNTER(s3_cp_flush_count, "Total S3 CP flush invocations");
        REGISTER_COUNTER(s3_cp_chunks_uploaded, "Total chunks uploaded to S3 across all CPs");
        REGISTER_COUNTER(s3_cp_chunks_failed, "Total chunk uploads that failed");
        REGISTER_COUNTER(s3_cp_superblock_writes, "Total pdev_s3 superblock writes");
        REGISTER_COUNTER(s3_cp_superblock_failures, "Failed superblock writes");
        REGISTER_COUNTER(s3_cp_bytes_uploaded, "Total bytes uploaded to S3");

        REGISTER_HISTOGRAM(s3_cp_flush_latency, "Total S3 CP flush latency in us",
                           HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(s3_cp_chunk_upload_latency, "Per-chunk upload latency in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~S3CpMetrics() { deregister_me_from_farm(); }
};

// ─── CP Context ──────────────────────────────────────────────────────────────

/**
 * @brief CP context for the S3 flush consumer.
 *
 * Tracks which chunks were dirty at the time of CP switchover so the flush
 * phase knows exactly what to upload.
 */
class S3CpContext : public CPContext {
public:
    explicit S3CpContext(CP* cp) : CPContext{cp} {}

    /// Chunk IDs that were dirty at switchover time.
    std::vector< chunk_id_t > dirty_chunk_ids;

    /// Dirty blocks per chunk, drained from S3PhysicalDev at switchover.
    std::map< chunk_id_t, std::vector< DirtyBlock > > dirty_blocks;
};

// ─── S3 CP Callbacks ─────────────────────────────────────────────────────────

/**
 * @brief CPCallbacks implementation that flushes dirty S3 chunks at checkpoint.
 *
 * Hooks into CPManager's checkpoint flow as a registered consumer. After NVMe
 * flush completes (other consumers), this handler:
 *
 * 1. Drains dirty cache from S3PhysicalDev (captured at switchover time)
 * 2. Uploads each dirty chunk via ChunkStore::put() (parallel, bounded concurrency)
 * 3. Updates pdev_s3 superblock on S3 (commit point — written last)
 * 4. On chunk upload failure: chunk stays dirty, retried next CP
 *
 * S3 flush ordering matches NVMe CP order:
 *   a. Data chunks → b. WAL chunks → c. Index chunks → d. MetaBlk chunks
 *   e. pdev_s3 superblock (commit point)
 *
 * Thread safety: switchover runs under CP lock; flush is async on CP fibers.
 */
class S3CpHandler : public CPCallbacks {
public:
    /**
     * @param s3_pdev          The S3 physical device (owns dirty cache)
     * @param upload_concurrency  Max parallel chunk uploads (from config)
     */
    S3CpHandler(std::shared_ptr< S3PhysicalDev > s3_pdev, uint32_t upload_concurrency = 4);

    ~S3CpHandler() override = default;

    // CPCallbacks interface

    /// Called at CP switchover: drain dirty cache into the CP context.
    std::unique_ptr< CPContext > on_switchover_cp(CP* cur_cp, CP* new_cp) override;

    /// Called to flush: upload dirty chunks to S3 via ChunkStore, then superblock.
    folly::Future< bool > cp_flush(CP* cp) override;

    /// Called after flush: cleanup.
    void cp_cleanup(CP* cp) override;

    /// Progress tracking.
    int cp_progress_percent() override;

    // ─── Accessors ───────────────────────────────────────────────────────

    S3CpMetrics& metrics() { return m_metrics; }

    /// Chunks that failed upload in the last CP (will be retried next CP).
    std::set< chunk_id_t > failed_chunks() const;

private:
    /// Upload a single chunk's dirty blocks via ChunkStore.
    S3Result upload_chunk(chunk_id_t chunk_id, const std::vector< DirtyBlock >& dirty_blocks);

    /// Upload chunks grouped by type in the correct order.
    void flush_chunks_ordered(S3CpContext* ctx);

    std::shared_ptr< S3PhysicalDev > m_s3_pdev;
    uint32_t m_upload_concurrency;

    /// Chunks that failed upload — carried over to next CP.
    mutable std::mutex m_failed_mutex;
    std::set< chunk_id_t > m_failed_chunks;

    /// Progress tracking
    std::atomic< int > m_progress_pct{0};
    std::atomic< uint32_t > m_total_chunks_to_flush{0};
    std::atomic< uint32_t > m_chunks_flushed{0};

    S3CpMetrics m_metrics;
};

} // namespace homestore
