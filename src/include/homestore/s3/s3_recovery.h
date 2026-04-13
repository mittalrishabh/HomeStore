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
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/s3/chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_object_store.h>

namespace homestore {

/// Result of an NVMe validity check.
enum class NvmeState : uint8_t {
    VALID = 0,     ///< NVMe has valid HomeStore data
    EMPTY = 1,     ///< NVMe is empty / zeroed
    CORRUPTED = 2, ///< NVMe has data but superblock is invalid
};

/// Per-chunk result from recovery download.
struct ChunkRecoveryResult {
    chunk_id_t chunk_id{0};
    S3ChunkType chunk_type{S3ChunkType::DATA};
    uint64_t bytes_downloaded{0};
    bool success{false};
    std::string error_message;
};

/// Overall result of the bulk recovery operation.
struct RecoveryResult {
    bool success{false};
    uint64_t total_chunks{0};
    uint64_t chunks_recovered{0};
    uint64_t chunks_failed{0};
    uint64_t total_bytes{0};
    std::chrono::milliseconds elapsed{0};
    std::vector< ChunkRecoveryResult > chunk_results;
    std::string error_message;
};

/// Metrics for recovery operations.
class S3RecoveryMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit S3RecoveryMetrics() : sisl::MetricsGroupWrapper{"S3Recovery", "s3_recovery"} {
        REGISTER_COUNTER(recovery_chunks_total, "Total chunks to recover");
        REGISTER_COUNTER(recovery_chunks_succeeded, "Chunks recovered successfully");
        REGISTER_COUNTER(recovery_chunks_failed, "Chunks that failed recovery");
        REGISTER_COUNTER(recovery_bytes_downloaded, "Total bytes downloaded from S3");
        REGISTER_COUNTER(recovery_retries, "Total retry attempts");

        REGISTER_HISTOGRAM(recovery_chunk_download_latency_us, "Per-chunk download latency in us",
                           HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(recovery_total_latency_ms, "Total recovery time in ms",
                           HistogramBucketsType(ExponentialOfTwoBuckets));

        register_me_to_farm();
    }

    ~S3RecoveryMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief Callback interface for writing recovered chunk data to NVMe.
 *
 * Isolates S3RecoveryManager from direct PhysicalDev coupling so it
 * can be tested with a mock NVMe backend.
 */
class NvmeRecoveryWriter {
public:
    virtual ~NvmeRecoveryWriter() = default;

    /// Write recovered chunk data to NVMe at the appropriate location.
    virtual std::error_code write_chunk(chunk_id_t chunk_id, const sisl::byte_array& data) = 0;

    /// Check if NVMe has valid HomeStore data.
    virtual NvmeState check_nvme_state() = 0;
};

/**
 * @brief S3RecoveryManager — bulk recovery of all chunks from S3 to NVMe.
 *
 * On startup, if NVMe is empty/wiped, this manager downloads all chunks
 * from S3 to restore local state:
 *
 * 1. Read pdev_s3 superblock from S3 → discover all chunk_ids and types
 * 2. Download essential chunks first (MetaBlk, WAL, Index)
 * 3. Download data chunks in parallel via chunk_store->recover()
 * 4. Write each recovered chunk to NVMe via NvmeRecoveryWriter
 *
 * After recovery, HomeStore initializes from MetaBlk + WAL + B+tree
 * (same as normal NVMe boot) and replays binlog for post-CP mutations.
 *
 * Thread safety: recover_from_s3() must be called from a single thread
 * (startup path). It is not reentrant.
 */
class S3RecoveryManager {
public:
    /**
     * @brief Construct an S3RecoveryManager.
     *
     * @param chunk_store       ChunkStore for downloading chunk data
     * @param s3_store          S3ObjectStore for reading the pdev_s3 superblock
     * @param nvme_writer       Callback for writing recovered data to NVMe
     * @param volume_id         Volume ID for S3 key construction
     * @param concurrency       Max parallel downloads (uses s3.upload_concurrency)
     * @param retry_count       Retries per failed chunk download
     * @param retry_backoff_ms  Initial backoff between retries
     */
    S3RecoveryManager(std::shared_ptr< ChunkStore > chunk_store,
                      std::shared_ptr< S3ObjectStore > s3_store,
                      std::shared_ptr< NvmeRecoveryWriter > nvme_writer,
                      std::string volume_id,
                      uint32_t concurrency = 4,
                      uint32_t retry_count = 3,
                      uint32_t retry_backoff_ms = 1000);

    ~S3RecoveryManager() = default;

    S3RecoveryManager(const S3RecoveryManager&) = delete;
    S3RecoveryManager& operator=(const S3RecoveryManager&) = delete;

    /**
     * @brief Check if recovery is needed by examining NVMe state.
     *
     * @return true if NVMe is empty/corrupted and S3 has valid data
     */
    bool needs_recovery();

    /**
     * @brief Execute bulk recovery from S3 to NVMe.
     *
     * Downloads all chunks from S3 in priority order:
     *   1. METABLK chunks (CP superblock, bitmap, service state)
     *   2. WAL chunks (log replay)
     *   3. INDEX chunks (B+tree nodes)
     *   4. DATA chunks (user data — largest, parallelized)
     *
     * Each chunk is verified (ChunkState::valid) and written to NVMe.
     * Progress is logged at regular intervals.
     *
     * @return RecoveryResult with per-chunk details and overall status
     */
    RecoveryResult recover_from_s3();

    /**
     * @brief Recover only essential chunks (METABLK, WAL, INDEX) — skip DATA.
     *
     * Used by OnDemandRecoveryManager for fast startup. Data chunks are left
     * on S3 and served via TieredReadHandler until proactively hydrated.
     */
    RecoveryResult recover_essential_only();

    /// Get the loaded superblock (valid after needs_recovery() or recover_from_s3())
    const PdevS3Superblock& superblock() const { return m_superblock; }

    S3RecoveryMetrics& metrics() { return m_metrics; }

private:
    /// Download and write a single chunk with retry logic.
    ChunkRecoveryResult recover_single_chunk(const s3_chunk_entry& entry);

    /// Download a batch of chunks in parallel with bounded concurrency.
    std::vector< ChunkRecoveryResult > recover_batch(const std::vector< const s3_chunk_entry* >& entries);

    /// Load the pdev_s3 superblock from S3.
    bool load_superblock();

    std::shared_ptr< ChunkStore > m_chunk_store;
    std::shared_ptr< S3ObjectStore > m_s3_store;
    std::shared_ptr< NvmeRecoveryWriter > m_nvme_writer;
    std::string m_volume_id;
    uint32_t m_concurrency;
    uint32_t m_retry_count;
    uint32_t m_retry_backoff_ms;

    PdevS3Superblock m_superblock;
    bool m_superblock_loaded{false};

    S3RecoveryMetrics m_metrics;
};

} // namespace homestore
