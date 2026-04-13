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
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/s3/chunk_eviction_manager.h>
#include <homestore/s3/chunk_hydration_manager.h>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_recovery.h>

namespace homestore {

struct OnDemandRecoveryResult {
    bool success{false};
    uint64_t essential_chunks_total{0};
    uint64_t essential_chunks_recovered{0};
    uint64_t essential_bytes{0};
    uint64_t data_chunks_deferred{0};
    std::chrono::milliseconds elapsed{0};
    std::vector< ChunkRecoveryResult > essential_results;
    std::vector< chunk_id_t > deferred_chunk_ids;
    std::string error_message;
};

struct ProactiveHydrationConfig {
    uint32_t interval_ms{5000};
    uint32_t batch_size{4};
};

class OnDemandRecoveryMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit OnDemandRecoveryMetrics()
        : sisl::MetricsGroupWrapper{"OnDemandRecovery", "on_demand_recovery"} {
        REGISTER_COUNTER(essential_chunks_recovered, "Essential chunks recovered from S3");
        REGISTER_COUNTER(essential_bytes_downloaded, "Essential chunk bytes downloaded");
        REGISTER_COUNTER(data_chunks_deferred, "Data chunks deferred (S3-only)");
        REGISTER_COUNTER(hydration_chunks_completed, "Data chunks proactively hydrated");
        REGISTER_COUNTER(hydration_chunks_failed, "Proactive hydration failures");

        REGISTER_HISTOGRAM(essential_recovery_latency_ms, "Essential chunk recovery time in ms",
                           HistogramBucketsType(ExponentialOfTwoBuckets));

        register_me_to_farm();
    }

    ~OnDemandRecoveryMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief OnDemandRecoveryManager — fast startup via essential-chunk-only recovery.
 *
 * Instead of downloading all chunks from S3 (bulk recovery), this manager:
 *
 * 1. Downloads only essential chunks (METABLK, WAL, INDEX) to NVMe
 * 2. Marks data chunks as S3-only (on_nvme=false, on_s3=true)
 * 3. HomeStore initializes from essential chunks (same as normal NVMe boot)
 * 4. First read to any data chunk is served from S3 via TieredReadHandler
 * 5. Background thread proactively hydrates data chunks to NVMe
 *
 * The node starts serving reads within seconds of recovery starting.
 */
class OnDemandRecoveryManager {
public:
    OnDemandRecoveryManager(std::shared_ptr< ChunkStore > chunk_store,
                            std::shared_ptr< S3ObjectStore > s3_store,
                            std::shared_ptr< NvmeRecoveryWriter > nvme_writer,
                            std::shared_ptr< ChunkEvictionManager > eviction_mgr,
                            std::string volume_id,
                            uint32_t retry_count = 3,
                            uint32_t retry_backoff_ms = 1000);

    ~OnDemandRecoveryManager();

    OnDemandRecoveryManager(const OnDemandRecoveryManager&) = delete;
    OnDemandRecoveryManager& operator=(const OnDemandRecoveryManager&) = delete;

    bool needs_recovery();

    OnDemandRecoveryResult recover_essential_chunks();

    void start_proactive_hydration(std::shared_ptr< ChunkHydrationManager > hydration_mgr,
                                   ProactiveHydrationConfig config = {});

    void stop_proactive_hydration();

    bool is_hydration_running() const { return m_hydration_running.load(std::memory_order_relaxed); }

    uint64_t hydrated_count() const { return m_hydrated_count.load(std::memory_order_relaxed); }

    uint64_t deferred_remaining() const;

    const PdevS3Superblock& superblock() const { return m_recovery_mgr->superblock(); }

    OnDemandRecoveryMetrics& metrics() { return m_metrics; }

private:
    void hydration_loop(std::shared_ptr< ChunkHydrationManager > hydration_mgr,
                        ProactiveHydrationConfig config);

    std::unique_ptr< S3RecoveryManager > m_recovery_mgr;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;

    mutable std::mutex m_deferred_mtx;
    std::vector< s3_chunk_entry > m_deferred_data_chunks;

    std::atomic< bool > m_hydration_running{false};
    std::atomic< bool > m_hydration_stop{false};
    std::atomic< uint64_t > m_hydrated_count{0};
    std::thread m_hydration_thread;

    OnDemandRecoveryMetrics m_metrics;
};

} // namespace homestore
