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

#include <algorithm>
#include <chrono>
#include <thread>

#include <homestore/s3/on_demand_recovery.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

OnDemandRecoveryManager::OnDemandRecoveryManager(std::shared_ptr< ChunkStore > chunk_store,
                                                 std::shared_ptr< S3ObjectStore > s3_store,
                                                 std::shared_ptr< NvmeRecoveryWriter > nvme_writer,
                                                 std::shared_ptr< ChunkEvictionManager > eviction_mgr,
                                                 std::string volume_id,
                                                 uint32_t retry_count,
                                                 uint32_t retry_backoff_ms)
    : m_recovery_mgr{std::make_unique< S3RecoveryManager >(
          chunk_store, s3_store, nvme_writer, volume_id,
          /*concurrency=*/1, retry_count, retry_backoff_ms)},
      m_eviction_mgr{std::move(eviction_mgr)} {}

OnDemandRecoveryManager::~OnDemandRecoveryManager() { stop_proactive_hydration(); }

bool OnDemandRecoveryManager::needs_recovery() { return m_recovery_mgr->needs_recovery(); }

OnDemandRecoveryResult OnDemandRecoveryManager::recover_essential_chunks() {
    auto start_time = std::chrono::steady_clock::now();
    OnDemandRecoveryResult result;

    if (!m_recovery_mgr->needs_recovery()) {
        // NVMe is valid or no S3 data — nothing to do
    }

    const auto& sb = m_recovery_mgr->superblock();
    auto data_chunks = sb.get_chunks_by_type(S3ChunkType::DATA);
    result.data_chunks_deferred = data_chunks.size();

    auto essential_result = m_recovery_mgr->recover_essential_only();

    result.essential_chunks_total = essential_result.total_chunks;
    result.essential_chunks_recovered = essential_result.chunks_recovered;
    result.essential_bytes = essential_result.total_bytes;
    result.essential_results = std::move(essential_result.chunk_results);

    for (uint64_t i = 0; i < result.essential_chunks_recovered; ++i) {
        COUNTER_INCREMENT(m_metrics, essential_chunks_recovered, 1);
    }
    COUNTER_INCREMENT(m_metrics, essential_bytes_downloaded, result.essential_bytes);

    if (!essential_result.success) {
        result.error_message = essential_result.error_message.empty()
            ? "Essential chunk recovery failed — cannot start serving reads"
            : essential_result.error_message;
        result.success = false;
        LOGERRORMOD(s3, "{}", result.error_message);
        auto elapsed = std::chrono::duration_cast< std::chrono::milliseconds >(
            std::chrono::steady_clock::now() - start_time);
        result.elapsed = elapsed;
        HISTOGRAM_OBSERVE(m_metrics, essential_recovery_latency_ms, elapsed.count());
        return result;
    }

    // Mark data chunks as evicted (on_nvme=false, on_s3=true) — they were never
    // downloaded, so TieredReadHandler will serve them from S3 on first access.
    {
        std::lock_guard lock{m_deferred_mtx};
        m_deferred_data_chunks.reserve(data_chunks.size());
        for (const auto* entry : data_chunks) {
            m_eviction_mgr->evict_chunk(entry->chunk_id, entry->chunk_size);
            m_deferred_data_chunks.push_back(*entry);
            result.deferred_chunk_ids.push_back(entry->chunk_id);
        }
    }

    COUNTER_INCREMENT(m_metrics, data_chunks_deferred, result.data_chunks_deferred);

    result.success = true;

    auto elapsed = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::steady_clock::now() - start_time);
    result.elapsed = elapsed;
    HISTOGRAM_OBSERVE(m_metrics, essential_recovery_latency_ms, elapsed.count());

    LOGINFO("On-demand recovery complete: {}/{} essential chunks in {} ms, {} data chunks deferred to S3",
            result.essential_chunks_recovered, result.essential_chunks_total,
            elapsed.count(), result.data_chunks_deferred);

    return result;
}

void OnDemandRecoveryManager::start_proactive_hydration(
    std::shared_ptr< ChunkHydrationManager > hydration_mgr,
    ProactiveHydrationConfig config) {
    if (m_hydration_running.load(std::memory_order_relaxed)) {
        LOGWARNMOD(s3, "Proactive hydration already running");
        return;
    }

    m_hydration_stop.store(false, std::memory_order_relaxed);
    m_hydration_running.store(true, std::memory_order_relaxed);

    m_hydration_thread = std::thread([this, mgr = std::move(hydration_mgr), config]() {
        hydration_loop(mgr, config);
    });
}

void OnDemandRecoveryManager::stop_proactive_hydration() {
    m_hydration_stop.store(true, std::memory_order_relaxed);
    if (m_hydration_thread.joinable()) {
        m_hydration_thread.join();
    }
    m_hydration_running.store(false, std::memory_order_relaxed);
}

uint64_t OnDemandRecoveryManager::deferred_remaining() const {
    std::lock_guard lock{m_deferred_mtx};
    return m_deferred_data_chunks.size();
}

void OnDemandRecoveryManager::hydration_loop(std::shared_ptr< ChunkHydrationManager > hydration_mgr,
                                             ProactiveHydrationConfig config) {
    LOGINFO("Proactive hydration thread started: interval={}ms batch={}",
            config.interval_ms, config.batch_size);

    while (!m_hydration_stop.load(std::memory_order_relaxed)) {
        std::vector< s3_chunk_entry > batch;
        {
            std::lock_guard lock{m_deferred_mtx};
            if (m_deferred_data_chunks.empty()) {
                LOGINFO("Proactive hydration complete — all data chunks hydrated");
                break;
            }

            auto count = std::min(static_cast< size_t >(config.batch_size),
                                  m_deferred_data_chunks.size());
            batch.assign(m_deferred_data_chunks.end() - static_cast< ptrdiff_t >(count),
                         m_deferred_data_chunks.end());
        }

        for (const auto& entry : batch) {
            if (m_hydration_stop.load(std::memory_order_relaxed)) break;

            auto hr = hydration_mgr->hydrate_sync(entry.chunk_id, entry.chunk_size);
            if (hr == HydrationResult::SUCCESS || hr == HydrationResult::ALREADY_ON_NVME) {
                m_hydrated_count.fetch_add(1, std::memory_order_relaxed);
                COUNTER_INCREMENT(m_metrics, hydration_chunks_completed, 1);

                std::lock_guard lock{m_deferred_mtx};
                auto it = std::find_if(m_deferred_data_chunks.begin(), m_deferred_data_chunks.end(),
                                       [&](const auto& e) { return e.chunk_id == entry.chunk_id; });
                if (it != m_deferred_data_chunks.end()) {
                    m_deferred_data_chunks.erase(it);
                }

                LOGDEBUGMOD(s3, "Proactive hydration: chunk_id={} hydrated ({} remaining)",
                            entry.chunk_id, m_deferred_data_chunks.size());
            } else {
                LOGWARNMOD(s3, "Proactive hydration: chunk_id={} failed: {}",
                           entry.chunk_id, to_string(hr));
                COUNTER_INCREMENT(m_metrics, hydration_chunks_failed, 1);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(config.interval_ms));
    }

    m_hydration_running.store(false, std::memory_order_relaxed);
    LOGINFO("Proactive hydration thread stopped: {} chunks hydrated, {} remaining",
            m_hydrated_count.load(), deferred_remaining());
}

} // namespace homestore
