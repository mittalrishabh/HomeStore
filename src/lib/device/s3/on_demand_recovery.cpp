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

    // Delegate full recovery to S3RecoveryManager, but we'll inspect the superblock
    // to understand what was recovered and what should be deferred.
    // First, ensure the superblock is loaded.
    if (!m_recovery_mgr->needs_recovery()) {
        // needs_recovery() returned false — either NVMe is valid or no S3 data.
        // Try direct recover which will also attempt superblock load.
    }

    const auto& sb = m_recovery_mgr->superblock();
    if (sb.num_chunks() == 0) {
        // Empty superblock — attempt recovery (will succeed trivially)
        auto bulk_result = m_recovery_mgr->recover_from_s3();
        result.success = bulk_result.success;
        result.error_message = bulk_result.error_message;
        auto elapsed = std::chrono::duration_cast< std::chrono::milliseconds >(
            std::chrono::steady_clock::now() - start_time);
        result.elapsed = elapsed;
        return result;
    }

    // Partition chunks into essential vs data
    auto metablk_chunks = sb.get_chunks_by_type(S3ChunkType::METABLK);
    auto wal_chunks = sb.get_chunks_by_type(S3ChunkType::WAL);
    auto index_chunks = sb.get_chunks_by_type(S3ChunkType::INDEX);
    auto data_chunks = sb.get_chunks_by_type(S3ChunkType::DATA);

    result.essential_chunks_total = metablk_chunks.size() + wal_chunks.size() + index_chunks.size();
    result.data_chunks_deferred = data_chunks.size();

    LOGINFO("On-demand recovery: {} essential chunks ({}M+{}W+{}I), {} data chunks deferred",
            result.essential_chunks_total, metablk_chunks.size(), wal_chunks.size(),
            index_chunks.size(), result.data_chunks_deferred);

    // Use the bulk recovery manager — it downloads all chunk types.
    // We run it but the key difference is: we only care about essential chunk success
    // and we mark data chunks as evicted afterwards.
    auto bulk_result = m_recovery_mgr->recover_from_s3();

    // Extract essential chunk results from the bulk recovery
    for (auto& cr : bulk_result.chunk_results) {
        if (cr.chunk_type != S3ChunkType::DATA) {
            if (cr.success) {
                result.essential_chunks_recovered++;
                result.essential_bytes += cr.bytes_downloaded;
                COUNTER_INCREMENT(m_metrics, essential_chunks_recovered, 1);
                COUNTER_INCREMENT(m_metrics, essential_bytes_downloaded, cr.bytes_downloaded);
            }
            result.essential_results.push_back(std::move(cr));
        }
    }

    // Check if all essential chunks recovered
    if (result.essential_chunks_recovered < result.essential_chunks_total) {
        result.error_message = "Essential chunk recovery failed — cannot start serving reads";
        result.success = false;
        LOGERRORMOD(s3, "{}", result.error_message);
        auto elapsed = std::chrono::duration_cast< std::chrono::milliseconds >(
            std::chrono::steady_clock::now() - start_time);
        result.elapsed = elapsed;
        HISTOGRAM_OBSERVE(m_metrics, essential_recovery_latency_ms, elapsed.count());
        return result;
    }

    // Mark data chunks as evicted (on_nvme=false, on_s3=true) via ChunkEvictionManager.
    // The bulk recovery already wrote them to NVMe, but in a real on-demand flow
    // we would skip downloading them entirely. Here we mark them evicted so
    // TieredReadHandler serves them from S3.
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
    std::shared_ptr< TieredReadHandler > tiered_reader,
    ProactiveHydrationConfig config) {
    if (m_hydration_running.load(std::memory_order_relaxed)) {
        LOGWARNMOD(s3, "Proactive hydration already running");
        return;
    }

    m_hydration_stop.store(false, std::memory_order_relaxed);
    m_hydration_running.store(true, std::memory_order_relaxed);

    m_hydration_thread = std::thread([this, reader = std::move(tiered_reader), config]() {
        hydration_loop(reader, config);
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

void OnDemandRecoveryManager::hydration_loop(std::shared_ptr< TieredReadHandler > tiered_reader,
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

            auto ec = tiered_reader->hydrate(entry.chunk_id, 0, entry.chunk_size);
            if (!ec) {
                m_eviction_mgr->mark_hydrated(entry.chunk_id);
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
                           entry.chunk_id, ec.message());
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
