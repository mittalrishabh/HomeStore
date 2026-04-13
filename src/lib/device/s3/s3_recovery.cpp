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

#include <folly/executors/GlobalExecutor.h>
#include <folly/futures/Future.h>
#include <homestore/s3/s3_recovery.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

S3RecoveryManager::S3RecoveryManager(std::shared_ptr< ChunkStore > chunk_store,
                                     std::shared_ptr< S3ObjectStore > s3_store,
                                     std::shared_ptr< NvmeRecoveryWriter > nvme_writer,
                                     std::string volume_id,
                                     uint32_t concurrency,
                                     uint32_t retry_count,
                                     uint32_t retry_backoff_ms)
    : m_chunk_store{std::move(chunk_store)},
      m_s3_store{std::move(s3_store)},
      m_nvme_writer{std::move(nvme_writer)},
      m_volume_id{std::move(volume_id)},
      m_concurrency{concurrency > 0 ? concurrency : 1u},
      m_retry_count{retry_count > 0 ? retry_count : 1u},
      m_retry_backoff_ms{retry_backoff_ms} {}

bool S3RecoveryManager::needs_recovery() {
    auto nvme_state = m_nvme_writer->check_nvme_state();
    if (nvme_state == NvmeState::VALID) {
        LOGDEBUGMOD(s3, "NVMe state is VALID — no recovery needed");
        return false;
    }

    LOGINFO("NVMe state is {} — checking S3 for recovery data",
            nvme_state == NvmeState::EMPTY ? "EMPTY" : "CORRUPTED");

    if (!load_superblock()) {
        LOGWARNMOD(s3, "No valid pdev_s3 superblock on S3 — recovery not possible");
        return false;
    }

    LOGINFO("S3 recovery available: {} chunks, generation={}",
            m_superblock.num_chunks(), m_superblock.generation());
    return true;
}

RecoveryResult S3RecoveryManager::recover_from_s3() {
    auto start_time = std::chrono::steady_clock::now();

    RecoveryResult result;

    if (!m_superblock_loaded && !load_superblock()) {
        result.error_message = "Failed to load pdev_s3 superblock from S3";
        LOGERRORMOD(s3, "{}", result.error_message);
        return result;
    }

    result.total_chunks = m_superblock.num_chunks();
    if (result.total_chunks == 0) {
        result.success = true;
        LOGINFO("S3 recovery: no chunks to recover (empty superblock)");
        return result;
    }

    LOGINFO("S3 recovery starting: {} chunks, generation={}, concurrency={}",
            result.total_chunks, m_superblock.generation(), m_concurrency);

    COUNTER_INCREMENT(m_metrics, recovery_chunks_total, result.total_chunks);

    // Recovery order: METABLK → WAL → INDEX → DATA
    // Essential chunks are small and downloaded first (serially for safety).
    // Data chunks are the bulk and downloaded in parallel.
    static constexpr std::array< S3ChunkType, 4 > RECOVERY_ORDER = {
        S3ChunkType::METABLK, S3ChunkType::WAL, S3ChunkType::INDEX, S3ChunkType::DATA};

    static constexpr std::array< const char*, 4 > TYPE_NAMES = {"METABLK", "WAL", "INDEX", "DATA"};

    for (size_t phase = 0; phase < RECOVERY_ORDER.size(); ++phase) {
        auto type = RECOVERY_ORDER[phase];
        auto entries = m_superblock.get_chunks_by_type(type);

        if (entries.empty()) continue;

        LOGINFO("S3 recovery phase {}/4 ({}): {} chunks",
                phase + 1, TYPE_NAMES[phase], entries.size());

        std::vector< ChunkRecoveryResult > phase_results;

        if (type == S3ChunkType::DATA) {
            // Data chunks in parallel batches
            phase_results = recover_batch(entries);
        } else {
            // Essential chunks serially (small, ordering matters for correctness)
            phase_results.reserve(entries.size());
            for (const auto* entry : entries) {
                phase_results.push_back(recover_single_chunk(*entry));
            }
        }

        for (auto& cr : phase_results) {
            if (cr.success) {
                result.chunks_recovered++;
                result.total_bytes += cr.bytes_downloaded;
                COUNTER_INCREMENT(m_metrics, recovery_chunks_succeeded, 1);
                COUNTER_INCREMENT(m_metrics, recovery_bytes_downloaded, cr.bytes_downloaded);
            } else {
                result.chunks_failed++;
                COUNTER_INCREMENT(m_metrics, recovery_chunks_failed, 1);
            }
            result.chunk_results.push_back(std::move(cr));
        }

        // Fail fast: if essential chunks fail, abort recovery
        if (type != S3ChunkType::DATA && result.chunks_failed > 0) {
            result.error_message = fmt::format(
                "Essential {} chunk recovery failed — aborting", TYPE_NAMES[phase]);
            LOGERRORMOD(s3, "{}", result.error_message);
            break;
        }
    }

    auto elapsed = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::steady_clock::now() - start_time);
    result.elapsed = elapsed;

    HISTOGRAM_OBSERVE(m_metrics, recovery_total_latency_ms, elapsed.count());

    result.success = (result.chunks_failed == 0);

    if (result.success) {
        LOGINFO("S3 recovery complete: {}/{} chunks, {} bytes, {} ms",
                result.chunks_recovered, result.total_chunks,
                result.total_bytes, elapsed.count());
    } else {
        LOGERRORMOD(s3, "S3 recovery failed: {}/{} chunks recovered, {} failed, {} ms — {}",
                    result.chunks_recovered, result.total_chunks,
                    result.chunks_failed, elapsed.count(), result.error_message);
    }

    return result;
}

RecoveryResult S3RecoveryManager::recover_essential_only() {
    auto start_time = std::chrono::steady_clock::now();

    RecoveryResult result;

    if (!m_superblock_loaded && !load_superblock()) {
        result.error_message = "Failed to load pdev_s3 superblock from S3";
        LOGERRORMOD(s3, "{}", result.error_message);
        return result;
    }

    static constexpr std::array< S3ChunkType, 3 > ESSENTIAL_ORDER = {
        S3ChunkType::METABLK, S3ChunkType::WAL, S3ChunkType::INDEX};

    static constexpr std::array< const char*, 3 > TYPE_NAMES = {"METABLK", "WAL", "INDEX"};

    uint64_t essential_total = 0;
    for (auto type : ESSENTIAL_ORDER) {
        essential_total += m_superblock.get_chunks_by_type(type).size();
    }

    result.total_chunks = essential_total;

    if (essential_total == 0) {
        result.success = true;
        LOGINFO("S3 essential recovery: no essential chunks to recover");
        return result;
    }

    LOGINFO("S3 essential recovery starting: {} essential chunks, generation={}",
            essential_total, m_superblock.generation());

    COUNTER_INCREMENT(m_metrics, recovery_chunks_total, essential_total);

    for (size_t phase = 0; phase < ESSENTIAL_ORDER.size(); ++phase) {
        auto type = ESSENTIAL_ORDER[phase];
        auto entries = m_superblock.get_chunks_by_type(type);

        if (entries.empty()) continue;

        LOGINFO("S3 essential recovery phase {}/3 ({}): {} chunks",
                phase + 1, TYPE_NAMES[phase], entries.size());

        for (const auto* entry : entries) {
            auto cr = recover_single_chunk(*entry);
            if (cr.success) {
                result.chunks_recovered++;
                result.total_bytes += cr.bytes_downloaded;
                COUNTER_INCREMENT(m_metrics, recovery_chunks_succeeded, 1);
                COUNTER_INCREMENT(m_metrics, recovery_bytes_downloaded, cr.bytes_downloaded);
            } else {
                result.chunks_failed++;
                COUNTER_INCREMENT(m_metrics, recovery_chunks_failed, 1);
            }
            result.chunk_results.push_back(std::move(cr));
        }

        if (result.chunks_failed > 0) {
            result.error_message = fmt::format(
                "Essential {} chunk recovery failed — aborting", TYPE_NAMES[phase]);
            LOGERRORMOD(s3, "{}", result.error_message);
            break;
        }
    }

    auto elapsed = std::chrono::duration_cast< std::chrono::milliseconds >(
        std::chrono::steady_clock::now() - start_time);
    result.elapsed = elapsed;

    HISTOGRAM_OBSERVE(m_metrics, recovery_total_latency_ms, elapsed.count());

    result.success = (result.chunks_failed == 0);

    if (result.success) {
        LOGINFO("S3 essential recovery complete: {}/{} chunks, {} bytes, {} ms (DATA skipped)",
                result.chunks_recovered, result.total_chunks,
                result.total_bytes, elapsed.count());
    } else {
        LOGERRORMOD(s3, "S3 essential recovery failed: {}/{} recovered, {} failed, {} ms — {}",
                    result.chunks_recovered, result.total_chunks,
                    result.chunks_failed, elapsed.count(), result.error_message);
    }

    return result;
}

ChunkRecoveryResult S3RecoveryManager::recover_single_chunk(const s3_chunk_entry& entry) {
    ChunkRecoveryResult result;
    result.chunk_id = entry.chunk_id;
    result.chunk_type = entry.chunk_type;

    for (uint32_t attempt = 0; attempt <= m_retry_count; ++attempt) {
        if (attempt > 0) {
            auto backoff_ms = m_retry_backoff_ms * (1u << (attempt - 1));
            LOGWARNMOD(s3, "S3 recovery: retrying chunk_id={} (attempt {}/{}), backoff={}ms",
                       entry.chunk_id, attempt + 1, m_retry_count + 1, backoff_ms);
            COUNTER_INCREMENT(m_metrics, recovery_retries, 1);
            // TODO(v2): use folly::futures::sleep() instead of blocking sleep to avoid starving the executor pool under concurrent failures
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        }

        auto chunk_start = std::chrono::steady_clock::now();
        auto state = m_chunk_store->recover(entry.chunk_id);
        auto chunk_elapsed = std::chrono::duration_cast< std::chrono::microseconds >(
            std::chrono::steady_clock::now() - chunk_start).count();

        HISTOGRAM_OBSERVE(m_metrics, recovery_chunk_download_latency_us, chunk_elapsed);

        if (!state.valid) {
            result.error_message = fmt::format("ChunkStore::recover() returned invalid for chunk_id={}",
                                               entry.chunk_id);
            LOGWARNMOD(s3, "{}", result.error_message);
            continue;
        }

        // TODO(v2): validate chunk CRC when superblock supports per-chunk checksums
        // Verify downloaded size matches expected chunk size
        if (state.data->size() != entry.chunk_size) {
            result.error_message = fmt::format(
                "Size mismatch for chunk_id={}: expected={} got={}",
                entry.chunk_id, entry.chunk_size, state.data->size());
            LOGWARNMOD(s3, "{}", result.error_message);
            continue;
        }

        // Write to NVMe
        auto ec = m_nvme_writer->write_chunk(entry.chunk_id, state.data);
        if (ec) {
            result.error_message = fmt::format(
                "NVMe write failed for chunk_id={}: {}", entry.chunk_id, ec.message());
            LOGERRORMOD(s3, "{}", result.error_message);
            continue;
        }

        result.bytes_downloaded = state.data->size();
        result.success = true;
        result.error_message.clear();

        LOGDEBUGMOD(s3, "S3 recovery: chunk_id={} type={} recovered {} bytes in {} us",
                    entry.chunk_id, static_cast< int >(entry.chunk_type),
                    result.bytes_downloaded, chunk_elapsed);
        return result;
    }

    LOGERRORMOD(s3, "S3 recovery: chunk_id={} failed after {} attempts — {}",
                entry.chunk_id, m_retry_count + 1, result.error_message);
    return result;
}

std::vector< ChunkRecoveryResult >
S3RecoveryManager::recover_batch(const std::vector< const s3_chunk_entry* >& entries) {
    std::vector< ChunkRecoveryResult > results;
    results.reserve(entries.size());

    for (size_t i = 0; i < entries.size(); i += m_concurrency) {
        size_t batch_end = std::min(i + static_cast< size_t >(m_concurrency), entries.size());

        std::vector< folly::Future< ChunkRecoveryResult > > futs;
        futs.reserve(batch_end - i);

        for (size_t j = i; j < batch_end; ++j) {
            const auto* entry = entries[j];
            // Capture entry by value (it's a pointer to superblock storage, stable)
            futs.emplace_back(
                folly::via(folly::getGlobalCPUExecutor().get(),
                           [this, entry]() -> ChunkRecoveryResult {
                               return recover_single_chunk(*entry);
                           }));
        }

        auto batch_results = folly::collectAll(std::move(futs)).get();
        for (auto& try_result : batch_results) {
            if (try_result.hasValue()) {
                results.push_back(std::move(try_result.value()));
            } else {
                ChunkRecoveryResult cr;
                cr.error_message = "Exception during chunk recovery";
                results.push_back(std::move(cr));
            }
        }

        // Progress logging: report after each batch
        uint64_t done = results.size();
        uint64_t total = entries.size();
        uint64_t succeeded = 0;
        for (const auto& r : results) {
            if (r.success) ++succeeded;
        }
        LOGINFO("S3 recovery progress: {}/{} data chunks downloaded ({} succeeded)",
                done, total, succeeded);
    }

    return results;
}

bool S3RecoveryManager::load_superblock() {
    if (m_superblock_loaded) return true;

    auto read_result = m_superblock.read_from_s3(*m_s3_store, m_volume_id);
    if (!read_result.ok()) {
        LOGWARNMOD(s3, "Failed to read pdev_s3 superblock from S3: {}", read_result.error_message);
        return false;
    }

    m_superblock_loaded = true;
    LOGINFO("S3 recovery: loaded superblock — pdev_id={} generation={} chunks={}",
            m_superblock.pdev_id(), m_superblock.generation(), m_superblock.num_chunks());
    return true;
}

} // namespace homestore
