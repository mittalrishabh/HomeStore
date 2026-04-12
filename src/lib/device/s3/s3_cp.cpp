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

#include <folly/executors/GlobalExecutor.h>
#include <folly/futures/Future.h>
#include <homestore/s3/s3_cp.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

S3CpCallbacks::S3CpCallbacks(std::vector< S3PhysicalDev* > s3_pdevs, uint32_t upload_concurrency)
    : m_s3_pdevs{std::move(s3_pdevs)},
      m_upload_concurrency{upload_concurrency > 0 ? upload_concurrency : 1u} {
    LOGDEBUGMOD(s3, "S3CpCallbacks created with {} S3 pdevs, upload_concurrency={}",
                m_s3_pdevs.size(), m_upload_concurrency);
}

std::unique_ptr< CPContext > S3CpCallbacks::on_switchover_cp(CP* cur_cp, CP* new_cp) {
    auto ctx = std::make_unique< S3CpContext >(new_cp);

    for (auto* pdev : m_s3_pdevs) {
        auto dirty_chunks = pdev->get_dirty_chunks();
        bool has_dirty = !dirty_chunks.empty();

        // Check if there are failed chunks from previous CP to retry
        std::set< chunk_id_t > retry_chunks;
        {
            std::lock_guard< std::mutex > lock{m_failed_mutex};
            auto it = m_failed_chunks.find(pdev->pdev_id());
            if (it != m_failed_chunks.end()) {
                retry_chunks = std::move(it->second);
                m_failed_chunks.erase(it);
            }
        }

        if (!has_dirty && retry_chunks.empty()) continue;

        // Drain dirty cache atomically — new writes go to next CP
        auto drained = pdev->drain_all_dirty_cache();

        // Add retry chunks with empty dirty_blocks.
        // ChunkStore::put() with empty blocks will re-read full chunk from
        // NVMe and re-upload — exactly what we want for retry.
        for (auto cid : retry_chunks) {
            if (drained.find(cid) == drained.end()) {
                drained[cid] = {};
                LOGDEBUGMOD(s3, "S3 CP switchover: retrying failed chunk_id={} from previous CP", cid);
                COUNTER_INCREMENT(m_metrics, s3_cp_retry_chunks, 1);
            }
        }

        ctx->m_total_chunks.fetch_add(drained.size(), std::memory_order_relaxed);
        LOGDEBUGMOD(s3, "S3 CP switchover: pdev_id={} drained {} dirty chunks ({} retries)",
                    pdev->pdev_id(), drained.size(), retry_chunks.size());

        ctx->m_dirty_pdevs.push_back(pdev);
        ctx->m_drained_data.push_back(std::move(drained));
    }

    m_total_chunks_this_cp.store(ctx->m_total_chunks.load(), std::memory_order_relaxed);
    m_flushed_chunks_this_cp.store(0, std::memory_order_relaxed);

    LOGDEBUGMOD(s3, "S3 CP switchover: {} pdevs, {} total chunks",
                ctx->m_dirty_pdevs.size(), ctx->m_total_chunks.load());
    return ctx;
}

folly::Future< bool > S3CpCallbacks::cp_flush(CP* cp) {
    auto* ctx = static_cast< S3CpContext* >(cp->context(cp_consumer_t::S3_SVC));
    if (!ctx || ctx->m_dirty_pdevs.empty()) {
        LOGDEBUGMOD(s3, "S3 CP flush: nothing to flush for cp_id={}", cp->id());
        if (ctx) { ctx->complete(true); }
        return folly::makeFuture< bool >(true);
    }

    auto start_time = std::chrono::steady_clock::now();
    COUNTER_INCREMENT(m_metrics, s3_cp_flush_count, 1);

    LOGDEBUGMOD(s3, "S3 CP flush starting: cp_id={} pdevs={} chunks={}",
                cp->id(), ctx->m_dirty_pdevs.size(), ctx->m_total_chunks.load());

    // Chain pdev flushes sequentially (each pdev flushes its chunks concurrently)
    folly::Future< bool > chain = folly::makeFuture< bool >(true);

    for (size_t i = 0; i < ctx->m_dirty_pdevs.size(); ++i) {
        auto* pdev = ctx->m_dirty_pdevs[i];
        auto& drained = ctx->m_drained_data[i];

        chain = std::move(chain).thenValue(
            [this, pdev, &drained, ctx](bool prev_ok) mutable -> folly::Future< bool > {
                return flush_pdev(pdev, drained, ctx).thenValue(
                    [prev_ok](bool this_ok) { return prev_ok && this_ok; });
            });
    }

    return std::move(chain).thenValue(
        [this, ctx, start_time, cp_id = cp->id()](bool all_success) -> bool {
            auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
                std::chrono::steady_clock::now() - start_time).count();
            HISTOGRAM_OBSERVE(m_metrics, s3_cp_flush_latency_us, elapsed_us);

            if (!all_success) {
                COUNTER_INCREMENT(m_metrics, s3_cp_flush_errors, 1);
                LOGERRORMOD(s3, "S3 CP flush completed with errors for cp_id={} ({}us)", cp_id, elapsed_us);
            } else {
                LOGDEBUGMOD(s3, "S3 CP flush completed: cp_id={} {}us chunks={}",
                            cp_id, elapsed_us, ctx->m_flushed_chunks.load());
            }

            ctx->complete(all_success);
            return all_success;
        });
}

void S3CpCallbacks::cp_cleanup(CP* /* cp */) {}

int S3CpCallbacks::cp_progress_percent() {
    auto total = m_total_chunks_this_cp.load(std::memory_order_relaxed);
    if (total == 0) return 100;
    auto flushed = m_flushed_chunks_this_cp.load(std::memory_order_relaxed);
    return static_cast< int >((flushed * 100) / total);
}

void S3CpCallbacks::trigger_early_cp_flush() {
    LOGWARNMOD(s3, "S3 dirty cache threshold exceeded — triggering early CP flush");
    COUNTER_INCREMENT(m_metrics, s3_cp_early_triggers, 1);
    cp_mgr().trigger_cp_flush(true /* force */);
}

std::set< chunk_id_t > S3CpCallbacks::failed_chunks(uint32_t pdev_id) const {
    std::lock_guard< std::mutex > lock{m_failed_mutex};
    auto it = m_failed_chunks.find(pdev_id);
    if (it != m_failed_chunks.end()) return it->second;
    return {};
}

folly::Future< bool > S3CpCallbacks::flush_pdev(S3PhysicalDev* pdev,
                                                  std::map< chunk_id_t, std::vector< DirtyBlock > >& drained,
                                                  S3CpContext* ctx) {
    if (drained.empty()) {
        return folly::makeFuture< bool >(true);
    }

    auto* chunk_store = pdev->chunk_store();
    auto ordered_chunks = sort_chunks_by_flush_order(drained, pdev->superblock());

    LOGDEBUGMOD(s3, "S3 CP flushing pdev_id={} ({} chunks, concurrency={})",
                pdev->pdev_id(), ordered_chunks.size(), m_upload_concurrency);

    // Group chunks by type for ordered flushing
    static constexpr std::array< S3ChunkType, 4 > FLUSH_ORDER = {
        S3ChunkType::DATA, S3ChunkType::WAL, S3ChunkType::INDEX, S3ChunkType::METABLK};

    std::set< chunk_id_t > all_failed;

    for (auto type : FLUSH_ORDER) {
        // Collect chunks of this type from the ordered list
        std::vector< chunk_id_t > type_chunks;
        for (auto cid : ordered_chunks) {
            auto* entry = pdev->superblock().find_chunk(cid);
            if (entry && entry->chunk_type == type) {
                type_chunks.push_back(cid);
            } else if (!entry && type == S3ChunkType::DATA) {
                // Chunks not in superblock default to DATA ordering
                type_chunks.push_back(cid);
            }
        }

        if (type_chunks.empty()) continue;

        // Upload this type group with bounded concurrency
        auto failed = upload_batch(chunk_store, pdev->superblock(), type_chunks, drained, ctx);
        all_failed.insert(failed.begin(), failed.end());
    }

    // Record failed chunks for retry in next CP
    if (!all_failed.empty()) {
        std::lock_guard< std::mutex > lock{m_failed_mutex};
        m_failed_chunks[pdev->pdev_id()] = std::move(all_failed);
    }

    // Count total bytes for metrics
    uint64_t total_bytes = 0;
    for (auto& [cid, blocks] : drained) {
        for (const auto& db : blocks) {
            if (db.data) { total_bytes += db.data->size(); }
        }
    }
    COUNTER_INCREMENT(m_metrics, s3_cp_bytes_flushed, total_bytes);

    bool has_failures = false;
    {
        std::lock_guard< std::mutex > lock{m_failed_mutex};
        auto it = m_failed_chunks.find(pdev->pdev_id());
        has_failures = (it != m_failed_chunks.end() && !it->second.empty());
    }

    if (has_failures) {
        // Don't write superblock — previous generation stays valid on S3
        LOGERRORMOD(s3, "S3 CP: skipping superblock for pdev_id={} due to chunk failures", pdev->pdev_id());
        return folly::makeFuture< bool >(false);
    }

    // Write superblock — S3 commit point (written LAST)
    LOGDEBUGMOD(s3, "S3 CP: writing superblock for pdev_id={} (commit point)", pdev->pdev_id());
    auto sb_result = pdev->write_superblock();
    if (!sb_result.ok()) {
        LOGERRORMOD(s3, "S3 CP: superblock write failed for pdev_id={}: {}",
                    pdev->pdev_id(), sb_result.error_message);
        return folly::makeFuture< bool >(false);
    }
    COUNTER_INCREMENT(m_metrics, s3_cp_superblock_writes, 1);

    LOGDEBUGMOD(s3, "S3 CP: pdev_id={} complete — {} chunks, {} bytes, gen={}",
                pdev->pdev_id(), ordered_chunks.size(), total_bytes, pdev->superblock().generation());
    return folly::makeFuture< bool >(true);
}

std::set< chunk_id_t > S3CpCallbacks::upload_batch(
    ChunkStore* chunk_store,
    const PdevS3Superblock& superblock,
    const std::vector< chunk_id_t >& chunk_ids,
    std::map< chunk_id_t, std::vector< DirtyBlock > >& drained,
    S3CpContext* ctx) {

    std::set< chunk_id_t > failed;

    // Upload in batches of m_upload_concurrency using folly::collectAll
    for (size_t i = 0; i < chunk_ids.size(); i += m_upload_concurrency) {
        size_t batch_end = std::min(i + static_cast< size_t >(m_upload_concurrency), chunk_ids.size());

        std::vector< folly::Future< std::pair< chunk_id_t, S3Result > > > futs;
        futs.reserve(batch_end - i);

        for (size_t j = i; j < batch_end; ++j) {
            auto cid = chunk_ids[j];
            auto it = drained.find(cid);
            if (it == drained.end()) continue;

            auto* entry = superblock.find_chunk(cid);
            uint64_t chunk_size = entry ? entry->chunk_size : 0;

            // Copy dirty blocks for the lambda (they're shared_ptrs, cheap)
            auto blocks_copy = it->second;

            futs.emplace_back(
                folly::via(folly::getGlobalCPUExecutor().get(),
                           [chunk_store, cid, blocks = std::move(blocks_copy), chunk_size, this]() mutable
                               -> std::pair< chunk_id_t, S3Result > {
                               auto start = std::chrono::steady_clock::now();
                               auto result = chunk_store->put(cid, blocks, chunk_size);
                               auto elapsed = std::chrono::duration_cast< std::chrono::microseconds >(
                                   std::chrono::steady_clock::now() - start).count();
                               HISTOGRAM_OBSERVE(m_metrics, s3_cp_chunk_put_latency_us, elapsed);
                               return {cid, result};
                           }));
        }

        // Wait for this batch
        auto results = folly::collectAll(std::move(futs)).get();
        for (auto& try_result : results) {
            if (try_result.hasValue()) {
                auto& [cid, s3_result] = try_result.value();
                if (s3_result.ok()) {
                    COUNTER_INCREMENT(m_metrics, s3_cp_chunks_flushed, 1);
                    ctx->m_flushed_chunks.fetch_add(1, std::memory_order_relaxed);
                    m_flushed_chunks_this_cp.fetch_add(1, std::memory_order_relaxed);
                } else {
                    LOGERRORMOD(s3, "S3 CP: chunk put failed for chunk_id={}: {}",
                                cid, s3_result.error_message);
                    COUNTER_INCREMENT(m_metrics, s3_cp_chunks_failed, 1);
                    failed.insert(cid);
                }
            } else {
                LOGERRORMOD(s3, "S3 CP: chunk upload threw exception");
            }
        }
    }

    return failed;
}

std::vector< chunk_id_t > S3CpCallbacks::sort_chunks_by_flush_order(
    const std::map< chunk_id_t, std::vector< DirtyBlock > >& drained,
    const PdevS3Superblock& superblock) const {

    static constexpr std::array< S3ChunkType, 4 > FLUSH_ORDER = {
        S3ChunkType::DATA, S3ChunkType::WAL, S3ChunkType::INDEX, S3ChunkType::METABLK};

    std::vector< chunk_id_t > result;
    result.reserve(drained.size());
    std::set< chunk_id_t > added;

    for (auto type : FLUSH_ORDER) {
        for (auto& [chunk_id, _] : drained) {
            auto* entry = superblock.find_chunk(chunk_id);
            if (entry && entry->chunk_type == type) {
                result.push_back(chunk_id);
                added.insert(chunk_id);
            }
        }
    }

    // Defensive: any chunks not in superblock
    for (auto& [chunk_id, _] : drained) {
        if (added.find(chunk_id) == added.end()) {
            LOGWARNMOD(s3, "S3 CP: chunk_id={} not in superblock during flush ordering", chunk_id);
            result.push_back(chunk_id);
        }
    }

    return result;
}

} // namespace homestore
