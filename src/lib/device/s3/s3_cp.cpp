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

#include <folly/executors/InlineExecutor.h>
#include <homestore/s3/s3_cp.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

S3CpCallbacks::S3CpCallbacks(std::vector< S3PhysicalDev* > s3_pdevs)
    : m_s3_pdevs{std::move(s3_pdevs)} {
    LOGDEBUGMOD(s3, "S3CpCallbacks created with {} S3 pdevs", m_s3_pdevs.size());
}

std::unique_ptr< CPContext > S3CpCallbacks::on_switchover_cp(CP* cur_cp, CP* new_cp) {
    auto ctx = std::make_unique< S3CpContext >(new_cp);

    // Snapshot and drain dirty data from each pdev atomically.
    // New writes after this point go to the new CP's dirty cache.
    for (auto* pdev : m_s3_pdevs) {
        auto dirty_chunks = pdev->get_dirty_chunks();
        if (!dirty_chunks.empty()) {
            // Drain immediately at switchover — this is the atomic handoff.
            // The drained data belongs to the current CP; new writes
            // from here forward accumulate in the next CP.
            auto drained = pdev->drain_all_dirty_cache();
            ctx->m_total_chunks.fetch_add(drained.size(), std::memory_order_relaxed);

            LOGDEBUGMOD(s3, "S3 CP switchover: pdev_id={} drained {} dirty chunks",
                        pdev->pdev_id(), drained.size());

            ctx->m_dirty_pdevs.push_back(pdev);
            ctx->m_drained_data.push_back(std::move(drained));
        }
    }

    m_total_chunks_this_cp.store(ctx->m_total_chunks.load(), std::memory_order_relaxed);
    m_flushed_chunks_this_cp.store(0, std::memory_order_relaxed);

    LOGDEBUGMOD(s3, "S3 CP switchover: {} pdevs with dirty data, {} total dirty chunks",
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

    // Chain pdev flushes sequentially via folly::Future.
    // Each flush_pdev returns Future<bool>; we chain them so they execute
    // one after another (S3 uploads are already async internally).
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
                LOGERRORMOD(s3, "S3 CP flush completed with errors for cp_id={} (elapsed={}us)",
                            cp_id, elapsed_us);
            } else {
                LOGDEBUGMOD(s3, "S3 CP flush completed: cp_id={} elapsed={}us chunks_flushed={}",
                            cp_id, elapsed_us, ctx->m_flushed_chunks.load());
            }

            ctx->complete(all_success);
            return all_success;
        });
}

void S3CpCallbacks::cp_cleanup(CP* /* cp */) {
    // Nothing to clean up — dirty cache was drained at switchover time.
    // The S3CpContext will be destroyed when the CP is destroyed.
}

int S3CpCallbacks::cp_progress_percent() {
    auto total = m_total_chunks_this_cp.load(std::memory_order_relaxed);
    if (total == 0) return 100;
    auto flushed = m_flushed_chunks_this_cp.load(std::memory_order_relaxed);
    return static_cast< int >((flushed * 100) / total);
}

void S3CpCallbacks::trigger_early_cp_flush() {
    LOGWARNMOD(s3, "S3 dirty cache threshold exceeded — triggering early CP flush");
    COUNTER_INCREMENT(m_metrics, s3_cp_early_triggers, 1);

    // Trigger a CP flush via the global CPManager.
    // force=true so it queues even if another CP is in progress.
    cp_mgr().trigger_cp_flush(true /* force */);
}

folly::Future< bool > S3CpCallbacks::flush_pdev(S3PhysicalDev* pdev,
                                                  std::map< chunk_id_t, std::vector< DirtyBlock > >& drained,
                                                  S3CpContext* ctx) {
    LOGDEBUGMOD(s3, "S3 CP flushing pdev_id={} ({} chunks)", pdev->pdev_id(), drained.size());

    if (drained.empty()) {
        LOGDEBUGMOD(s3, "S3 CP: pdev_id={} had no dirty data", pdev->pdev_id());
        return folly::makeFuture< bool >(true);
    }

    auto* chunk_store = pdev->chunk_store();

    // Sort chunks by flush order: DATA → WAL → INDEX → METABLK
    auto ordered_chunks = sort_chunks_by_flush_order(drained, pdev->superblock());

    // Chain chunk puts sequentially via Future
    folly::Future< bool > chain = folly::makeFuture< bool >(true);
    uint64_t total_bytes = 0;

    for (auto chunk_id : ordered_chunks) {
        auto it = drained.find(chunk_id);
        if (it == drained.end()) continue;

        auto& dirty_blocks = it->second;
        auto* entry = pdev->superblock().find_chunk(chunk_id);
        if (!entry) {
            LOGWARNMOD(s3, "S3 CP: chunk_id={} not in superblock — skipping", chunk_id);
            continue;
        }

        uint64_t bytes_in_chunk = 0;
        for (const auto& db : dirty_blocks) {
            if (db.data) { bytes_in_chunk += db.data->size(); }
        }
        total_bytes += bytes_in_chunk;

        chain = std::move(chain).thenValue(
            [this, chunk_store, chunk_id, &dirty_blocks, chunk_size = entry->chunk_size,
             bytes_in_chunk, ctx, pdev](bool prev_ok) mutable -> bool {
                auto chunk_start = std::chrono::steady_clock::now();

                LOGTRACEMOD(s3, "S3 CP: flushing chunk_id={} ({} dirty blocks, {} bytes)",
                            chunk_id, dirty_blocks.size(), bytes_in_chunk);

                // ChunkStore::put() is synchronous (returns S3Result).
                // It internally does async S3 upload + .get().
                auto result = chunk_store->put(chunk_id, dirty_blocks, chunk_size);

                auto chunk_elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
                    std::chrono::steady_clock::now() - chunk_start).count();
                HISTOGRAM_OBSERVE(m_metrics, s3_cp_chunk_put_latency_us, chunk_elapsed_us);

                if (!result.ok()) {
                    LOGERRORMOD(s3, "S3 CP: chunk_store->put failed for chunk_id={}: {}",
                                chunk_id, result.error_message);
                    // Continue with remaining chunks (best effort) but mark failure
                    return false;
                }

                ctx->m_flushed_chunks.fetch_add(1, std::memory_order_relaxed);
                m_flushed_chunks_this_cp.fetch_add(1, std::memory_order_relaxed);

                return prev_ok; // propagate previous failure
            });
    }

    // After all chunks: write superblock if no errors
    return std::move(chain).thenValue(
        [this, pdev, total_bytes, num_chunks = ordered_chunks.size()](bool all_chunks_ok) -> bool {
            COUNTER_INCREMENT(m_metrics, s3_cp_chunks_flushed, num_chunks);
            COUNTER_INCREMENT(m_metrics, s3_cp_bytes_flushed, total_bytes);

            if (!all_chunks_ok) {
                // Don't write superblock — previous generation stays valid on S3.
                // Failed data must be re-written by the application for the next CP.
                LOGERRORMOD(s3, "S3 CP: skipping superblock write for pdev_id={} due to chunk upload errors",
                            pdev->pdev_id());
                return false;
            }

            // Write the pdev_s3 superblock — this is the S3 commit point.
            // Written LAST after all chunk data is successfully uploaded.
            // If crash happens before this write, recovery uses the previous generation.
            LOGDEBUGMOD(s3, "S3 CP: writing superblock for pdev_id={} (commit point)", pdev->pdev_id());
            auto sb_result = pdev->write_superblock();
            if (!sb_result.ok()) {
                LOGERRORMOD(s3, "S3 CP: superblock write failed for pdev_id={}: {}",
                            pdev->pdev_id(), sb_result.error_message);
                return false;
            }
            COUNTER_INCREMENT(m_metrics, s3_cp_superblock_writes, 1);

            LOGDEBUGMOD(s3, "S3 CP: pdev_id={} flush complete — {} chunks, {} bytes, superblock gen={}",
                        pdev->pdev_id(), num_chunks, total_bytes,
                        pdev->superblock().generation());
            return true;
        });
}

std::vector< chunk_id_t > S3CpCallbacks::sort_chunks_by_flush_order(
    const std::map< chunk_id_t, std::vector< DirtyBlock > >& drained,
    const PdevS3Superblock& superblock) const {

    // Flush order: DATA(0) → WAL(2) → INDEX(1) → METABLK(3)
    // This matches recovery priority (MetaBlk downloaded first during recovery,
    // but uploaded last during CP so it's the freshest commit).
    static constexpr std::array< S3ChunkType, 4 > FLUSH_ORDER = {
        S3ChunkType::DATA,
        S3ChunkType::WAL,
        S3ChunkType::INDEX,
        S3ChunkType::METABLK,
    };

    std::vector< chunk_id_t > result;
    result.reserve(drained.size());

    // Group chunks by type, then output in flush order
    for (auto type : FLUSH_ORDER) {
        for (auto& [chunk_id, _] : drained) {
            auto* entry = superblock.find_chunk(chunk_id);
            if (entry && entry->chunk_type == type) {
                result.push_back(chunk_id);
            }
        }
    }

    // Any chunks not found in superblock (shouldn't happen, but defensive)
    for (auto& [chunk_id, _] : drained) {
        if (std::find(result.begin(), result.end(), chunk_id) == result.end()) {
            LOGWARNMOD(s3, "S3 CP: chunk_id={} not in superblock during flush ordering", chunk_id);
            result.push_back(chunk_id);
        }
    }

    return result;
}

} // namespace homestore
