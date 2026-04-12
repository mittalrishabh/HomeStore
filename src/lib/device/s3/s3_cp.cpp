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
#include <set>

#include <folly/executors/GlobalExecutor.h>
#include <folly/futures/Future.h>

#include <homestore/s3/s3_cp.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

static auto now() { return std::chrono::steady_clock::now(); }

static uint64_t elapsed_us(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast< std::chrono::microseconds >(now() - start).count();
}

// ─── Construction ────────────────────────────────────────────────────────────

S3CpCallbacks::S3CpCallbacks(std::vector< S3PhysicalDev* > s3_pdevs, uint32_t upload_concurrency)
    : m_s3_pdevs{std::move(s3_pdevs)}
    , m_upload_concurrency{upload_concurrency < 1 ? 1u : upload_concurrency} {
    LOGDEBUGMOD(s3, "S3CpCallbacks created with {} S3 pdevs, upload_concurrency={}",
                m_s3_pdevs.size(), m_upload_concurrency);
}

// ─── Switchover ──────────────────────────────────────────────────────────────

std::unique_ptr< CPContext > S3CpCallbacks::on_switchover_cp(CP* cur_cp, CP* new_cp) {
    auto ctx = std::make_unique< S3CpContext >(new_cp);

    // Snapshot which pdevs have dirty data right now.
    for (auto* pdev : m_s3_pdevs) {
        auto dirty_chunks = pdev->get_dirty_chunks();
        bool has_retries = false;

        // Check for failed chunks from previous CP that need retry
        {
            std::lock_guard< std::mutex > lg(m_failed_mutex);
            auto it = m_failed_chunks.find(pdev->pdev_id());
            if (it != m_failed_chunks.end() && !it->second.empty()) {
                has_retries = true;
                ctx->m_retry_chunks[pdev->pdev_id()] = it->second;
                LOGINFO("S3 CP switchover: pdev_id={} has {} chunks to retry from previous CP",
                        pdev->pdev_id(), it->second.size());
                COUNTER_INCREMENT(m_metrics, s3_cp_retry_chunks, it->second.size());
                it->second.clear();
            }
        }

        if (!dirty_chunks.empty() || has_retries) {
            ctx->m_dirty_pdevs.push_back(pdev);
            ctx->m_total_chunks.fetch_add(dirty_chunks.size(), std::memory_order_relaxed);
            LOGDEBUGMOD(s3, "S3 CP switchover: pdev_id={} has {} dirty chunks",
                        pdev->pdev_id(), dirty_chunks.size());
        }
    }

    m_total_chunks_this_cp.store(ctx->m_total_chunks.load(), std::memory_order_relaxed);
    m_flushed_chunks_this_cp.store(0, std::memory_order_relaxed);

    LOGDEBUGMOD(s3, "S3 CP switchover: {} pdevs with dirty data, {} total dirty chunks",
                ctx->m_dirty_pdevs.size(), ctx->m_total_chunks.load());

    return ctx;
}

// ─── Sort by flush order ─────────────────────────────────────────────────────

std::vector< std::pair< chunk_id_t, S3ChunkType > >
S3CpCallbacks::sort_by_flush_order(const PdevS3Superblock& sb,
                                   const std::map< chunk_id_t, std::vector< DirtyBlock > >& dirty_map) {
    // Flush order: DATA → WAL → INDEX → METABLK
    static constexpr S3ChunkType order[] = {
        S3ChunkType::DATA, S3ChunkType::WAL, S3ChunkType::INDEX, S3ChunkType::METABLK};

    std::vector< std::pair< chunk_id_t, S3ChunkType > > sorted;
    sorted.reserve(dirty_map.size());

    std::set< chunk_id_t > added;

    for (auto type : order) {
        for (auto& [cid, _] : dirty_map) {
            if (added.count(cid)) continue;
            auto* entry = sb.find_chunk(cid);
            if (entry && entry->chunk_type == type) {
                sorted.emplace_back(cid, type);
                added.insert(cid);
            }
        }
    }

    // Any chunks not in the superblock (newly created) — flush as DATA
    for (auto& [cid, _] : dirty_map) {
        if (!added.count(cid)) {
            sorted.emplace_back(cid, S3ChunkType::DATA);
        }
    }

    return sorted;
}

// ─── Single chunk upload ─────────────────────────────────────────────────────

std::pair< chunk_id_t, S3Result >
S3CpCallbacks::upload_one_chunk(S3PhysicalDev* pdev, chunk_id_t chunk_id,
                                const std::vector< DirtyBlock >& dirty_blocks) {
    auto* chunk_store = pdev->chunk_store();
    if (!chunk_store) {
        return {chunk_id, S3Result{.status_code = 500, .error_message = "ChunkStore is null"}};
    }

    auto* entry = pdev->superblock().find_chunk(chunk_id);
    uint64_t chunk_size = entry ? entry->chunk_size : 0;

    auto start = now();
    auto result = chunk_store->put(chunk_id, dirty_blocks, chunk_size);
    HISTOGRAM_OBSERVE(m_metrics, s3_cp_chunk_put_latency_us, elapsed_us(start));

    if (result.ok()) {
        uint64_t bytes = 0;
        for (auto& blk : dirty_blocks) {
            if (blk.data) bytes += blk.data->size();
        }
        if (dirty_blocks.empty() && chunk_size > 0) bytes = chunk_size;
        COUNTER_INCREMENT(m_metrics, s3_cp_bytes_flushed, bytes);
    } else {
        COUNTER_INCREMENT(m_metrics, s3_cp_chunks_failed, 1);
    }

    return {chunk_id, result};
}

// ─── Flush one pdev ──────────────────────────────────────────────────────────

bool S3CpCallbacks::flush_pdev(S3PhysicalDev* pdev, S3CpContext* ctx) {
    LOGDEBUGMOD(s3, "S3 CP flushing pdev_id={}", pdev->pdev_id());

    // Step 1: Drain all dirty blocks from this pdev atomically.
    auto all_dirty = pdev->drain_all_dirty_cache();

    // Add retry chunks (with empty dirty_blocks — triggers full re-upload)
    auto retry_it = ctx->m_retry_chunks.find(pdev->pdev_id());
    if (retry_it != ctx->m_retry_chunks.end()) {
        for (auto cid : retry_it->second) {
            if (all_dirty.find(cid) == all_dirty.end()) {
                all_dirty[cid] = {};  // Empty → FullChunkStore re-reads from NVMe
                LOGINFO("S3 CP: retrying failed chunk {} for pdev_id={}", cid, pdev->pdev_id());
            }
        }
    }

    if (all_dirty.empty()) {
        LOGDEBUGMOD(s3, "S3 CP: pdev_id={} had no dirty data", pdev->pdev_id());
        return true;
    }

    // Step 2: Sort by flush order
    auto sorted = sort_by_flush_order(pdev->superblock(), all_dirty);

    // Step 3: Upload in parallel batches (bounded by m_upload_concurrency)
    std::set< chunk_id_t > failed_this_cp;
    uint64_t chunks_ok = 0;

    for (size_t i = 0; i < sorted.size(); i += m_upload_concurrency) {
        size_t batch_end = std::min(i + m_upload_concurrency, sorted.size());
        std::vector< folly::Future< std::pair< chunk_id_t, S3Result > > > futs;

        for (size_t j = i; j < batch_end; ++j) {
            auto cid = sorted[j].first;
            auto& blocks = all_dirty[cid];

            futs.emplace_back(
                folly::via(folly::getGlobalCPUExecutor().get(),
                           [this, pdev, cid, blocks_copy = blocks]() mutable {
                               return upload_one_chunk(pdev, cid, blocks_copy);
                           }));
        }

        auto results = folly::collectAll(futs).get();
        for (auto& try_result : results) {
            if (try_result.hasValue()) {
                auto& [cid, s3_result] = try_result.value();
                if (s3_result.ok()) {
                    ++chunks_ok;
                    ctx->m_flushed_chunks.fetch_add(1, std::memory_order_relaxed);
                    m_flushed_chunks_this_cp.fetch_add(1, std::memory_order_relaxed);

                    // Update chunk generation in superblock
                    auto* entry = pdev->superblock().find_chunk(cid);
                    if (entry) {
                        pdev->superblock_mutable().update_chunk_key(
                            cid, entry->get_s3_key(), entry->generation + 1);
                    }
                } else {
                    LOGERROR("S3 CP: failed to upload chunk {} for pdev_id={}: {}",
                             cid, pdev->pdev_id(), s3_result.error_message);
                    failed_this_cp.insert(cid);
                }
            } else {
                LOGERROR("S3 CP: chunk upload threw exception for pdev_id={}", pdev->pdev_id());
            }
        }
    }

    COUNTER_INCREMENT(m_metrics, s3_cp_chunks_flushed, chunks_ok);

    // Record failures for retry
    if (!failed_this_cp.empty()) {
        std::lock_guard< std::mutex > lg(m_failed_mutex);
        m_failed_chunks[pdev->pdev_id()] = std::move(failed_this_cp);
    }

    // Step 4: Write superblock — only if ALL chunks succeeded
    if (!failed_this_cp.empty() || chunks_ok == 0) {
        LOGERROR("S3 CP: skipping superblock for pdev_id={} ({} failed, {} ok)",
                 pdev->pdev_id(), failed_this_cp.size(), chunks_ok);
        COUNTER_INCREMENT(m_metrics, s3_cp_flush_errors, 1);
        return false;
    }

    pdev->superblock_mutable().increment_generation();
    auto sb_result = pdev->write_superblock();
    if (!sb_result.ok()) {
        LOGERROR("S3 CP: superblock write failed for pdev_id={}: {}",
                 pdev->pdev_id(), sb_result.error_message);
        COUNTER_INCREMENT(m_metrics, s3_cp_superblock_failures, 1);
        return false;
    }
    COUNTER_INCREMENT(m_metrics, s3_cp_superblock_writes, 1);

    LOGDEBUGMOD(s3, "S3 CP: pdev_id={} flush complete — {} chunks, superblock gen={}",
                pdev->pdev_id(), chunks_ok, pdev->superblock().generation());
    return true;
}

// ─── CP Flush ────────────────────────────────────────────────────────────────

folly::Future< bool > S3CpCallbacks::cp_flush(CP* cp) {
    auto* ctx = static_cast< S3CpContext* >(cp->context(cp_consumer_t::S3_SVC));
    if (!ctx || ctx->m_dirty_pdevs.empty()) {
        LOGDEBUGMOD(s3, "S3 CP flush: nothing to flush for cp_id={}", cp->id());
        if (ctx) ctx->complete(true);
        return folly::makeFuture< bool >(true);
    }

    auto start_time = now();
    COUNTER_INCREMENT(m_metrics, s3_cp_flush_count, 1);

    LOGINFO("S3 CP flush starting: cp_id={} pdevs={} chunks={}",
            cp->id(), ctx->m_dirty_pdevs.size(), ctx->m_total_chunks.load());

    bool all_success = true;
    for (auto* pdev : ctx->m_dirty_pdevs) {
        if (!flush_pdev(pdev, ctx)) {
            all_success = false;
        }
    }

    HISTOGRAM_OBSERVE(m_metrics, s3_cp_flush_latency_us, elapsed_us(start_time));

    if (!all_success) {
        LOGWARN("S3 CP flush completed with errors for cp_id={} — failed chunks will retry next CP",
                cp->id());
    } else {
        LOGINFO("S3 CP flush completed: cp_id={} elapsed={}us chunks_flushed={}",
                cp->id(), elapsed_us(start_time), ctx->m_flushed_chunks.load());
    }

    // S3 failure is NON-FATAL — NVMe is already durable
    ctx->complete(true);
    return folly::makeFuture< bool >(true);
}

// ─── Cleanup ─────────────────────────────────────────────────────────────────

void S3CpCallbacks::cp_cleanup(CP* /* cp */) {
    m_total_chunks_this_cp.store(0, std::memory_order_relaxed);
    m_flushed_chunks_this_cp.store(0, std::memory_order_relaxed);
}

// ─── Progress ────────────────────────────────────────────────────────────────

int S3CpCallbacks::cp_progress_percent() {
    auto total = m_total_chunks_this_cp.load(std::memory_order_relaxed);
    if (total == 0) return 100;
    auto flushed = m_flushed_chunks_this_cp.load(std::memory_order_relaxed);
    return static_cast< int >((flushed * 100) / total);
}

// ─── Failed chunks accessor ─────────────────────────────────────────────────

std::map< uint32_t, std::set< chunk_id_t > > S3CpCallbacks::failed_chunks() const {
    std::lock_guard< std::mutex > lg(m_failed_mutex);
    return m_failed_chunks;
}

} // namespace homestore
