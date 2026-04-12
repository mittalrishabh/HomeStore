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
#include <sisl/logging/logging.h>

#include <homestore/s3/s3_cp_handler.h>

namespace homestore {

static auto get_current_time() { return std::chrono::steady_clock::now(); }

static uint64_t elapsed_us(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast< std::chrono::microseconds >(
               std::chrono::steady_clock::now() - start)
        .count();
}

// ─── Construction ────────────────────────────────────────────────────────────

S3CpHandler::S3CpHandler(std::shared_ptr< S3PhysicalDev > s3_pdev, uint32_t upload_concurrency)
    : m_s3_pdev{std::move(s3_pdev)}, m_upload_concurrency{upload_concurrency} {
    RELEASE_ASSERT(m_s3_pdev != nullptr, "S3PhysicalDev must not be null");
    if (m_upload_concurrency == 0) { m_upload_concurrency = 1; }
}

// ─── CP Switchover ───────────────────────────────────────────────────────────

std::unique_ptr< CPContext > S3CpHandler::on_switchover_cp(CP* cur_cp, CP* new_cp) {
    auto ctx = std::make_unique< S3CpContext >(new_cp);

    if (cur_cp == nullptr) {
        // First CP at startup — no dirty data yet.
        return ctx;
    }

    // Drain the entire dirty cache atomically at switchover time.
    // This captures a consistent snapshot of all dirty blocks accumulated
    // during the previous CP period. New writes after this point go into
    // the new CP's dirty cache.
    auto dirty_map = m_s3_pdev->drain_all_dirty_cache();

    // Also include any chunks that failed in the previous CP — they need
    // to be retried. For failed chunks, we don't have the dirty blocks
    // anymore (they were drained last time), so we add them with empty
    // dirty_blocks. ChunkStore::put() with empty dirty_blocks for
    // FullChunkStore will re-read from NVMe and re-upload.
    {
        std::lock_guard< std::mutex > lg(m_failed_mutex);
        for (auto chunk_id : m_failed_chunks) {
            if (dirty_map.find(chunk_id) == dirty_map.end()) {
                dirty_map[chunk_id] = {};  // Empty dirty_blocks — will trigger full re-upload
                LOGINFO("Retrying failed chunk {} from previous CP", chunk_id);
            }
        }
        m_failed_chunks.clear();
    }

    for (auto& [chunk_id, blocks] : dirty_map) {
        ctx->dirty_chunk_ids.push_back(chunk_id);
    }
    ctx->dirty_blocks = std::move(dirty_map);

    LOGINFO("S3 CP switchover: {} dirty chunks to flush", ctx->dirty_chunk_ids.size());
    return ctx;
}

// ─── Single chunk upload ─────────────────────────────────────────────────────

S3Result S3CpHandler::upload_chunk(chunk_id_t chunk_id, const std::vector< DirtyBlock >& dirty_blocks) {
    auto* cs = m_s3_pdev->chunk_store();
    if (cs == nullptr) {
        return S3Result{.status_code = 500, .error_message = "ChunkStore is null"};
    }

    // Look up chunk size from the superblock
    auto* entry = m_s3_pdev->superblock().find_chunk(chunk_id);
    uint64_t chunk_size = entry ? entry->chunk_size : 0;

    auto start = get_current_time();
    auto result = cs->put(chunk_id, dirty_blocks, chunk_size);
    auto latency = elapsed_us(start);

    HISTOGRAM_OBSERVE(m_metrics, s3_cp_chunk_upload_latency, latency);

    if (result.ok()) {
        // Calculate bytes uploaded (approximate from dirty blocks)
        uint64_t bytes = 0;
        for (auto& blk : dirty_blocks) {
            if (blk.data) { bytes += blk.data->size(); }
        }
        // If dirty_blocks is empty (retry case), the full chunk was uploaded
        if (dirty_blocks.empty() && chunk_size > 0) { bytes = chunk_size; }
        COUNTER_INCREMENT(m_metrics, s3_cp_bytes_uploaded, bytes);
        COUNTER_INCREMENT(m_metrics, s3_cp_chunks_uploaded, 1);
        LOGDEBUG("Uploaded chunk {} to S3 ({} dirty blocks, {} us)", chunk_id, dirty_blocks.size(), latency);
    } else {
        COUNTER_INCREMENT(m_metrics, s3_cp_chunks_failed, 1);
        LOGERROR("Failed to upload chunk {} to S3: status={}, error={}", chunk_id, result.status_code,
                 result.error_message);
    }

    return result;
}

// ─── Ordered flush ───────────────────────────────────────────────────────────

void S3CpHandler::flush_chunks_ordered(S3CpContext* ctx) {
    // S3 flush ordering (matches NVMe CP order):
    //   a. Data chunks
    //   b. WAL chunks
    //   c. Index chunks
    //   d. MetaBlk chunks
    //   e. pdev_s3 superblock (commit point) — done after this method returns
    //
    // Within each category, upload up to m_upload_concurrency chunks in parallel.

    const auto& sb = m_s3_pdev->superblock();
    const S3ChunkType flush_order[] = {
        S3ChunkType::DATA, S3ChunkType::WAL, S3ChunkType::INDEX, S3ChunkType::METABLK};

    std::set< chunk_id_t > failed_this_cp;

    for (auto type : flush_order) {
        // Collect chunks of this type that are dirty
        std::vector< chunk_id_t > chunks_of_type;
        for (auto cid : ctx->dirty_chunk_ids) {
            auto* entry = sb.find_chunk(cid);
            if (entry && entry->chunk_type == type) {
                chunks_of_type.push_back(cid);
            } else if (!entry) {
                // Chunk not in superblock — might be newly created.
                // Default to DATA type ordering.
                if (type == S3ChunkType::DATA) { chunks_of_type.push_back(cid); }
            }
        }

        if (chunks_of_type.empty()) { continue; }

        LOGINFO("S3 CP: flushing {} {} chunks", chunks_of_type.size(),
                type == S3ChunkType::DATA      ? "DATA"
                : type == S3ChunkType::WAL     ? "WAL"
                : type == S3ChunkType::INDEX   ? "INDEX"
                                               : "METABLK");

        // Upload in batches of m_upload_concurrency
        for (size_t i = 0; i < chunks_of_type.size(); i += m_upload_concurrency) {
            size_t batch_end = std::min(i + m_upload_concurrency, chunks_of_type.size());
            std::vector< folly::Future< std::pair< chunk_id_t, S3Result > > > futs;

            for (size_t j = i; j < batch_end; ++j) {
                auto cid = chunks_of_type[j];
                auto& blocks = ctx->dirty_blocks[cid];

                futs.emplace_back(
                    folly::via(folly::getGlobalCPUExecutor().get(),
                               [this, cid, blocks_copy = blocks]() mutable
                                   -> std::pair< chunk_id_t, S3Result > {
                                   return {cid, upload_chunk(cid, blocks_copy)};
                               }));
            }

            // Wait for this batch to complete
            auto results = folly::collectAll(futs).get();
            for (auto& try_result : results) {
                if (try_result.hasValue()) {
                    auto& [cid, s3_result] = try_result.value();
                    if (s3_result.ok()) {
                        m_chunks_flushed.fetch_add(1);
                    } else {
                        failed_this_cp.insert(cid);
                    }
                } else {
                    LOGERROR("S3 CP: chunk upload threw exception");
                }

                // Update progress
                if (m_total_chunks_to_flush.load() > 0) {
                    m_progress_pct.store(
                        static_cast< int >(m_chunks_flushed.load() * 100 / m_total_chunks_to_flush.load()));
                }
            }
        }
    }

    // Record failed chunks for retry in next CP
    if (!failed_this_cp.empty()) {
        std::lock_guard< std::mutex > lg(m_failed_mutex);
        m_failed_chunks = std::move(failed_this_cp);
        LOGWARN("S3 CP: {} chunks failed upload, will retry next CP", m_failed_chunks.size());
    }
}

// ─── CP Flush ────────────────────────────────────────────────────────────────

folly::Future< bool > S3CpHandler::cp_flush(CP* cp) {
    auto* ctx = dynamic_cast< S3CpContext* >(cp->context(cp_consumer_t::HS_CLIENT));
    if (ctx == nullptr || ctx->dirty_chunk_ids.empty()) {
        LOGINFO("S3 CP {}: nothing to flush", cp->id());
        return folly::makeFuture(true);
    }

    auto flush_start = get_current_time();
    COUNTER_INCREMENT(m_metrics, s3_cp_flush_count, 1);

    m_total_chunks_to_flush.store(static_cast< uint32_t >(ctx->dirty_chunk_ids.size()));
    m_chunks_flushed.store(0);
    m_progress_pct.store(0);

    LOGINFO("S3 CP {}: starting flush of {} dirty chunks (concurrency={})",
            cp->id(), ctx->dirty_chunk_ids.size(), m_upload_concurrency);

    // Phase 1: Upload all dirty chunks in type order
    flush_chunks_ordered(ctx);

    // Phase 2: Update pdev_s3 superblock on S3 (commit point — written last)
    // Only write the superblock if at least some chunks uploaded successfully.
    auto chunks_ok = m_chunks_flushed.load();
    if (chunks_ok > 0) {
        m_s3_pdev->superblock_mutable().increment_generation();
        auto sb_result = m_s3_pdev->write_superblock();
        if (sb_result.ok()) {
            COUNTER_INCREMENT(m_metrics, s3_cp_superblock_writes, 1);
            LOGINFO("S3 CP {}: superblock written (gen={})", cp->id(),
                    m_s3_pdev->superblock().generation());
        } else {
            COUNTER_INCREMENT(m_metrics, s3_cp_superblock_failures, 1);
            LOGERROR("S3 CP {}: superblock write FAILED: {}", cp->id(), sb_result.error_message);
            // Superblock failure is serious but not fatal. The previous generation
            // superblock is still valid. Next CP will try again.
        }
    }

    auto total_latency = elapsed_us(flush_start);
    HISTOGRAM_OBSERVE(m_metrics, s3_cp_flush_latency, total_latency);

    auto total = m_total_chunks_to_flush.load();
    auto failed_count = total - chunks_ok;
    LOGINFO("S3 CP {}: flush complete — {}/{} chunks uploaded, {} failed, {} us",
            cp->id(), chunks_ok, total, failed_count, total_latency);

    m_progress_pct.store(100);

    // S3 flush failure does NOT fail the CP. NVMe is already durable.
    // S3 is best-effort until it catches up.
    return folly::makeFuture(true);
}

// ─── CP Cleanup ──────────────────────────────────────────────────────────────

void S3CpHandler::cp_cleanup(CP* /*cp*/) {
    m_progress_pct.store(0);
    m_total_chunks_to_flush.store(0);
    m_chunks_flushed.store(0);
}

// ─── Progress ────────────────────────────────────────────────────────────────

int S3CpHandler::cp_progress_percent() {
    return m_progress_pct.load();
}

// ─── Failed chunks accessor ─────────────────────────────────────────────────

std::set< chunk_id_t > S3CpHandler::failed_chunks() const {
    std::lock_guard< std::mutex > lg(m_failed_mutex);
    return m_failed_chunks;
}

} // namespace homestore
