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

#include <chrono>

#include <homestore/s3/s3_cp.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

S3CpCallbacks::S3CpCallbacks(std::vector< S3PhysicalDev* > s3_pdevs)
    : m_s3_pdevs{std::move(s3_pdevs)} {
    LOGDEBUGMOD(s3, "S3CpCallbacks created with {} S3 pdevs", m_s3_pdevs.size());
}

std::unique_ptr< CPContext > S3CpCallbacks::on_switchover_cp(CP* cur_cp, CP* new_cp) {
    auto ctx = std::make_unique< S3CpContext >(new_cp);

    // Snapshot which pdevs have dirty data right now. During the switchover,
    // new writes will go to the new CP's dirty cache (via S3PhysicalDev::write),
    // while we flush the current CP's data.
    for (auto* pdev : m_s3_pdevs) {
        auto dirty_chunks = pdev->get_dirty_chunks();
        if (!dirty_chunks.empty()) {
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

folly::Future< bool > S3CpCallbacks::cp_flush(CP* cp) {
    auto* ctx = static_cast< S3CpContext* >(cp->context(cp_consumer_t::S3_SVC));
    if (!ctx || ctx->m_dirty_pdevs.empty()) {
        LOGDEBUGMOD(s3, "S3 CP flush: nothing to flush for cp_id={}", cp->id());
        ctx->complete(true);
        return folly::makeFuture< bool >(true);
    }

    auto start_time = std::chrono::steady_clock::now();
    COUNTER_INCREMENT(m_metrics, s3_cp_flush_count, 1);

    LOGDEBUGMOD(s3, "S3 CP flush starting: cp_id={} pdevs={} chunks={}",
                cp->id(), ctx->m_dirty_pdevs.size(), ctx->m_total_chunks.load());

    bool all_success = true;
    for (auto* pdev : ctx->m_dirty_pdevs) {
        if (!flush_pdev(pdev, ctx)) {
            all_success = false;
            LOGERRORMOD(s3, "S3 CP flush failed for pdev_id={} in cp_id={}",
                        pdev->pdev_id(), cp->id());
        }
    }

    auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
        std::chrono::steady_clock::now() - start_time).count();
    HISTOGRAM_OBSERVE(m_metrics, s3_cp_flush_latency_us, elapsed_us);

    if (!all_success) {
        COUNTER_INCREMENT(m_metrics, s3_cp_flush_errors, 1);
        LOGERRORMOD(s3, "S3 CP flush completed with errors for cp_id={} (elapsed={}us)",
                    cp->id(), elapsed_us);
    } else {
        LOGDEBUGMOD(s3, "S3 CP flush completed: cp_id={} elapsed={}us chunks_flushed={}",
                    cp->id(), elapsed_us, ctx->m_flushed_chunks.load());
    }

    ctx->complete(all_success);
    return folly::makeFuture< bool >(all_success);
}

void S3CpCallbacks::cp_cleanup(CP* /* cp */) {
    // Nothing to clean up — dirty cache was already drained during flush.
    // The S3CpContext will be destroyed when the CP is destroyed.
}

int S3CpCallbacks::cp_progress_percent() {
    auto total = m_total_chunks_this_cp.load(std::memory_order_relaxed);
    if (total == 0) return 100;
    auto flushed = m_flushed_chunks_this_cp.load(std::memory_order_relaxed);
    return static_cast< int >((flushed * 100) / total);
}

bool S3CpCallbacks::flush_pdev(S3PhysicalDev* pdev, S3CpContext* ctx) {
    LOGDEBUGMOD(s3, "S3 CP flushing pdev_id={}", pdev->pdev_id());

    // Step 1: Drain all dirty blocks from this pdev.
    // This atomically moves the dirty cache out — new writes during flush
    // go to a fresh cache (which will be flushed in the next CP).
    auto all_dirty = pdev->drain_all_dirty_cache();
    if (all_dirty.empty()) {
        LOGDEBUGMOD(s3, "S3 CP: pdev_id={} had no dirty data (race with drain)", pdev->pdev_id());
        return true;
    }

    auto* chunk_store = pdev->chunk_store();
    bool has_error = false;
    uint64_t total_bytes_flushed = 0;

    // Step 2: For each dirty chunk, upload to S3 via ChunkStore::put().
    // NOTE: put() blocks via .get() on the S3 future. This is intentional
    // for v1. v2 should pipeline puts across chunks for better throughput.
    for (auto& [chunk_id, dirty_blocks] : all_dirty) {
        auto chunk_start = std::chrono::steady_clock::now();

        // Get the chunk size from the superblock entry
        auto* entry = pdev->superblock().find_chunk(chunk_id);
        if (!entry) {
            LOGWARNMOD(s3, "S3 CP: chunk_id={} not in superblock — skipping", chunk_id);
            continue;
        }

        uint64_t bytes_in_chunk = 0;
        for (const auto& db : dirty_blocks) {
            if (db.data) { bytes_in_chunk += db.data->size(); }
        }

        LOGTRACEMOD(s3, "S3 CP: flushing chunk_id={} with {} dirty blocks ({} bytes)",
                    chunk_id, dirty_blocks.size(), bytes_in_chunk);

        auto result = chunk_store->put(chunk_id, dirty_blocks, entry->chunk_size);
        if (!result.ok()) {
            LOGERRORMOD(s3, "S3 CP: chunk_store->put failed for chunk_id={}: {}",
                        chunk_id, result.error_message);
            has_error = true;
            // Continue flushing other chunks — best effort
            continue;
        }

        auto chunk_elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
            std::chrono::steady_clock::now() - chunk_start).count();
        HISTOGRAM_OBSERVE(m_metrics, s3_cp_chunk_put_latency_us, chunk_elapsed_us);

        total_bytes_flushed += bytes_in_chunk;
        ctx->m_flushed_chunks.fetch_add(1, std::memory_order_relaxed);
        m_flushed_chunks_this_cp.fetch_add(1, std::memory_order_relaxed);

        // Update the chunk's generation in the superblock
        pdev->superblock_mutable().update_chunk_key(chunk_id, entry->get_s3_key(),
                                                     entry->generation + 1);
    }

    COUNTER_INCREMENT(m_metrics, s3_cp_chunks_flushed, ctx->m_flushed_chunks.load());
    COUNTER_INCREMENT(m_metrics, s3_cp_bytes_flushed, total_bytes_flushed);

    if (has_error) {
        // Don't write superblock if any chunk upload failed — previous
        // generation remains valid on S3. Dirty data that failed will
        // be retried on the next CP (caller must re-write).
        LOGERRORMOD(s3, "S3 CP: skipping superblock write for pdev_id={} due to chunk upload errors",
                    pdev->pdev_id());
        return false;
    }

    // Step 3: Write the pdev_s3 superblock — this is the S3 commit point.
    // Written LAST after all chunk data is successfully uploaded.
    // If crash happens before this, recovery uses previous generation.
    LOGDEBUGMOD(s3, "S3 CP: writing superblock for pdev_id={} (commit point)", pdev->pdev_id());
    auto sb_result = pdev->write_superblock();
    if (!sb_result.ok()) {
        LOGERRORMOD(s3, "S3 CP: superblock write failed for pdev_id={}: {}",
                    pdev->pdev_id(), sb_result.error_message);
        return false;
    }
    COUNTER_INCREMENT(m_metrics, s3_cp_superblock_writes, 1);

    LOGDEBUGMOD(s3, "S3 CP: pdev_id={} flush complete — {} chunks, {} bytes, superblock gen={}",
                pdev->pdev_id(), all_dirty.size(), total_bytes_flushed,
                pdev->superblock().generation());
    return true;
}

} // namespace homestore
