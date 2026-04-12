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
#include <system_error>

#include <homestore/s3/s3_physical_dev.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

S3PhysicalDev::S3PhysicalDev(shared< S3ObjectStore > s3_store, uint64_t dirty_cache_max_mb)
    : m_s3_store{std::move(s3_store)}, m_dirty_cache_max_bytes{dirty_cache_max_mb * 1024 * 1024} {
    LOGINFO("S3PhysicalDev created: dirty_cache_max={}MB", dirty_cache_max_mb);
}

// ─── ChunkStore lifecycle ──────────────────────────────────────────────────

void S3PhysicalDev::set_chunk_store(shared< ChunkStore > cs) {
    m_chunk_store = std::move(cs);
    LOGINFO("S3PhysicalDev: ChunkStore attached");
}

void S3PhysicalDev::set_early_cp_flush_cb(early_cp_flush_cb_t cb) { m_early_cp_flush_cb = std::move(cb); }

// ─── Write path ────────────────────────────────────────────────────────────

folly::Future< std::error_code > S3PhysicalDev::write(chunk_id_t chunk_id, offset_t offset, sisl::byte_array data) {
    auto const start = std::chrono::steady_clock::now();
    auto const data_size = data->size();

    LOGDEBUG("S3PhysicalDev::write chunk_id={} offset={} size={}", chunk_id, offset, data_size);

    {
        std::lock_guard lg(m_dirty_mtx);
        DirtyBlock db;
        db.offset = offset;
        db.data = std::move(data);
        m_dirty_cache[chunk_id].push_back(std::move(db));
    }

    m_dirty_cache_bytes.fetch_add(data_size, std::memory_order_relaxed);
    COUNTER_INCREMENT(m_metrics, s3_pdev_writes, 1);

    auto const elapsed_us =
        std::chrono::duration_cast< std::chrono::microseconds >(std::chrono::steady_clock::now() - start).count();
    HISTOGRAM_OBSERVE(m_metrics, s3_pdev_write_latency_us, elapsed_us);

    // Check if we need to trigger early CP flush due to memory pressure.
    check_dirty_cache_pressure();

    return folly::makeFuture(std::error_code{});
}

// ─── Read path ─────────────────────────────────────────────────────────────

folly::Future< std::pair< std::error_code, sisl::byte_array > >
S3PhysicalDev::read(chunk_id_t chunk_id, offset_t offset, uint64_t size) {
    LOGDEBUG("S3PhysicalDev::read chunk_id={} offset={} size={}", chunk_id, offset, size);

    if (!m_chunk_store) {
        LOGERROR("S3PhysicalDev::read called but no ChunkStore is attached");
        return folly::makeFuture(std::make_pair(
            std::make_error_code(std::errc::not_connected), sisl::byte_array{}));
    }

    COUNTER_INCREMENT(m_metrics, s3_pdev_reads, 1);
    auto const start = std::chrono::steady_clock::now();

    // Delegate to ChunkStore::get() — handles v1 full-chunk or v3 SST transparently.
    // NOTE: ChunkStore::get() returns folly::Future<pair<S3Result, byte_array>>.
    //       We convert S3Result to std::error_code here.
    return m_chunk_store->get(chunk_id, offset, size)
        .thenValue([this, start, chunk_id](auto&& result) -> std::pair< std::error_code, sisl::byte_array > {
            auto const elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
                                        std::chrono::steady_clock::now() - start)
                                        .count();
            HISTOGRAM_OBSERVE(m_metrics, s3_pdev_read_latency_us, elapsed_us);

            auto& [s3_res, data] = result;
            if (!s3_res.ok()) {
                LOGERROR("S3PhysicalDev::read chunk_id={} failed: {}", chunk_id, s3_res.message);
                return {std::make_error_code(std::errc::io_error), sisl::byte_array{}};
            }
            return {std::error_code{}, std::move(data)};
        });
}

// ─── Chunk management ──────────────────────────────────────────────────────

void S3PhysicalDev::create_chunk(chunk_id_t chunk_id, uint64_t chunk_size) {
    std::lock_guard lg(m_chunk_mtx);
    m_registered_chunks[chunk_id] = chunk_size;
    COUNTER_INCREMENT(m_metrics, s3_pdev_chunks_registered, 1);
    LOGINFO("S3PhysicalDev: registered chunk_id={} size={}", chunk_id, chunk_size);
}

void S3PhysicalDev::remove_chunk(chunk_id_t chunk_id) {
    {
        std::lock_guard lg(m_dirty_mtx);
        auto it = m_dirty_cache.find(chunk_id);
        if (it != m_dirty_cache.end()) {
            // Reclaim dirty cache bytes for removed chunk
            for (const auto& db : it->second) {
                m_dirty_cache_bytes.fetch_sub(db.data->size(), std::memory_order_relaxed);
            }
            m_dirty_cache.erase(it);
        }
    }
    {
        std::lock_guard lg(m_chunk_mtx);
        m_registered_chunks.erase(chunk_id);
    }
    LOGINFO("S3PhysicalDev: removed chunk_id={}", chunk_id);
}

bool S3PhysicalDev::has_chunk(chunk_id_t chunk_id) const {
    std::lock_guard lg(m_chunk_mtx);
    return m_registered_chunks.count(chunk_id) > 0;
}

// ─── CP interface ──────────────────────────────────────────────────────────

std::vector< DirtyBlock > S3PhysicalDev::drain_dirty_cache(chunk_id_t chunk_id) {
    std::lock_guard lg(m_dirty_mtx);
    auto it = m_dirty_cache.find(chunk_id);
    if (it == m_dirty_cache.end()) { return {}; }

    auto blocks = std::move(it->second);
    m_dirty_cache.erase(it);

    // Update dirty cache byte counter
    uint64_t freed = 0;
    for (const auto& db : blocks) {
        freed += db.data->size();
    }
    m_dirty_cache_bytes.fetch_sub(freed, std::memory_order_relaxed);

    LOGDEBUG("S3PhysicalDev: drained {} dirty blocks ({} bytes) for chunk_id={}", blocks.size(), freed, chunk_id);
    return blocks;
}

std::unordered_set< chunk_id_t > S3PhysicalDev::get_dirty_chunk_ids() const {
    std::lock_guard lg(m_dirty_mtx);
    std::unordered_set< chunk_id_t > ids;
    for (const auto& [cid, blocks] : m_dirty_cache) {
        if (!blocks.empty()) { ids.insert(cid); }
    }
    return ids;
}

bool S3PhysicalDev::has_dirty_data() const {
    std::lock_guard lg(m_dirty_mtx);
    for (const auto& [cid, blocks] : m_dirty_cache) {
        if (!blocks.empty()) return true;
    }
    return false;
}

uint32_t S3PhysicalDev::num_chunks() const {
    std::lock_guard lg(m_chunk_mtx);
    return static_cast< uint32_t >(m_registered_chunks.size());
}

// ─── Internal ──────────────────────────────────────────────────────────────

void S3PhysicalDev::check_dirty_cache_pressure() {
    auto const current = m_dirty_cache_bytes.load(std::memory_order_relaxed);
    if (current > m_dirty_cache_max_bytes && m_early_cp_flush_cb) {
        LOGWARN("S3PhysicalDev: dirty cache {}MB exceeds limit {}MB — triggering early CP flush",
                current / (1024 * 1024), m_dirty_cache_max_bytes / (1024 * 1024));
        COUNTER_INCREMENT(m_metrics, s3_pdev_early_cp_flushes, 1);
        m_early_cp_flush_cb();
    }
}

} // namespace homestore
