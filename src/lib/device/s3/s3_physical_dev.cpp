/*********************************************************************************
 * Copyright 2024 eBay Inc.
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

#include <homestore/s3/s3_physical_dev.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

S3PhysicalDev::S3PhysicalDev(uint32_t pdev_id,
                             std::shared_ptr< ChunkStore > chunk_store,
                             std::shared_ptr< S3ObjectStore > s3_store,
                             const std::string& volume_id,
                             uint64_t dirty_cache_max_mb,
                             std::shared_ptr< S3CpFlushCallback > cp_flush_cb)
    : m_pdev_id{pdev_id},
      m_chunk_store{std::move(chunk_store)},
      m_s3_store{std::move(s3_store)},
      m_volume_id{volume_id},
      m_dirty_cache_max_bytes{dirty_cache_max_mb * 1024ULL * 1024ULL},
      m_cp_flush_cb{std::move(cp_flush_cb)},
      m_metrics{fmt::format("s3_pdev_{}", pdev_id)} {

    m_superblock.set_pdev_id(pdev_id);
    LOGDEBUGMOD(s3, "S3PhysicalDev created: pdev_id={} volume_id={} dirty_cache_max={}MB",
                pdev_id, volume_id, dirty_cache_max_mb);
}

///////////////////////// Write/Read Operations /////////////////////////

void S3PhysicalDev::write(chunk_id_t chunk_id, offset_t offset, const sisl::byte_array& data) {
    // Validate chunk exists in superblock — writes to unknown chunks would be
    // orphaned at drain time (no superblock entry = no recovery).
    DEBUG_ASSERT(has_chunk(chunk_id), "write() to chunk_id={} that was never create_chunk()'d", chunk_id);
    if (!has_chunk(chunk_id)) {
        LOGWARNMOD(s3, "S3PhysicalDev::write() to unknown chunk_id={} — ignoring (no superblock entry)", chunk_id);
        return;
    }

    // This is ~instant: just a map insert + shared_ptr bump (no data copy).
    // The sisl::byte_array is a shared_ptr<io_blob_safe>, so "caching" is
    // incrementing the refcount, not copying bytes.

    DirtyBlock db;
    db.offset = offset;
    db.data = data; // shared_ptr bump, not a copy

    uint64_t data_size = data ? data->size() : 0;

    {
        std::lock_guard< std::mutex > lock{m_dirty_cache_mutex};
        m_dirty_cache[chunk_id].push_back(std::move(db));
    }

    m_dirty_cache_bytes.fetch_add(data_size, std::memory_order_relaxed);

    COUNTER_INCREMENT(m_metrics, s3_write_count, 1);
    COUNTER_INCREMENT(m_metrics, s3_dirty_cache_inserts, 1);
    COUNTER_SET(m_metrics, s3_dirty_cache_bytes, m_dirty_cache_bytes.load());

    LOGTRACEMOD(s3, "S3PhysicalDev::write chunk_id={} offset={} size={} dirty_cache_total={}",
                chunk_id, offset, data_size, m_dirty_cache_bytes.load());

    // Check if dirty cache exceeds threshold
    check_dirty_cache_threshold();
}

folly::Future< std::pair< S3Result, sisl::byte_array > >
S3PhysicalDev::read(chunk_id_t chunk_id, offset_t offset, uint64_t size) {
    LOGTRACEMOD(s3, "S3PhysicalDev::read chunk_id={} offset={} size={}", chunk_id, offset, size);
    COUNTER_INCREMENT(m_metrics, s3_read_count, 1);

    // Read goes through ChunkStore::get() which handles v1 full-chunk
    // and v3 SST transparently
    return m_chunk_store->get(chunk_id, offset, size);
}

///////////////////////// Chunk Management /////////////////////////

void S3PhysicalDev::create_chunk(chunk_id_t chunk_id, uint64_t chunk_size,
                                 S3ChunkType chunk_type, uint64_t vdev_id) {
    LOGDEBUGMOD(s3, "S3PhysicalDev::create_chunk chunk_id={} size={} type={} vdev_id={}",
                chunk_id, chunk_size, static_cast< int >(chunk_type), vdev_id);

    // Add to superblock
    s3_chunk_entry entry;
    entry.chunk_id = chunk_id;
    entry.chunk_size = chunk_size;
    entry.chunk_type = chunk_type;
    entry.vdev_id = vdev_id;
    entry.generation = 0;

    // Set the S3 key based on the key mapper pattern
    auto key = m_volume_id + "/chunks/" + std::to_string(chunk_id) + "/data.dat";
    entry.set_s3_key(key);

    m_superblock.add_chunk(entry);

    COUNTER_INCREMENT(m_metrics, s3_chunk_create_count, 1);
}

void S3PhysicalDev::remove_chunk(chunk_id_t chunk_id) {
    LOGDEBUGMOD(s3, "S3PhysicalDev::remove_chunk chunk_id={}", chunk_id);

    // Get S3 key before removing from superblock
    auto* entry = m_superblock.find_chunk(chunk_id);
    if (entry) {
        // Delete the S3 object.
        // NOTE: .get() blocks the calling thread. Intentional for v1;
        // v2 should batch deletes or pipeline them.
        auto s3_key = entry->get_s3_key();
        if (!s3_key.empty()) {
            auto result = m_s3_store->delete_object(s3_key).get();
            if (!result.ok()) {
                LOGWARNMOD(s3, "Failed to delete S3 object for chunk {}: {}", chunk_id, result.error_message);
            }
        }
    }

    // Remove from superblock
    m_superblock.remove_chunk(chunk_id);

    // Clear any dirty cache for this chunk
    {
        std::lock_guard< std::mutex > lock{m_dirty_cache_mutex};
        auto it = m_dirty_cache.find(chunk_id);
        if (it != m_dirty_cache.end()) {
            // Recalculate bytes
            uint64_t freed_bytes = 0;
            for (const auto& db : it->second) {
                if (db.data) { freed_bytes += db.data->size(); }
            }
            m_dirty_cache.erase(it);
            m_dirty_cache_bytes.fetch_sub(freed_bytes, std::memory_order_relaxed);
            COUNTER_SET(m_metrics, s3_dirty_cache_bytes, m_dirty_cache_bytes.load());
        }
    }

    COUNTER_INCREMENT(m_metrics, s3_chunk_remove_count, 1);
}

bool S3PhysicalDev::has_chunk(chunk_id_t chunk_id) const {
    return m_superblock.find_chunk(chunk_id) != nullptr;
}

///////////////////////// Dirty Cache Management /////////////////////////

std::vector< DirtyBlock > S3PhysicalDev::drain_dirty_cache(chunk_id_t chunk_id) {
    std::lock_guard< std::mutex > lock{m_dirty_cache_mutex};

    auto it = m_dirty_cache.find(chunk_id);
    if (it == m_dirty_cache.end()) { return {}; }

    auto blocks = std::move(it->second);
    m_dirty_cache.erase(it);

    // Update bytes counter
    uint64_t drained_bytes = 0;
    for (const auto& db : blocks) {
        if (db.data) { drained_bytes += db.data->size(); }
    }
    m_dirty_cache_bytes.fetch_sub(drained_bytes, std::memory_order_relaxed);

    COUNTER_INCREMENT(m_metrics, s3_dirty_cache_drains, 1);
    COUNTER_SET(m_metrics, s3_dirty_cache_bytes, m_dirty_cache_bytes.load());

    LOGDEBUGMOD(s3, "S3PhysicalDev::drain_dirty_cache chunk_id={} drained {} blocks ({} bytes)",
                chunk_id, blocks.size(), drained_bytes);
    return blocks;
}

std::map< chunk_id_t, std::vector< DirtyBlock > > S3PhysicalDev::drain_all_dirty_cache() {
    std::lock_guard< std::mutex > lock{m_dirty_cache_mutex};

    auto all_blocks = std::move(m_dirty_cache);
    // Note: moved-from map is in a valid empty state; no .clear() needed.
    m_dirty_cache_bytes.store(0, std::memory_order_relaxed);

    COUNTER_SET(m_metrics, s3_dirty_cache_bytes, 0);

    uint64_t total_blocks = 0;
    for (const auto& [cid, blocks] : all_blocks) {
        total_blocks += blocks.size();
    }

    LOGDEBUGMOD(s3, "S3PhysicalDev::drain_all_dirty_cache drained {} chunks, {} blocks total",
                all_blocks.size(), total_blocks);
    return all_blocks;
}

std::vector< chunk_id_t > S3PhysicalDev::get_dirty_chunks() const {
    std::lock_guard< std::mutex > lock{m_dirty_cache_mutex};

    std::vector< chunk_id_t > result;
    result.reserve(m_dirty_cache.size());
    for (const auto& [cid, _] : m_dirty_cache) {
        result.push_back(cid);
    }
    return result;
}

bool S3PhysicalDev::is_chunk_dirty(chunk_id_t chunk_id) const {
    std::lock_guard< std::mutex > lock{m_dirty_cache_mutex};
    auto it = m_dirty_cache.find(chunk_id);
    return it != m_dirty_cache.end() && !it->second.empty();
}

///////////////////////// Superblock Operations /////////////////////////

S3Result S3PhysicalDev::write_superblock() {
    // Increment generation before writing (each CP flush gets a new generation)
    m_superblock.increment_generation();

    LOGDEBUGMOD(s3, "S3PhysicalDev::write_superblock pdev_id={} gen={} num_chunks={}",
                m_pdev_id, m_superblock.generation(), m_superblock.num_chunks());

    return m_superblock.write_to_s3(*m_s3_store, m_volume_id);
}

S3Result S3PhysicalDev::load_superblock() {
    LOGDEBUGMOD(s3, "S3PhysicalDev::load_superblock pdev_id={} volume_id={}", m_pdev_id, m_volume_id);

    auto result = m_superblock.read_from_s3(*m_s3_store, m_volume_id);
    if (result) {
        LOGDEBUGMOD(s3, "Loaded superblock: pdev_id={} gen={} num_chunks={}",
                    m_superblock.pdev_id(), m_superblock.generation(), m_superblock.num_chunks());
    }
    return result;
}

///////////////////////// Private /////////////////////////

void S3PhysicalDev::check_dirty_cache_threshold() {
    auto current_bytes = m_dirty_cache_bytes.load(std::memory_order_relaxed);
    if (current_bytes > m_dirty_cache_max_bytes && m_cp_flush_cb) {
        LOGWARNMOD(s3, "Dirty cache exceeded threshold: current={}MB max={}MB — triggering early CP flush",
                   current_bytes / (1024 * 1024), m_dirty_cache_max_bytes / (1024 * 1024));
        COUNTER_INCREMENT(m_metrics, s3_early_cp_flushes, 1);
        m_cp_flush_cb->trigger_early_cp_flush();
    }
}

} // namespace homestore
