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

#include <homestore/s3/full_chunk_store.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

// Helper: convert sisl::byte_array to sisl::io_blob_safe for the async S3ObjectStore API
static sisl::io_blob_safe byte_array_to_blob(const sisl::byte_array& arr) {
    sisl::io_blob_safe blob(arr->size(), 0);
    std::memcpy(blob.bytes(), arr->cbytes(), arr->size());
    return blob;
}

// Helper: convert sisl::io_blob_safe to sisl::byte_array for our internal cache
static sisl::byte_array blob_to_byte_array(const sisl::io_blob_safe& blob) {
    auto arr = sisl::make_byte_array(static_cast< uint32_t >(blob.size()), 0);
    std::memcpy(arr->bytes(), blob.cbytes(), blob.size());
    return arr;
}

// Helper: make an error S3Result with an HTTP-style status code
static S3Result make_error_result(int code, const std::string& msg) {
    S3Result r;
    r.status_code = code;
    r.error_message = msg;
    return r;
}

static S3Result make_ok_result() {
    S3Result r;
    r.status_code = 0;
    return r;
}

FullChunkStore::FullChunkStore(std::shared_ptr< S3ObjectStore > s3_store,
                               std::shared_ptr< NvmeChunkReader > nvme_reader,
                               S3KeyMapper key_mapper,
                               uint64_t max_cache_bytes)
    : m_s3_store{std::move(s3_store)},
      m_nvme_reader{std::move(nvme_reader)},
      m_key_mapper{std::move(key_mapper)},
      m_max_cache_bytes{max_cache_bytes} {}

S3Result FullChunkStore::put(chunk_id_t chunk_id, const std::vector< DirtyBlock >& /* dirty_blocks */,
                             uint64_t chunk_size) {
    // v1: Ignore dirty_blocks granularity. Read full chunk from NVMe and upload.

    LOGTRACEMOD(s3, "FullChunkStore::put chunk_id={} chunk_size={}", chunk_id, chunk_size);

    // Step 1: Read the full chunk from NVMe
    auto [read_result, chunk_data] = m_nvme_reader->read_full_chunk(chunk_id, chunk_size);
    if (!read_result.ok()) {
        LOGERRORMOD(s3, "Failed to read chunk {} from NVMe: {}", chunk_id, read_result.error_message);
        return read_result;
    }

    // Step 2: Upload to S3 as a single object.
    // NOTE: .get() blocks the calling thread. This is intentional for v1 CP-time
    // behavior. v2 should pipeline puts across multiple chunks.
    auto s3_key = m_key_mapper.chunk_data_key(chunk_id);
    auto blob = byte_array_to_blob(chunk_data);
    auto put_result = m_s3_store->put_object(s3_key, std::move(blob)).get();
    if (!put_result.ok()) {
        LOGERRORMOD(s3, "Failed to upload chunk {} to S3 key={}: {}", chunk_id, s3_key, put_result.error_message);
        return put_result;
    }

    // Step 3: Update local cache with the data we just uploaded
    insert_cache(chunk_id, chunk_data);

    LOGDEBUGMOD(s3, "FullChunkStore::put chunk_id={} uploaded {} bytes to key={}",
                chunk_id, chunk_data->size(), s3_key);
    return make_ok_result();
}

folly::Future< std::pair< S3Result, sisl::byte_array > >
FullChunkStore::get(chunk_id_t chunk_id, offset_t offset, uint64_t size) {
    LOGTRACEMOD(s3, "FullChunkStore::get chunk_id={} offset={} size={}", chunk_id, offset, size);

    // Step 1: Check local cache
    auto cached = lookup_cache(chunk_id);
    if (cached) {
        if (offset + size > cached->size()) {
            return folly::makeFuture(std::make_pair(
                make_error_result(400, fmt::format("offset={} + size={} exceeds chunk size={}", offset, size,
                                                   cached->size())),
                sisl::byte_array{}));
        }

        auto result = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(result->bytes(), cached->cbytes() + offset, size);

        LOGTRACEMOD(s3, "FullChunkStore::get chunk_id={} cache HIT, returning {} bytes at offset={}",
                    chunk_id, size, offset);
        return folly::makeFuture(std::make_pair(make_ok_result(), std::move(result)));
    }

    // Step 2: Cache miss — fetch with dedup, then extract range
    return fetch_and_cache(chunk_id).thenValue(
        [chunk_id, offset, size](std::pair< S3Result, sisl::byte_array >&& fetch_pair) mutable
            -> std::pair< S3Result, sisl::byte_array > {
            auto& [fetch_result, fetched_data] = fetch_pair;
            if (!fetch_result.ok()) {
                return {std::move(fetch_result), sisl::byte_array{}};
            }

            if (offset + size > fetched_data->size()) {
                return {make_error_result(400, fmt::format("offset={} + size={} exceeds chunk size={}", offset, size,
                                                           fetched_data->size())),
                        sisl::byte_array{}};
            }

            auto result = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
            std::memcpy(result->bytes(), fetched_data->cbytes() + offset, size);
            return {make_ok_result(), std::move(result)};
        });
}

S3Result FullChunkStore::compact(chunk_id_t chunk_id) {
    LOGTRACEMOD(s3, "FullChunkStore::compact chunk_id={} (no-op for v1)", chunk_id);
    return make_ok_result();
}

ChunkState FullChunkStore::recover(chunk_id_t chunk_id) {
    LOGDEBUGMOD(s3, "FullChunkStore::recover chunk_id={}", chunk_id);

    ChunkState state;
    state.chunk_id = chunk_id;

    auto s3_key = m_key_mapper.chunk_data_key(chunk_id);
    // NOTE: .get() blocks the calling thread. Intentional for recovery path.
    auto [get_result, blob] = m_s3_store->get_object(s3_key).get();
    if (!get_result.ok()) {
        LOGERRORMOD(s3, "Failed to recover chunk {} from S3 key={}: {}", chunk_id, s3_key, get_result.error_message);
        state.valid = false;
        return state;
    }

    state.data = blob_to_byte_array(blob);
    state.chunk_size = state.data->size();
    state.valid = true;

    LOGDEBUGMOD(s3, "FullChunkStore::recover chunk_id={} recovered {} bytes from key={}",
                chunk_id, state.chunk_size, s3_key);
    return state;
}

ChunkMetadata FullChunkStore::describe(chunk_id_t chunk_id) {
    ChunkMetadata meta;
    meta.chunk_id = chunk_id;
    meta.s3_key = m_key_mapper.chunk_data_key(chunk_id);

    // NOTE: .get() blocks. Intentional for metadata queries.
    auto head_result = m_s3_store->head_object(meta.s3_key).get();
    if (head_result.ok() && head_result.exists) {
        meta.s3_object_size = head_result.content_length;
    }

    return meta;
}

void FullChunkStore::invalidate_cache(chunk_id_t chunk_id) {
    std::lock_guard< std::mutex > lock{m_cache_mutex};
    auto it = m_cache_map.find(chunk_id);
    if (it != m_cache_map.end()) {
        auto& entry = *(it->second);
        if (entry.second) { m_cache_bytes -= entry.second->size(); }
        m_lru_list.erase(it->second);
        m_cache_map.erase(it);
    }
}

void FullChunkStore::invalidate_all_cache() {
    std::lock_guard< std::mutex > lock{m_cache_mutex};
    m_lru_list.clear();
    m_cache_map.clear();
    m_cache_bytes = 0;
}

uint64_t FullChunkStore::cache_size_bytes() const {
    std::lock_guard< std::mutex > lock{m_cache_mutex};
    return m_cache_bytes;
}

folly::Future< std::pair< S3Result, sisl::byte_array > >
FullChunkStore::fetch_and_cache(chunk_id_t chunk_id) {
    // Dedup: if another caller is already fetching this chunk, piggyback on that fetch
    {
        std::lock_guard< std::mutex > lock{m_inflight_mutex};
        auto it = m_inflight_fetches.find(chunk_id);
        if (it != m_inflight_fetches.end()) {
            LOGTRACEMOD(s3, "FullChunkStore::fetch_and_cache chunk_id={} — joining in-flight fetch", chunk_id);
            return it->second->getFuture();
        }

        // First caller for this chunk — create the SharedPromise
        auto sp = std::make_shared< folly::SharedPromise< std::pair< S3Result, sisl::byte_array > > >();
        m_inflight_fetches[chunk_id] = sp;
    }

    auto s3_key = m_key_mapper.chunk_data_key(chunk_id);

    return m_s3_store->get_object(s3_key).thenValue(
        [this, chunk_id](std::pair< S3Result, sisl::io_blob_safe >&& result) mutable
            -> std::pair< S3Result, sisl::byte_array > {
            auto& [get_result, blob] = result;

            std::pair< S3Result, sisl::byte_array > ret;
            if (!get_result.ok()) {
                ret = {get_result, sisl::byte_array{}};
            } else {
                auto data = blob_to_byte_array(blob);
                insert_cache(chunk_id, data);
                ret = {make_ok_result(), std::move(data)};
            }

            // Fulfil the SharedPromise and remove from in-flight map
            std::shared_ptr< folly::SharedPromise< std::pair< S3Result, sisl::byte_array > > > sp;
            {
                std::lock_guard< std::mutex > lock{m_inflight_mutex};
                auto it = m_inflight_fetches.find(chunk_id);
                if (it != m_inflight_fetches.end()) {
                    sp = it->second;
                    m_inflight_fetches.erase(it);
                }
            }
            if (sp) { sp->setValue(ret); }

            return ret;
        });
}

sisl::byte_array FullChunkStore::lookup_cache(chunk_id_t chunk_id) {
    std::lock_guard< std::mutex > lock{m_cache_mutex};
    auto it = m_cache_map.find(chunk_id);
    if (it != m_cache_map.end()) {
        // Promote to MRU (front of list)
        m_lru_list.splice(m_lru_list.begin(), m_lru_list, it->second);
        return it->second->second;
    }
    return {};
}

void FullChunkStore::insert_cache(chunk_id_t chunk_id, const sisl::byte_array& data) {
    std::lock_guard< std::mutex > lock{m_cache_mutex};

    // If already cached, update in-place
    auto it = m_cache_map.find(chunk_id);
    if (it != m_cache_map.end()) {
        auto& entry = *(it->second);
        if (entry.second) { m_cache_bytes -= entry.second->size(); }
        entry.second = data;
        if (data) { m_cache_bytes += data->size(); }
        m_lru_list.splice(m_lru_list.begin(), m_lru_list, it->second);
    } else {
        m_lru_list.emplace_front(chunk_id, data);
        m_cache_map[chunk_id] = m_lru_list.begin();
        if (data) { m_cache_bytes += data->size(); }
    }

    // Evict if over budget
    evict_cache_locked();
}

void FullChunkStore::evict_cache_locked() {
    if (m_max_cache_bytes == 0) return; // unlimited

    while (m_cache_bytes > m_max_cache_bytes && !m_lru_list.empty()) {
        auto& victim = m_lru_list.back();
        LOGTRACEMOD(s3, "FullChunkStore LRU evict chunk_id={} size={}", victim.first,
                    victim.second ? victim.second->size() : 0);
        if (victim.second) { m_cache_bytes -= victim.second->size(); }
        m_cache_map.erase(victim.first);
        m_lru_list.pop_back();
    }
}

} // namespace homestore
