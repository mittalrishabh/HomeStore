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

#include <homestore/s3/lru_eviction_policy.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

void LruEvictionPolicy::add_chunk(chunk_id_t chunk_id, uint64_t chunk_size) {
    std::lock_guard lock{m_mutex};
    if (m_chunk_map.count(chunk_id) > 0) return;

    m_lru_list.push_back(ChunkEntry{chunk_id, chunk_size});
    m_chunk_map[chunk_id] = std::prev(m_lru_list.end());

    COUNTER_INCREMENT(m_metrics, eviction_policy_chunks_tracked, 1);
    LOGDEBUGMOD(s3, "LruEvictionPolicy: add_chunk chunk_id={} size={} tracked={}",
                chunk_id, chunk_size, m_chunk_map.size());
}

void LruEvictionPolicy::remove_chunk(chunk_id_t chunk_id) {
    std::lock_guard lock{m_mutex};
    auto it = m_chunk_map.find(chunk_id);
    if (it == m_chunk_map.end()) return;

    m_lru_list.erase(it->second);
    m_chunk_map.erase(it);

    COUNTER_DECREMENT(m_metrics, eviction_policy_chunks_tracked, 1);
    LOGDEBUGMOD(s3, "LruEvictionPolicy: remove_chunk chunk_id={} tracked={}", chunk_id, m_chunk_map.size());
}

void LruEvictionPolicy::record_access(chunk_id_t chunk_id) {
    std::lock_guard lock{m_mutex};
    auto it = m_chunk_map.find(chunk_id);
    if (it == m_chunk_map.end()) return;

    auto entry = *(it->second);
    m_lru_list.erase(it->second);
    m_lru_list.push_back(entry);
    it->second = std::prev(m_lru_list.end());

    COUNTER_INCREMENT(m_metrics, eviction_policy_access_updates, 1);
}

bool LruEvictionPolicy::select_eviction_candidate(chunk_id_t& out_chunk_id, uint64_t& out_chunk_size) {
    std::lock_guard lock{m_mutex};
    if (m_lru_list.empty()) {
        COUNTER_INCREMENT(m_metrics, eviction_policy_no_candidate, 1);
        return false;
    }

    auto& front = m_lru_list.front();
    out_chunk_id = front.chunk_id;
    out_chunk_size = front.chunk_size;

    m_lru_list.pop_front();
    m_chunk_map.erase(out_chunk_id);

    COUNTER_INCREMENT(m_metrics, eviction_policy_candidates_selected, 1);
    COUNTER_DECREMENT(m_metrics, eviction_policy_chunks_tracked, 1);
    LOGDEBUGMOD(s3, "LruEvictionPolicy: selected eviction candidate chunk_id={} size={}", out_chunk_id, out_chunk_size);
    return true;
}

uint32_t LruEvictionPolicy::tracked_count() const {
    std::lock_guard lock{m_mutex};
    return static_cast< uint32_t >(m_chunk_map.size());
}

bool LruEvictionPolicy::is_tracked(chunk_id_t chunk_id) const {
    std::lock_guard lock{m_mutex};
    return m_chunk_map.count(chunk_id) > 0;
}

} // namespace homestore
