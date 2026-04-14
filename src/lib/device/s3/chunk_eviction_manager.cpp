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

#include <homestore/s3/chunk_eviction_manager.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

std::string to_string(EvictionResult r) {
    switch (r) {
    case EvictionResult::SUCCESS: return "SUCCESS";
    case EvictionResult::NOT_ON_S3: return "NOT_ON_S3";
    case EvictionResult::ALREADY_EVICTED: return "ALREADY_EVICTED";
    case EvictionResult::EVICTION_IN_PROGRESS: return "EVICTION_IN_PROGRESS";
    case EvictionResult::HAS_DIRTY_DATA: return "HAS_DIRTY_DATA";
    case EvictionResult::FLUSH_FAILED: return "FLUSH_FAILED";
    case EvictionResult::INTERNAL_ERROR: return "INTERNAL_ERROR";
    }
    return "UNKNOWN";
}

ChunkEvictionManager::ChunkEvictionManager(S3PhysicalDev* s3_pdev,
                                           std::shared_ptr< NvmeChunkManager > nvme_mgr)
    : m_s3_pdev{s3_pdev}, m_nvme_mgr{std::move(nvme_mgr)} {
    RELEASE_ASSERT(m_s3_pdev != nullptr, "S3PhysicalDev must not be null");
    RELEASE_ASSERT(m_nvme_mgr != nullptr, "NvmeChunkManager must not be null");
    LOGDEBUGMOD(s3, "ChunkEvictionManager created");
}

EvictionResult ChunkEvictionManager::evict_chunk(chunk_id_t chunk_id, uint64_t chunk_size) {
    auto start = std::chrono::steady_clock::now();

    LOGDEBUGMOD(s3, "evict_chunk: starting eviction for chunk_id={}", chunk_id);

    // Step 1: Verify chunk is on S3 (precondition)
    if (!m_s3_pdev->has_chunk(chunk_id)) {
        LOGWARNMOD(s3, "evict_chunk: chunk_id={} not on S3 — cannot evict", chunk_id);
        COUNTER_INCREMENT(m_metrics, eviction_rejected_not_on_s3, 1);
        return EvictionResult::NOT_ON_S3;
    }

    // Step 2: Transition to EVICTING state (blocks writes)
    auto& state = get_or_create_state(chunk_id);
    {
        std::unique_lock lock{state.mtx};

        if (state.state == ChunkEvictionState::EVICTED) {
            LOGDEBUGMOD(s3, "evict_chunk: chunk_id={} already evicted", chunk_id);
            COUNTER_INCREMENT(m_metrics, eviction_rejected_already_evicted, 1);
            return EvictionResult::ALREADY_EVICTED;
        }

        if (state.state == ChunkEvictionState::EVICTING) {
            LOGDEBUGMOD(s3, "evict_chunk: chunk_id={} eviction already in progress", chunk_id);
            COUNTER_INCREMENT(m_metrics, eviction_rejected_in_progress, 1);
            return EvictionResult::EVICTION_IN_PROGRESS;
        }

        state.state = ChunkEvictionState::EVICTING;
    }

    // Step 3: Flush any dirty data to S3
    if (m_s3_pdev->is_chunk_dirty(chunk_id)) {
        LOGDEBUGMOD(s3, "evict_chunk: chunk_id={} has dirty data, flushing to S3", chunk_id);

        auto dirty_blocks = m_s3_pdev->drain_dirty_cache(chunk_id);
        if (!dirty_blocks.empty()) {
            auto result = m_s3_pdev->chunk_store()->put(chunk_id, dirty_blocks, chunk_size);
            if (!result.ok()) {
                LOGERRORMOD(s3, "evict_chunk: S3 flush failed for chunk_id={}: {}",
                            chunk_id, result.error_message);
                // Roll back state
                std::unique_lock lock{state.mtx};
                state.state = ChunkEvictionState::NORMAL;
                COUNTER_INCREMENT(m_metrics, eviction_flush_failures, 1);
                return EvictionResult::FLUSH_FAILED;
            }
        }
    }

    // Step 4: Verify chunk is on NVMe before releasing
    if (!m_nvme_mgr->is_chunk_on_nvme(chunk_id)) {
        LOGDEBUGMOD(s3, "evict_chunk: chunk_id={} not on NVMe (already S3-only), marking evicted", chunk_id);
        std::unique_lock lock{state.mtx};
        state.state = ChunkEvictionState::EVICTED;
        COUNTER_INCREMENT(m_metrics, eviction_success_count, 1);

        auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
            std::chrono::steady_clock::now() - start).count();
        HISTOGRAM_OBSERVE(m_metrics, eviction_latency_us, elapsed_us);
        return EvictionResult::SUCCESS;
    }

    // Step 5: Release NVMe space
    auto ec = m_nvme_mgr->release_nvme_chunk(chunk_id);
    if (ec) {
        LOGERRORMOD(s3, "evict_chunk: failed to release NVMe for chunk_id={}: {}",
                    chunk_id, ec.message());
        std::unique_lock lock{state.mtx};
        state.state = ChunkEvictionState::NORMAL;
        COUNTER_INCREMENT(m_metrics, eviction_errors, 1);
        return EvictionResult::INTERNAL_ERROR;
    }

    // Step 6: Mark as evicted
    {
        std::unique_lock lock{state.mtx};
        state.state = ChunkEvictionState::EVICTED;
    }

    auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
        std::chrono::steady_clock::now() - start).count();
    HISTOGRAM_OBSERVE(m_metrics, eviction_latency_us, elapsed_us);
    COUNTER_INCREMENT(m_metrics, eviction_success_count, 1);
    COUNTER_INCREMENT(m_metrics, eviction_nvme_bytes_freed, chunk_size);

    LOGDEBUGMOD(s3, "evict_chunk: chunk_id={} evicted successfully ({}us)", chunk_id, elapsed_us);
    return EvictionResult::SUCCESS;
}

bool ChunkEvictionManager::is_write_blocked(chunk_id_t chunk_id) const {
    auto* state = find_state(chunk_id);
    if (!state) return false;

    std::shared_lock lock{state->mtx};
    if (state->state == ChunkEvictionState::EVICTING) {
        COUNTER_INCREMENT(m_metrics, writes_blocked_by_eviction, 1);
        return true;
    }
    return false;
}

bool ChunkEvictionManager::is_chunk_evicted(chunk_id_t chunk_id) const {
    auto* state = find_state(chunk_id);
    if (!state) return false;

    std::shared_lock lock{state->mtx};
    return state->state == ChunkEvictionState::EVICTED;
}

void ChunkEvictionManager::mark_hydrated(chunk_id_t chunk_id) {
    auto* state = find_state_mut(chunk_id);
    if (!state) return;

    std::unique_lock lock{state->mtx};
    if (state->state == ChunkEvictionState::EVICTED) {
        state->state = ChunkEvictionState::NORMAL;
        LOGDEBUGMOD(s3, "mark_hydrated: chunk_id={} restored to NORMAL", chunk_id);
    }
}

ChunkEvictionManager::ChunkState& ChunkEvictionManager::get_or_create_state(chunk_id_t chunk_id) {
    std::lock_guard lock{m_states_mutex};
    auto it = m_chunk_states.find(chunk_id);
    if (it != m_chunk_states.end()) return *it->second;

    auto [ins_it, _] = m_chunk_states.emplace(chunk_id, std::make_unique< ChunkState >());
    return *ins_it->second;
}

const ChunkEvictionManager::ChunkState* ChunkEvictionManager::find_state(chunk_id_t chunk_id) const {
    std::lock_guard lock{m_states_mutex};
    auto it = m_chunk_states.find(chunk_id);
    if (it == m_chunk_states.end()) return nullptr;
    return it->second.get();
}

ChunkEvictionManager::ChunkState* ChunkEvictionManager::find_state_mut(chunk_id_t chunk_id) {
    std::lock_guard lock{m_states_mutex};
    auto it = m_chunk_states.find(chunk_id);
    if (it == m_chunk_states.end()) return nullptr;
    return it->second.get();
}

} // namespace homestore
