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

#include <homestore/s3/chunk_hydration_manager.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

std::string to_string(HydrationResult r) {
    switch (r) {
    case HydrationResult::SUCCESS: return "SUCCESS";
    case HydrationResult::ALREADY_ON_NVME: return "ALREADY_ON_NVME";
    case HydrationResult::ALREADY_HYDRATING: return "ALREADY_HYDRATING";
    case HydrationResult::S3_READ_FAILED: return "S3_READ_FAILED";
    case HydrationResult::NVME_WRITE_FAILED: return "NVME_WRITE_FAILED";
    case HydrationResult::NVME_FULL: return "NVME_FULL";
    case HydrationResult::EVICTION_FOR_SPACE_FAILED: return "EVICTION_FOR_SPACE_FAILED";
    case HydrationResult::CHUNK_NOT_ON_S3: return "CHUNK_NOT_ON_S3";
    case HydrationResult::SHUTTING_DOWN: return "SHUTTING_DOWN";
    }
    return "UNKNOWN";
}

ChunkHydrationManager::ChunkHydrationManager(std::shared_ptr< ChunkStore > chunk_store,
                                             std::shared_ptr< NvmeDeviceIO > nvme_io,
                                             std::shared_ptr< NvmeChunkAllocator > nvme_alloc,
                                             std::shared_ptr< ChunkEvictionManager > eviction_mgr,
                                             std::shared_ptr< EvictionCandidateSelector > eviction_selector,
                                             uint32_t num_workers)
    : m_chunk_store{std::move(chunk_store)}
    , m_nvme_io{std::move(nvme_io)}
    , m_nvme_alloc{std::move(nvme_alloc)}
    , m_eviction_mgr{std::move(eviction_mgr)}
    , m_eviction_selector{std::move(eviction_selector)} {
    RELEASE_ASSERT(m_chunk_store != nullptr, "ChunkStore must not be null");
    RELEASE_ASSERT(m_nvme_io != nullptr, "NvmeDeviceIO must not be null");
    RELEASE_ASSERT(m_nvme_alloc != nullptr, "NvmeChunkAllocator must not be null");
    RELEASE_ASSERT(m_eviction_mgr != nullptr, "ChunkEvictionManager must not be null");

    for (uint32_t i = 0; i < num_workers; ++i) {
        m_workers.emplace_back([this] { worker_loop(); });
    }

    LOGDEBUGMOD(s3, "ChunkHydrationManager created with {} worker(s)", num_workers);
}

ChunkHydrationManager::~ChunkHydrationManager() { shutdown(); }

HydrationResult ChunkHydrationManager::schedule_hydration(chunk_id_t chunk_id, uint64_t chunk_size) {
    if (m_shutdown.load(std::memory_order_relaxed)) {
        return HydrationResult::SHUTTING_DOWN;
    }

    if (m_nvme_io->is_chunk_on_nvme(chunk_id)) {
        COUNTER_INCREMENT(m_metrics, hydration_skipped_already_on_nvme, 1);
        return HydrationResult::ALREADY_ON_NVME;
    }

    {
        std::lock_guard lock{m_inflight_mutex};
        if (m_inflight.count(chunk_id) > 0) {
            COUNTER_INCREMENT(m_metrics, hydration_skipped_already_hydrating, 1);
            return HydrationResult::ALREADY_HYDRATING;
        }
        m_inflight.insert(chunk_id);
    }

    {
        std::lock_guard lock{m_queue_mutex};
        m_queue.push(HydrationRequest{chunk_id, chunk_size});
    }
    m_queue_cv.notify_one();

    COUNTER_INCREMENT(m_metrics, hydration_scheduled_count, 1);
    LOGDEBUGMOD(s3, "schedule_hydration: enqueued chunk_id={} size={}", chunk_id, chunk_size);
    return HydrationResult::SUCCESS;
}

HydrationResult ChunkHydrationManager::hydrate_sync(chunk_id_t chunk_id, uint64_t chunk_size) {
    if (m_nvme_io->is_chunk_on_nvme(chunk_id)) {
        COUNTER_INCREMENT(m_metrics, hydration_skipped_already_on_nvme, 1);
        return HydrationResult::ALREADY_ON_NVME;
    }

    {
        std::lock_guard lock{m_inflight_mutex};
        if (m_inflight.count(chunk_id) > 0) {
            COUNTER_INCREMENT(m_metrics, hydration_skipped_already_hydrating, 1);
            return HydrationResult::ALREADY_HYDRATING;
        }
        m_inflight.insert(chunk_id);
    }

    auto result = do_hydrate(chunk_id, chunk_size);

    {
        std::lock_guard lock{m_inflight_mutex};
        m_inflight.erase(chunk_id);
    }

    return result;
}

bool ChunkHydrationManager::is_hydrating(chunk_id_t chunk_id) const {
    std::lock_guard lock{m_inflight_mutex};
    return m_inflight.count(chunk_id) > 0;
}

uint32_t ChunkHydrationManager::pending_count() const {
    std::lock_guard lock{m_queue_mutex};
    return static_cast< uint32_t >(m_queue.size());
}

void ChunkHydrationManager::shutdown() {
    bool expected = false;
    if (!m_shutdown.compare_exchange_strong(expected, true)) {
        return;
    }

    m_queue_cv.notify_all();
    for (auto& w : m_workers) {
        if (w.joinable()) w.join();
    }
    m_workers.clear();

    LOGDEBUGMOD(s3, "ChunkHydrationManager shut down");
}

HydrationResult ChunkHydrationManager::do_hydrate(chunk_id_t chunk_id, uint64_t chunk_size) {
    auto start = std::chrono::steady_clock::now();

    LOGDEBUGMOD(s3, "do_hydrate: starting hydration for chunk_id={} size={}", chunk_id, chunk_size);

    // Step 1: Download chunk from S3
    auto chunk_state = m_chunk_store->recover(chunk_id);
    if (!chunk_state.valid || !chunk_state.data) {
        LOGERRORMOD(s3, "do_hydrate: S3 recover failed for chunk_id={}", chunk_id);
        COUNTER_INCREMENT(m_metrics, hydration_s3_read_failures, 1);
        return HydrationResult::S3_READ_FAILED;
    }

    // Step 2: Allocate NVMe space (evicting if necessary)
    auto alloc_ec = m_nvme_alloc->allocate_nvme_chunk(chunk_id, chunk_size);
    if (alloc_ec) {
        if (!ensure_nvme_space(chunk_size)) {
            LOGWARNMOD(s3, "do_hydrate: cannot free NVMe space for chunk_id={}", chunk_id);
            COUNTER_INCREMENT(m_metrics, hydration_eviction_for_space_failures, 1);
            return HydrationResult::NVME_FULL;
        }

        alloc_ec = m_nvme_alloc->allocate_nvme_chunk(chunk_id, chunk_size);
        if (alloc_ec) {
            LOGERRORMOD(s3, "do_hydrate: NVMe alloc failed after eviction for chunk_id={}: {}",
                        chunk_id, alloc_ec.message());
            return HydrationResult::NVME_FULL;
        }
    }

    // Step 3: Write chunk data to NVMe
    auto write_size = std::min(static_cast< uint64_t >(chunk_state.data->size()), chunk_size);
    auto write_ec = m_nvme_io->nvme_write(chunk_id, 0,
                                           reinterpret_cast< const char* >(chunk_state.data->cbytes()),
                                           write_size);
    if (write_ec) {
        LOGERRORMOD(s3, "do_hydrate: NVMe write failed for chunk_id={}: {}", chunk_id, write_ec.message());
        COUNTER_INCREMENT(m_metrics, hydration_nvme_write_failures, 1);
        return HydrationResult::NVME_WRITE_FAILED;
    }

    // Step 4: Mark chunk as hydrated (on_nvme = true)
    m_eviction_mgr->mark_hydrated(chunk_id);

    auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
        std::chrono::steady_clock::now() - start).count();
    HISTOGRAM_OBSERVE(m_metrics, hydration_latency_us, elapsed_us);
    COUNTER_INCREMENT(m_metrics, hydration_success_count, 1);
    COUNTER_INCREMENT(m_metrics, hydration_nvme_bytes_written, write_size);

    LOGDEBUGMOD(s3, "do_hydrate: chunk_id={} hydrated successfully ({}us, {} bytes)",
                chunk_id, elapsed_us, write_size);

    return HydrationResult::SUCCESS;
}

bool ChunkHydrationManager::ensure_nvme_space(uint64_t needed_bytes) {
    if (!m_eviction_selector) {
        LOGWARNMOD(s3, "ensure_nvme_space: no eviction selector — cannot free space");
        return false;
    }

    chunk_id_t victim_id;
    uint64_t victim_size;
    if (!m_eviction_selector->select_eviction_candidate(victim_id, victim_size)) {
        LOGWARNMOD(s3, "ensure_nvme_space: no eviction candidate available");
        return false;
    }

    LOGDEBUGMOD(s3, "ensure_nvme_space: evicting chunk_id={} (size={}) to make room for {} bytes",
                victim_id, victim_size, needed_bytes);

    COUNTER_INCREMENT(m_metrics, hydration_eviction_for_space_count, 1);
    auto result = m_eviction_mgr->evict_chunk(victim_id, victim_size);
    if (result != EvictionResult::SUCCESS) {
        LOGWARNMOD(s3, "ensure_nvme_space: eviction failed for chunk_id={}: {}",
                    victim_id, to_string(result));
        COUNTER_INCREMENT(m_metrics, hydration_eviction_for_space_failures, 1);
        return false;
    }

    return true;
}

void ChunkHydrationManager::worker_loop() {
    while (true) {
        HydrationRequest req;
        {
            std::unique_lock lock{m_queue_mutex};
            m_queue_cv.wait(lock, [this] {
                return m_shutdown.load(std::memory_order_relaxed) || !m_queue.empty();
            });

            if (m_shutdown.load(std::memory_order_relaxed) && m_queue.empty()) {
                return;
            }

            req = m_queue.front();
            m_queue.pop();
        }

        auto result = do_hydrate(req.chunk_id, req.chunk_size);
        if (result != HydrationResult::SUCCESS) {
            LOGWARNMOD(s3, "worker_loop: hydration failed for chunk_id={}: {}",
                        req.chunk_id, to_string(result));
        }

        {
            std::lock_guard lock{m_inflight_mutex};
            m_inflight.erase(req.chunk_id);
        }
    }
}

} // namespace homestore
