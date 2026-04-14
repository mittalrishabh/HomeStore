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

#include <homestore/s3/s3_gc.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

S3GarbageCollector::S3GarbageCollector(const PdevS3Superblock* superblock,
                                       std::shared_ptr< S3ObjectStore > s3_store)
    : m_superblock{superblock}, m_s3_store{std::move(s3_store)} {}

void S3GarbageCollector::on_generation_change(const std::vector< std::string >& superseded_keys) {
    std::lock_guard< std::mutex > lock{m_pending_mutex};
    for (const auto& key : superseded_keys) {
        m_pending_keys.insert(key);
    }
    LOGDEBUGMOD(s3, "S3 GC: added {} superseded keys as GC candidates (total pending={})",
                superseded_keys.size(), m_pending_keys.size());
}

void S3GarbageCollector::add_gc_candidate(const std::string& s3_key) {
    std::lock_guard< std::mutex > lock{m_pending_mutex};
    m_pending_keys.insert(s3_key);
}

std::unordered_set< std::string > S3GarbageCollector::collect_pinned_keys() const {
    std::unordered_set< std::string > pinned;
    for (const auto& snap : m_superblock->snapshots()) {
        for (const auto& ck : snap.chunk_keys) {
            pinned.insert(ck.get_s3_key());
        }
    }
    return pinned;
}

bool S3GarbageCollector::is_key_pinned(const std::string& s3_key) const {
    for (const auto& snap : m_superblock->snapshots()) {
        for (const auto& ck : snap.chunk_keys) {
            if (ck.get_s3_key() == s3_key) { return true; }
        }
    }
    return false;
}

uint64_t S3GarbageCollector::run_gc() {
    auto start = std::chrono::steady_clock::now();
    COUNTER_INCREMENT(m_metrics, gc_runs_total, 1);

    std::set< std::string > candidates;
    {
        std::lock_guard< std::mutex > lock{m_pending_mutex};
        candidates = std::move(m_pending_keys);
        m_pending_keys.clear();
    }

    if (candidates.empty()) {
        LOGDEBUGMOD(s3, "S3 GC: no pending candidates");
        return 0;
    }

    auto pinned = collect_pinned_keys();

    std::vector< std::string > to_delete;
    uint64_t retained = 0;

    for (const auto& key : candidates) {
        if (pinned.count(key) > 0) {
            ++retained;
        } else {
            to_delete.push_back(key);
        }
    }

    COUNTER_INCREMENT(m_metrics, gc_objects_retained, retained);

    if (to_delete.empty()) {
        LOGDEBUGMOD(s3, "S3 GC: all {} candidates pinned by snapshots, nothing to delete", candidates.size());
        auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
                              std::chrono::steady_clock::now() - start)
                              .count();
        HISTOGRAM_OBSERVE(m_metrics, gc_run_latency_us, elapsed_us);
        return 0;
    }

    LOGDEBUGMOD(s3, "S3 GC: deleting {} objects ({} retained by snapshots)", to_delete.size(), retained);

    auto result = m_s3_store->delete_objects(to_delete).get();
    uint64_t deleted = 0;
    if (result.ok()) {
        deleted = to_delete.size();
        COUNTER_INCREMENT(m_metrics, gc_objects_deleted, deleted);
        LOGDEBUGMOD(s3, "S3 GC: successfully deleted {} objects", deleted);
    } else {
        LOGERRORMOD(s3, "S3 GC: batch delete failed: {}", result.error_message);
        COUNTER_INCREMENT(m_metrics, gc_delete_errors, 1);

        // Fall back to individual deletes
        for (const auto& key : to_delete) {
            auto del_result = m_s3_store->delete_object(key).get();
            if (del_result.ok()) {
                ++deleted;
                COUNTER_INCREMENT(m_metrics, gc_objects_deleted, 1);
            } else {
                LOGERRORMOD(s3, "S3 GC: failed to delete {}: {}", key, del_result.error_message);
                COUNTER_INCREMENT(m_metrics, gc_delete_errors, 1);
                // Re-add failed key for next GC run
                std::lock_guard< std::mutex > lock{m_pending_mutex};
                m_pending_keys.insert(key);
            }
        }
    }

    auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
                          std::chrono::steady_clock::now() - start)
                          .count();
    HISTOGRAM_OBSERVE(m_metrics, gc_run_latency_us, elapsed_us);

    LOGDEBUGMOD(s3, "S3 GC: completed in {} us — deleted={} retained={}", elapsed_us, deleted, retained);
    return deleted;
}

uint64_t S3GarbageCollector::pending_count() const {
    std::lock_guard< std::mutex > lock{m_pending_mutex};
    return m_pending_keys.size();
}

void S3GarbageCollector::clear_pending() {
    std::lock_guard< std::mutex > lock{m_pending_mutex};
    m_pending_keys.clear();
}

} // namespace homestore
