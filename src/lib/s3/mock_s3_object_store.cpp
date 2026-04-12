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
#include <cstring>

#include "s3/s3_object_store_impl.h"

SISL_LOGGING_DEF(s3)

namespace homestore {

MockS3ObjectStore::MockS3ObjectStore(const S3ObjectStoreConfig& cfg) : m_cfg{cfg} {
    LOGINFO("MockS3ObjectStore created for bucket={} region={}", m_cfg.bucket, m_cfg.region);
}

// ─── helpers ────────────────────────────────────────────────────────────────

S3Result MockS3ObjectStore::make_ok() {
    S3Result r;
    r.status_code = 200;
    return r;
}

S3Result MockS3ObjectStore::make_not_found(const std::string& key) {
    S3Result r;
    r.status_code = 404;
    r.error_message = "NoSuchKey: " + key;
    return r;
}

// ─── put ────────────────────────────────────────────────────────────────────

folly::Future< S3Result > MockS3ObjectStore::put_object(const std::string& key, sisl::io_blob_safe data) {
    LOGDEBUG("MockS3: put_object key={} size={}", key, data.size());

    {
        std::lock_guard lg(m_mtx);
        // Copy data into a new blob so the caller can release theirs.
        sisl::io_blob_safe copy(data.size(), 0);
        std::memcpy(copy.bytes(), data.cbytes(), data.size());
        m_objects.insert_or_assign(key, std::move(copy));
    }

    m_total_puts.fetch_add(1, std::memory_order_relaxed);

    auto res = make_ok();
    res.content_length = data.size();
    return folly::makeFuture(std::move(res));
}

// ─── get (full) ─────────────────────────────────────────────────────────────

folly::Future< std::pair< S3Result, sisl::io_blob_safe > > MockS3ObjectStore::get_object(const std::string& key) {
    LOGDEBUG("MockS3: get_object key={}", key);

    std::lock_guard lg(m_mtx);
    auto it = m_objects.find(key);
    if (it == m_objects.end()) {
        return folly::makeFuture(std::make_pair(make_not_found(key), sisl::io_blob_safe{}));
    }

    m_total_gets.fetch_add(1, std::memory_order_relaxed);

    const auto& stored = it->second;
    sisl::io_blob_safe copy(stored.size(), 0);
    std::memcpy(copy.bytes(), stored.cbytes(), stored.size());

    auto res = make_ok();
    res.content_length = stored.size();
    return folly::makeFuture(std::make_pair(std::move(res), std::move(copy)));
}

// ─── get (range) ────────────────────────────────────────────────────────────

folly::Future< std::pair< S3Result, sisl::io_blob_safe > >
MockS3ObjectStore::get_object_range(const std::string& key, uint64_t offset, uint64_t length) {
    LOGDEBUG("MockS3: get_object_range key={} offset={} length={}", key, offset, length);

    std::lock_guard lg(m_mtx);
    auto it = m_objects.find(key);
    if (it == m_objects.end()) {
        return folly::makeFuture(std::make_pair(make_not_found(key), sisl::io_blob_safe{}));
    }

    m_total_gets.fetch_add(1, std::memory_order_relaxed);

    const auto& stored = it->second;
    if (offset >= stored.size()) {
        S3Result r;
        r.status_code = 416; // Range Not Satisfiable
        r.error_message = "InvalidRange";
        return folly::makeFuture(std::make_pair(std::move(r), sisl::io_blob_safe{}));
    }

    uint64_t actual_len = std::min(length, static_cast< uint64_t >(stored.size()) - offset);
    sisl::io_blob_safe copy(actual_len, 0);
    std::memcpy(copy.bytes(), stored.cbytes() + offset, actual_len);

    auto res = make_ok();
    res.content_length = actual_len;
    return folly::makeFuture(std::make_pair(std::move(res), std::move(copy)));
}

// ─── delete (single) ───────────────────────────────────────────────────────

folly::Future< S3Result > MockS3ObjectStore::delete_object(const std::string& key) {
    LOGDEBUG("MockS3: delete_object key={}", key);

    std::lock_guard lg(m_mtx);
    m_objects.erase(key);
    m_total_deletes.fetch_add(1, std::memory_order_relaxed);
    return folly::makeFuture(make_ok());
}

// ─── delete (batch) ─────────────────────────────────────────────────────────

folly::Future< S3Result > MockS3ObjectStore::delete_objects(const std::vector< std::string >& keys) {
    LOGDEBUG("MockS3: delete_objects count={}", keys.size());

    std::lock_guard lg(m_mtx);
    for (const auto& k : keys) {
        m_objects.erase(k);
    }
    m_total_deletes.fetch_add(keys.size(), std::memory_order_relaxed);
    return folly::makeFuture(make_ok());
}

// ─── head ───────────────────────────────────────────────────────────────────

folly::Future< S3Result > MockS3ObjectStore::head_object(const std::string& key) {
    LOGDEBUG("MockS3: head_object key={}", key);

    std::lock_guard lg(m_mtx);
    auto it = m_objects.find(key);
    if (it == m_objects.end()) {
        auto r = make_not_found(key);
        r.exists = false;
        return folly::makeFuture(std::move(r));
    }

    auto res = make_ok();
    res.exists = true;
    res.content_length = it->second.size();
    return folly::makeFuture(std::move(res));
}

// ─── list ───────────────────────────────────────────────────────────────────

folly::Future< S3ListResult >
MockS3ObjectStore::list_objects(const std::string& prefix, const std::string& continuation_token,
                                uint32_t max_keys) {
    LOGDEBUG("MockS3: list_objects prefix={} token={} max_keys={}", prefix, continuation_token, max_keys);

    // Collect all matching keys, sorted for deterministic pagination.
    std::vector< std::string > matching;
    {
        std::lock_guard lg(m_mtx);
        for (const auto& [key, blob] : m_objects) {
            if (key.rfind(prefix, 0) == 0) { matching.push_back(key); }
        }
    }
    std::sort(matching.begin(), matching.end());

    // Find start position based on continuation token.
    auto start_it = matching.begin();
    if (!continuation_token.empty()) {
        start_it = std::upper_bound(matching.begin(), matching.end(), continuation_token);
    }

    uint32_t limit = (max_keys > 0) ? max_keys : static_cast< uint32_t >(matching.size());

    S3ListResult lr;
    lr.result = make_ok();
    uint32_t count = 0;
    for (auto it = start_it; it != matching.end() && count < limit; ++it, ++count) {
        std::lock_guard lg(m_mtx);
        auto obj_it = m_objects.find(*it);
        if (obj_it == m_objects.end()) continue;
        S3ObjectInfo info;
        info.key = *it;
        info.size = obj_it->second.size();
        lr.objects.push_back(std::move(info));
    }

    // Check if there are more results.
    if (start_it != matching.end() &&
        std::distance(start_it, matching.end()) > static_cast< ptrdiff_t >(limit)) {
        lr.truncated = true;
        lr.next_continuation_token = lr.objects.back().key;
    }

    return folly::makeFuture(std::move(lr));
}

// ─── copy ───────────────────────────────────────────────────────────────────

folly::Future< S3Result > MockS3ObjectStore::copy_object(const std::string& src_key, const std::string& dst_key) {
    LOGDEBUG("MockS3: copy_object src={} dst={}", src_key, dst_key);

    std::lock_guard lg(m_mtx);
    auto it = m_objects.find(src_key);
    if (it == m_objects.end()) { return folly::makeFuture(make_not_found(src_key)); }

    const auto& src = it->second;
    sisl::io_blob_safe copy(src.size(), 0);
    std::memcpy(copy.bytes(), src.cbytes(), src.size());
    m_objects.insert_or_assign(dst_key, std::move(copy));

    return folly::makeFuture(make_ok());
}

// ─── test helpers ───────────────────────────────────────────────────────────

size_t MockS3ObjectStore::object_count() const {
    std::lock_guard lg(m_mtx);
    return m_objects.size();
}

bool MockS3ObjectStore::has_object(const std::string& key) const {
    std::lock_guard lg(m_mtx);
    return m_objects.count(key) > 0;
}

void MockS3ObjectStore::clear() {
    std::lock_guard lg(m_mtx);
    m_objects.clear();
}

} // namespace homestore
