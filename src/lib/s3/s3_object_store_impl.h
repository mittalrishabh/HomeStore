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
#pragma once

#include <atomic>
#include <mutex>
#include <unordered_map>

#include <homestore/s3/s3_object_store.h>

namespace homestore {

/// In-memory mock implementation of S3ObjectStore for unit testing.
///
/// Stores objects in a std::unordered_map. Thread-safe. No network calls.
class MockS3ObjectStore : public S3ObjectStore {
public:
    explicit MockS3ObjectStore(const S3ObjectStoreConfig& cfg);
    ~MockS3ObjectStore() override = default;

    folly::Future< S3Result > put_object(const std::string& key, sisl::io_blob_safe data) override;
    folly::Future< std::pair< S3Result, sisl::io_blob_safe > > get_object(const std::string& key) override;
    folly::Future< std::pair< S3Result, sisl::io_blob_safe > > get_object_range(const std::string& key,
                                                                                 uint64_t offset,
                                                                                 uint64_t length) override;
    folly::Future< S3Result > delete_object(const std::string& key) override;
    folly::Future< S3Result > delete_objects(const std::vector< std::string >& keys) override;
    folly::Future< S3Result > head_object(const std::string& key) override;
    folly::Future< S3ListResult >
    list_objects(const std::string& prefix, const std::string& continuation_token = {},
                 uint32_t max_keys = 0) override;
    const std::string& bucket_name() const override { return m_cfg.bucket; }
    folly::Future< S3Result > copy_object(const std::string& src_key, const std::string& dst_key) override;

    /// Test helpers
    size_t object_count() const;
    bool has_object(const std::string& key) const;
    void clear();

    /// Stats
    uint64_t total_puts() const { return m_total_puts.load(std::memory_order_relaxed); }
    uint64_t total_gets() const { return m_total_gets.load(std::memory_order_relaxed); }
    uint64_t total_deletes() const { return m_total_deletes.load(std::memory_order_relaxed); }

private:
    S3ObjectStoreConfig m_cfg;

    mutable std::mutex m_mtx;
    std::unordered_map< std::string, sisl::io_blob_safe > m_objects;

    std::atomic< uint64_t > m_total_puts{0};
    std::atomic< uint64_t > m_total_gets{0};
    std::atomic< uint64_t > m_total_deletes{0};

    static S3Result make_ok();
    static S3Result make_not_found(const std::string& key);
};

} // namespace homestore
