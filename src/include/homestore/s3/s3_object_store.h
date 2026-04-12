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

#include <cstdint>
#include <string>
#include <vector>

#include <folly/futures/Future.h>
#include <sisl/logging/logging.h>
#include <sisl/fds/buffer.hpp>

SISL_LOGGING_DECL(s3)

namespace homestore {

/// Result of an S3 operation — wraps a status code plus optional metadata.
struct S3Result {
    int status_code{0};           ///< HTTP status code (200, 404, etc.) or 0 on success
    std::string error_message;    ///< Human-readable error (empty on success)
    uint64_t content_length{0};   ///< Populated by head_object / get_object
    std::string etag;             ///< ETag returned by PUT / HEAD
    bool exists{false};           ///< Populated by head_object

    bool ok() const { return status_code == 0 || (status_code >= 200 && status_code < 300); }
};

/// Metadata about a listed S3 object.
struct S3ObjectInfo {
    std::string key;
    uint64_t size{0};
    std::string etag;
};

/// Result of a paginated list_objects call.
struct S3ListResult {
    S3Result result;
    std::vector< S3ObjectInfo > objects;
    bool truncated{false};                  ///< true if more results available
    std::string next_continuation_token;    ///< pass to next call to resume
};

/// Configuration needed to connect to an S3 (or S3-compatible) endpoint.
struct S3ObjectStoreConfig {
    std::string bucket;           ///< Full bucket name, e.g. "homestore-<cluster_id>"
    std::string region;           ///< AWS region
    std::string endpoint;         ///< Optional — for MinIO / LocalStack / etc.
    uint32_t retry_count{3};      ///< Retries per operation
    uint32_t retry_backoff_ms{1000}; ///< Initial exponential backoff
};

/// Low-level S3 abstraction — nothing HomeStore-specific.
///
/// All operations are async and return folly::Future. Operations log latency
/// and retry transient failures (5xx, timeouts) with exponential backoff.
///
/// This is a pure interface; concrete implementations (AWS SDK, mock) are
/// injected at construction time.
class S3ObjectStore {
public:
    virtual ~S3ObjectStore() = default;

    /// Upload `data` to `key`. Verifies upload via Content-MD5.
    virtual folly::Future< S3Result > put_object(const std::string& key, sisl::io_blob_safe data) = 0;

    /// Download the full object at `key`.
    virtual folly::Future< std::pair< S3Result, sisl::io_blob_safe > > get_object(const std::string& key) = 0;

    /// Partial read: download `length` bytes starting at `offset`.
    virtual folly::Future< std::pair< S3Result, sisl::io_blob_safe > >
    get_object_range(const std::string& key, uint64_t offset, uint64_t length) = 0;

    /// Delete a single object.
    virtual folly::Future< S3Result > delete_object(const std::string& key) = 0;

    /// Batch delete (for compaction cleanup).
    virtual folly::Future< S3Result > delete_objects(const std::vector< std::string >& keys) = 0;

    /// Check if object exists and get size/etag.
    virtual folly::Future< S3Result > head_object(const std::string& key) = 0;

    /// List objects under `prefix`, with optional pagination.
    /// @param prefix        Key prefix to filter by
    /// @param continuation_token  Resume token from a previous truncated response (empty = start)
    /// @param max_keys      Max objects to return per call (0 = S3 default, typically 1000)
    virtual folly::Future< S3ListResult >
    list_objects(const std::string& prefix, const std::string& continuation_token = {},
                 uint32_t max_keys = 0) = 0;

    /// Convenience: bucket this store operates on.
    virtual const std::string& bucket_name() const = 0;

    /// Server-side copy (for clone operations).
    virtual folly::Future< S3Result > copy_object(const std::string& src_key, const std::string& dst_key) = 0;
};

} // namespace homestore
