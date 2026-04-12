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

#include <string>
#include <stdexcept>

#include <sisl/logging/logging.h>
#include "common/homestore_config.hpp"
#include <homestore/s3/s3_object_store.h>

namespace homestore {

/// Helper that reads the S3 section from HomeStore's flatbuffers config,
/// applies defaults for non-scalar fields, validates constraints, and
/// produces an S3ObjectStoreConfig ready for constructing an S3ObjectStore.
struct S3Config {
    /// Whether the S3 layer is enabled at all.
    static bool is_enabled() { return HS_DYNAMIC_CONFIG(s3.enabled); }

    /// Build bucket name: "<prefix>-<cluster_id>"
    static std::string bucket_name() {
        auto prefix = bucket_prefix();
        auto cid = cluster_id();
        return prefix + "-" + cid;
    }

    static std::string bucket_prefix() {
        auto v = std::string{HS_DYNAMIC_CONFIG(s3.bucket_prefix)};
        return v.empty() ? "homestore" : v;
    }

    static std::string cluster_id() { return std::string{HS_DYNAMIC_CONFIG(s3.cluster_id)}; }

    static std::string region() { return std::string{HS_DYNAMIC_CONFIG(s3.region)}; }

    static std::string endpoint() { return std::string{HS_DYNAMIC_CONFIG(s3.endpoint)}; }

    static uint32_t upload_concurrency() { return HS_DYNAMIC_CONFIG(s3.upload_concurrency); }

    static uint32_t retry_count() { return HS_DYNAMIC_CONFIG(s3.retry_count); }

    static uint32_t retry_backoff_ms() { return HS_DYNAMIC_CONFIG(s3.retry_backoff_ms); }

    static std::string chunk_store_backend() {
        auto v = std::string{HS_DYNAMIC_CONFIG(s3.chunk_store_backend)};
        return v.empty() ? "full" : v;
    }

    static uint32_t dirty_cache_max_mb() { return HS_DYNAMIC_CONFIG(s3.dirty_cache_max_mb); }

    /// Validate config at startup. Throws on invalid configuration.
    static void validate() {
        if (!is_enabled()) return; // nothing to validate if disabled

        if (cluster_id().empty()) { throw std::invalid_argument("s3.cluster_id must be set when s3.enabled=true"); }

        auto backend = chunk_store_backend();
        if (backend != "full" && backend != "sst") {
            throw std::invalid_argument("s3.chunk_store_backend must be 'full' or 'sst', got: " + backend);
        }

        if (retry_count() == 0) { throw std::invalid_argument("s3.retry_count must be > 0"); }

        if (dirty_cache_max_mb() == 0) { throw std::invalid_argument("s3.dirty_cache_max_mb must be > 0"); }

        LOGINFO("S3 config validated: bucket={} region={} endpoint={} backend={} upload_concurrency={} "
                "retry_count={} retry_backoff_ms={} dirty_cache_max_mb={}",
                bucket_name(), region(), endpoint(), backend, upload_concurrency(), retry_count(), retry_backoff_ms(),
                dirty_cache_max_mb());
    }

    /// Produce an S3ObjectStoreConfig from the validated settings.
    static S3ObjectStoreConfig to_object_store_config() {
        S3ObjectStoreConfig cfg;
        cfg.bucket = bucket_name();
        cfg.region = region();
        cfg.endpoint = endpoint();
        cfg.retry_count = retry_count();
        cfg.retry_backoff_ms = retry_backoff_ms();
        return cfg;
    }
};

} // namespace homestore
