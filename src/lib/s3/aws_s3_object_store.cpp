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
#include <cstring>
#include <sstream>
#include <thread>

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <sisl/logging/logging.h>

#include "homestore/s3/aws_s3_object_store.h"

namespace homestore {

AwsS3ObjectStore::AwsS3ObjectStore(const S3ObjectStoreConfig& cfg) : m_cfg{cfg} {
    Aws::InitAPI(m_sdk_options);

    Aws::Client::ClientConfiguration client_cfg;
    client_cfg.region = Aws::String(m_cfg.region.begin(), m_cfg.region.end());

    if (!m_cfg.endpoint.empty()) {
        client_cfg.endpointOverride = Aws::String(m_cfg.endpoint.begin(), m_cfg.endpoint.end());
    }

    client_cfg.connectTimeoutMs = 5000;
    client_cfg.requestTimeoutMs = 30000;
    client_cfg.retryStrategy = nullptr; // we do our own retries

    const bool use_path_style = !m_cfg.endpoint.empty();

    m_client = std::make_shared< Aws::S3::S3Client >(
        Aws::MakeShared< Aws::Auth::DefaultAWSCredentialsProviderChain >("AwsS3ObjectStore"),
        client_cfg,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        use_path_style);

    m_executor = std::make_unique< folly::CPUThreadPoolExecutor >(4);

    LOGINFO("AwsS3ObjectStore created bucket={} region={} endpoint={} path_style={}",
            m_cfg.bucket, m_cfg.region, m_cfg.endpoint, use_path_style);
}

AwsS3ObjectStore::~AwsS3ObjectStore() {
    m_executor.reset();
    m_client.reset();
    Aws::ShutdownAPI(m_sdk_options);
}

template < typename Func >
auto AwsS3ObjectStore::execute_with_retry(const std::string& op_name, Func&& func)
    -> decltype(func()) {
    for (uint32_t attempt = 0; attempt <= m_cfg.retry_count; ++attempt) {
        auto result = func();

        using ResultT = decltype(result);
        bool should_retry = false;

        if constexpr (std::is_same_v< ResultT, S3Result >) {
            if (result.ok() || attempt == m_cfg.retry_count) return result;
            should_retry = (result.status_code >= 500 || result.status_code == 0);
        } else if constexpr (std::is_same_v< ResultT, std::pair< S3Result, sisl::io_blob_safe > >) {
            if (result.first.ok() || attempt == m_cfg.retry_count) return result;
            should_retry = (result.first.status_code >= 500 || result.first.status_code == 0);
        } else if constexpr (std::is_same_v< ResultT, S3ListResult >) {
            if (result.result.ok() || attempt == m_cfg.retry_count) return result;
            should_retry = (result.result.status_code >= 500 || result.result.status_code == 0);
        }

        if (!should_retry) return result;

        auto backoff_ms = m_cfg.retry_backoff_ms * (1u << attempt);
        LOGWARN("AwsS3: {} attempt {}/{} failed, retrying in {}ms", op_name, attempt + 1,
                m_cfg.retry_count + 1, backoff_ms);
        std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
    }
    // unreachable, but satisfy compiler
    return func();
}

S3Result AwsS3ObjectStore::outcome_to_result(const Aws::S3::S3Error& error) {
    S3Result r;
    r.status_code = static_cast< int >(error.GetResponseCode());
    if (r.status_code == 0) r.status_code = 500;
    r.error_message = std::string(error.GetMessage().c_str());
    return r;
}

// ─── put ────────────────────────────────────────────────────────────────────

folly::Future< S3Result > AwsS3ObjectStore::put_object(const std::string& key, sisl::io_blob_safe data) {
    auto data_ptr = std::make_shared< sisl::io_blob_safe >(std::move(data));

    return folly::via(m_executor.get(), [this, key, data_ptr]() {
        return execute_with_retry("put_object", [&]() -> S3Result {
            auto start = std::chrono::steady_clock::now();

            auto body = Aws::MakeShared< Aws::StringStream >("PutObjectBody");
            body->write(reinterpret_cast< const char* >(data_ptr->cbytes()),
                        static_cast< std::streamsize >(data_ptr->size()));

            auto md5 = Aws::Utils::HashingUtils::CalculateMD5(*body);
            body->seekg(0);

            Aws::S3::Model::PutObjectRequest req;
            req.SetBucket(Aws::String(m_cfg.bucket.begin(), m_cfg.bucket.end()));
            req.SetKey(Aws::String(key.begin(), key.end()));
            req.SetBody(body);
            req.SetContentLength(static_cast< long long >(data_ptr->size()));
            req.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(md5));

            auto outcome = m_client->PutObject(req);

            auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
                                  std::chrono::steady_clock::now() - start)
                                  .count();
            LOGDEBUG("AwsS3: put_object key={} size={} latency_us={}", key, data_ptr->size(), elapsed_us);

            if (!outcome.IsSuccess()) {
                return outcome_to_result(outcome.GetError());
            }

            m_total_puts.fetch_add(1, std::memory_order_relaxed);

            S3Result r;
            r.status_code = 200;
            r.content_length = data_ptr->size();
            r.etag = std::string(outcome.GetResult().GetETag().c_str());
            return r;
        });
    });
}

// ─── get (full) ─────────────────────────────────────────────────────────────

folly::Future< std::pair< S3Result, sisl::io_blob_safe > >
AwsS3ObjectStore::get_object(const std::string& key) {
    return folly::via(m_executor.get(), [this, key]() {
        return execute_with_retry("get_object", [&]() -> std::pair< S3Result, sisl::io_blob_safe > {
            auto start = std::chrono::steady_clock::now();

            Aws::S3::Model::GetObjectRequest req;
            req.SetBucket(Aws::String(m_cfg.bucket.begin(), m_cfg.bucket.end()));
            req.SetKey(Aws::String(key.begin(), key.end()));

            auto outcome = m_client->GetObject(req);

            auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
                                  std::chrono::steady_clock::now() - start)
                                  .count();
            LOGDEBUG("AwsS3: get_object key={} latency_us={}", key, elapsed_us);

            if (!outcome.IsSuccess()) {
                return {outcome_to_result(outcome.GetError()), sisl::io_blob_safe{}};
            }

            m_total_gets.fetch_add(1, std::memory_order_relaxed);

            auto& body = outcome.GetResult().GetBody();
            std::ostringstream ss;
            ss << body.rdbuf();
            auto str = ss.str();

            sisl::io_blob_safe blob(str.size(), 0);
            std::memcpy(blob.bytes(), str.data(), str.size());

            S3Result r;
            r.status_code = 200;
            r.content_length = str.size();
            r.etag = std::string(outcome.GetResult().GetETag().c_str());
            return {std::move(r), std::move(blob)};
        });
    });
}

// ─── get (range) ────────────────────────────────────────────────────────────

folly::Future< std::pair< S3Result, sisl::io_blob_safe > >
AwsS3ObjectStore::get_object_range(const std::string& key, uint64_t offset, uint64_t length) {
    return folly::via(m_executor.get(), [this, key, offset, length]() {
        return execute_with_retry("get_object_range",
                                  [&]() -> std::pair< S3Result, sisl::io_blob_safe > {
            auto start = std::chrono::steady_clock::now();

            Aws::S3::Model::GetObjectRequest req;
            req.SetBucket(Aws::String(m_cfg.bucket.begin(), m_cfg.bucket.end()));
            req.SetKey(Aws::String(key.begin(), key.end()));

            std::ostringstream range_str;
            range_str << "bytes=" << offset << "-" << (offset + length - 1);
            req.SetRange(Aws::String(range_str.str().c_str()));

            auto outcome = m_client->GetObject(req);

            auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
                                  std::chrono::steady_clock::now() - start)
                                  .count();
            LOGDEBUG("AwsS3: get_object_range key={} offset={} length={} latency_us={}",
                     key, offset, length, elapsed_us);

            if (!outcome.IsSuccess()) {
                return {outcome_to_result(outcome.GetError()), sisl::io_blob_safe{}};
            }

            m_total_gets.fetch_add(1, std::memory_order_relaxed);

            auto& body = outcome.GetResult().GetBody();
            std::ostringstream ss;
            ss << body.rdbuf();
            auto str = ss.str();

            sisl::io_blob_safe blob(str.size(), 0);
            std::memcpy(blob.bytes(), str.data(), str.size());

            S3Result r;
            r.status_code = 200;
            r.content_length = str.size();
            return {std::move(r), std::move(blob)};
        });
    });
}

// ─── delete (single) ───────────────────────────────────────────────────────

folly::Future< S3Result > AwsS3ObjectStore::delete_object(const std::string& key) {
    return folly::via(m_executor.get(), [this, key]() {
        return execute_with_retry("delete_object", [&]() -> S3Result {
            Aws::S3::Model::DeleteObjectRequest req;
            req.SetBucket(Aws::String(m_cfg.bucket.begin(), m_cfg.bucket.end()));
            req.SetKey(Aws::String(key.begin(), key.end()));

            auto outcome = m_client->DeleteObject(req);

            if (!outcome.IsSuccess()) {
                return outcome_to_result(outcome.GetError());
            }

            m_total_deletes.fetch_add(1, std::memory_order_relaxed);

            S3Result r;
            r.status_code = 200;
            return r;
        });
    });
}

// ─── delete (batch) ─────────────────────────────────────────────────────────

folly::Future< S3Result > AwsS3ObjectStore::delete_objects(const std::vector< std::string >& keys) {
    auto keys_copy = std::make_shared< std::vector< std::string > >(keys);

    return folly::via(m_executor.get(), [this, keys_copy]() {
        return execute_with_retry("delete_objects", [&]() -> S3Result {
            Aws::S3::Model::DeleteObjectsRequest req;
            req.SetBucket(Aws::String(m_cfg.bucket.begin(), m_cfg.bucket.end()));

            Aws::S3::Model::Delete del;
            for (const auto& k : *keys_copy) {
                Aws::S3::Model::ObjectIdentifier id;
                id.SetKey(Aws::String(k.begin(), k.end()));
                del.AddObjects(std::move(id));
            }
            del.SetQuiet(true);
            req.SetDelete(std::move(del));

            auto outcome = m_client->DeleteObjects(req);

            if (!outcome.IsSuccess()) {
                return outcome_to_result(outcome.GetError());
            }

            m_total_deletes.fetch_add(keys_copy->size(), std::memory_order_relaxed);

            S3Result r;
            r.status_code = 200;
            return r;
        });
    });
}

// ─── head ───────────────────────────────────────────────────────────────────

folly::Future< S3Result > AwsS3ObjectStore::head_object(const std::string& key) {
    return folly::via(m_executor.get(), [this, key]() {
        return execute_with_retry("head_object", [&]() -> S3Result {
            Aws::S3::Model::HeadObjectRequest req;
            req.SetBucket(Aws::String(m_cfg.bucket.begin(), m_cfg.bucket.end()));
            req.SetKey(Aws::String(key.begin(), key.end()));

            auto outcome = m_client->HeadObject(req);

            if (!outcome.IsSuccess()) {
                auto r = outcome_to_result(outcome.GetError());
                r.exists = false;
                return r;
            }

            S3Result r;
            r.status_code = 200;
            r.exists = true;
            r.content_length = static_cast< uint64_t >(outcome.GetResult().GetContentLength());
            r.etag = std::string(outcome.GetResult().GetETag().c_str());
            return r;
        });
    });
}

// ─── list ───────────────────────────────────────────────────────────────────

folly::Future< S3ListResult >
AwsS3ObjectStore::list_objects(const std::string& prefix, const std::string& continuation_token,
                                uint32_t max_keys) {
    return folly::via(m_executor.get(), [this, prefix, continuation_token, max_keys]() {
        return execute_with_retry("list_objects", [&]() -> S3ListResult {
            Aws::S3::Model::ListObjectsV2Request req;
            req.SetBucket(Aws::String(m_cfg.bucket.begin(), m_cfg.bucket.end()));
            req.SetPrefix(Aws::String(prefix.begin(), prefix.end()));

            if (!continuation_token.empty()) {
                req.SetContinuationToken(Aws::String(continuation_token.begin(), continuation_token.end()));
            }
            if (max_keys > 0) {
                req.SetMaxKeys(static_cast< int >(max_keys));
            }

            auto outcome = m_client->ListObjectsV2(req);

            if (!outcome.IsSuccess()) {
                S3ListResult lr;
                lr.result = outcome_to_result(outcome.GetError());
                return lr;
            }

            const auto& result = outcome.GetResult();
            S3ListResult lr;
            lr.result.status_code = 200;
            lr.truncated = result.GetIsTruncated();
            if (lr.truncated) {
                lr.next_continuation_token = std::string(result.GetNextContinuationToken().c_str());
            }

            for (const auto& obj : result.GetContents()) {
                S3ObjectInfo info;
                info.key = std::string(obj.GetKey().c_str());
                info.size = static_cast< uint64_t >(obj.GetSize());
                info.etag = std::string(obj.GetETag().c_str());
                lr.objects.push_back(std::move(info));
            }

            return lr;
        });
    });
}

// ─── bucket name ────────────────────────────────────────────────────────────

const std::string& AwsS3ObjectStore::bucket_name() const { return m_cfg.bucket; }

// ─── copy ───────────────────────────────────────────────────────────────────

folly::Future< S3Result > AwsS3ObjectStore::copy_object(const std::string& src_key, const std::string& dst_key) {
    return folly::via(m_executor.get(), [this, src_key, dst_key]() {
        return execute_with_retry("copy_object", [&]() -> S3Result {
            Aws::S3::Model::CopyObjectRequest req;
            req.SetBucket(Aws::String(m_cfg.bucket.begin(), m_cfg.bucket.end()));
            req.SetKey(Aws::String(dst_key.begin(), dst_key.end()));

            auto copy_source = m_cfg.bucket + "/" + src_key;
            req.SetCopySource(Aws::String(copy_source.begin(), copy_source.end()));

            auto outcome = m_client->CopyObject(req);

            if (!outcome.IsSuccess()) {
                return outcome_to_result(outcome.GetError());
            }

            S3Result r;
            r.status_code = 200;
            return r;
        });
    });
}

} // namespace homestore
