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
#include <memory>
#include <mutex>
#include <string>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include <homestore/s3/s3_object_store.h>

namespace homestore {

class AwsS3ObjectStore : public S3ObjectStore {
public:
    explicit AwsS3ObjectStore(const S3ObjectStoreConfig& cfg);
    ~AwsS3ObjectStore() override;

    AwsS3ObjectStore(const AwsS3ObjectStore&) = delete;
    AwsS3ObjectStore& operator=(const AwsS3ObjectStore&) = delete;

    folly::Future< S3Result > put_object(const std::string& key, sisl::io_blob_safe data) override;
    folly::Future< std::pair< S3Result, sisl::io_blob_safe > > get_object(const std::string& key) override;
    folly::Future< std::pair< S3Result, sisl::io_blob_safe > >
    get_object_range(const std::string& key, uint64_t offset, uint64_t length) override;
    folly::Future< S3Result > delete_object(const std::string& key) override;
    folly::Future< S3Result > delete_objects(const std::vector< std::string >& keys) override;
    folly::Future< S3Result > head_object(const std::string& key) override;
    folly::Future< S3ListResult >
    list_objects(const std::string& prefix, const std::string& continuation_token = {},
                 uint32_t max_keys = 0) override;
    const std::string& bucket_name() const override;
    folly::Future< S3Result > copy_object(const std::string& src_key, const std::string& dst_key) override;

private:
    template < typename Func >
    auto execute_with_retry(const std::string& op_name, Func&& func)
        -> decltype(func());

    static S3Result outcome_to_result(const Aws::S3::S3Error& error);

    static std::mutex s_sdk_mutex;
    static int s_ref_count;
    static bool s_sdk_initialized;
    static Aws::SDKOptions s_sdk_options;

    S3ObjectStoreConfig m_cfg;
    std::shared_ptr< Aws::S3::S3Client > m_client;
    std::unique_ptr< folly::CPUThreadPoolExecutor > m_executor;

    std::atomic< uint64_t > m_total_puts{0};
    std::atomic< uint64_t > m_total_gets{0};
    std::atomic< uint64_t > m_total_deletes{0};
};

} // namespace homestore
