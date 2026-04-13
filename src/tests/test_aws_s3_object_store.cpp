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
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/aws_s3_object_store.h>

using namespace homestore;

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

static std::string gen_unique_prefix() {
    static std::atomic< uint64_t > counter{0};
    auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
    return "test-" + std::to_string(ts) + "-" + std::to_string(counter.fetch_add(1)) + "/";
}

class AwsS3ObjectStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        S3ObjectStoreConfig cfg;
        cfg.bucket = "homestore-test";
        cfg.region = "us-east-1";
        cfg.endpoint = "http://localhost:9000";
        cfg.retry_count = 2;
        cfg.retry_backoff_ms = 100;
        m_store = std::make_unique< AwsS3ObjectStore >(cfg);
        m_prefix = gen_unique_prefix();
    }

    void TearDown() override {
        // Best-effort cleanup of objects created during test
        auto lr = m_store->list_objects(m_prefix).get();
        if (lr.result.ok()) {
            std::vector< std::string > keys;
            for (const auto& obj : lr.objects) {
                keys.push_back(obj.key);
            }
            if (!keys.empty()) { m_store->delete_objects(keys).get(); }
        }
        m_store.reset();
    }

    std::string prefixed(const std::string& key) { return m_prefix + key; }

    sisl::io_blob_safe make_blob(const std::string& content) {
        sisl::io_blob_safe blob(content.size(), 0);
        std::memcpy(blob.bytes(), content.data(), content.size());
        return blob;
    }

    sisl::io_blob_safe make_random_blob(size_t size) {
        sisl::io_blob_safe blob(size, 0);
        std::mt19937 rng(42);
        auto* p = reinterpret_cast< uint32_t* >(blob.bytes());
        for (size_t i = 0; i < size / sizeof(uint32_t); ++i) {
            p[i] = rng();
        }
        // fill remainder bytes
        auto* tail = blob.bytes() + (size / sizeof(uint32_t)) * sizeof(uint32_t);
        for (size_t i = 0; i < size % sizeof(uint32_t); ++i) {
            tail[i] = static_cast< uint8_t >(rng() & 0xFF);
        }
        return blob;
    }

    std::string blob_to_string(const sisl::io_blob_safe& blob) {
        return std::string(reinterpret_cast< const char* >(blob.cbytes()), blob.size());
    }

    std::unique_ptr< AwsS3ObjectStore > m_store;
    std::string m_prefix;
};

// ─── PUT / GET round-trip ──────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, PutGetRoundTrip) {
    auto key = prefixed("hello.dat");
    auto put_res = m_store->put_object(key, make_blob("hello world")).get();
    ASSERT_TRUE(put_res.ok()) << put_res.error_message;
    EXPECT_EQ(put_res.content_length, 11);

    auto [get_res, got] = m_store->get_object(key).get();
    ASSERT_TRUE(get_res.ok()) << get_res.error_message;
    EXPECT_EQ(blob_to_string(got), "hello world");
}

// ─── GET non-existent key returns error ────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, GetMissing) {
    auto [res, blob] = m_store->get_object(prefixed("does-not-exist")).get();
    EXPECT_FALSE(res.ok());
}

// ─── Range read ────────────────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, RangeRead) {
    auto key = prefixed("range.dat");
    m_store->put_object(key, make_blob("0123456789")).get();

    auto [res, blob] = m_store->get_object_range(key, 3, 4).get();
    ASSERT_TRUE(res.ok()) << res.error_message;
    EXPECT_EQ(blob_to_string(blob), "3456");
}

// ─── DELETE single ─────────────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, DeleteSingle) {
    auto key = prefixed("del-single.dat");
    m_store->put_object(key, make_blob("data")).get();

    auto head1 = m_store->head_object(key).get();
    ASSERT_TRUE(head1.ok());
    ASSERT_TRUE(head1.exists);

    auto del_res = m_store->delete_object(key).get();
    ASSERT_TRUE(del_res.ok()) << del_res.error_message;

    auto head2 = m_store->head_object(key).get();
    EXPECT_FALSE(head2.exists);
}

// ─── DELETE batch ──────────────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, DeleteBatch) {
    auto k1 = prefixed("batch-a");
    auto k2 = prefixed("batch-b");
    auto k3 = prefixed("batch-c");
    m_store->put_object(k1, make_blob("1")).get();
    m_store->put_object(k2, make_blob("2")).get();
    m_store->put_object(k3, make_blob("3")).get();

    auto res = m_store->delete_objects({k1, k3}).get();
    ASSERT_TRUE(res.ok()) << res.error_message;

    EXPECT_FALSE(m_store->head_object(k1).get().exists);
    EXPECT_TRUE(m_store->head_object(k2).get().exists);
    EXPECT_FALSE(m_store->head_object(k3).get().exists);
}

// ─── HEAD ──────────────────────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, HeadExists) {
    auto key = prefixed("head-exists.dat");
    m_store->put_object(key, make_blob("payload")).get();

    auto res = m_store->head_object(key).get();
    ASSERT_TRUE(res.ok()) << res.error_message;
    EXPECT_TRUE(res.exists);
    EXPECT_EQ(res.content_length, 7);
}

TEST_F(AwsS3ObjectStoreTest, HeadMissing) {
    auto res = m_store->head_object(prefixed("no-such-key")).get();
    EXPECT_FALSE(res.exists);
}

// ─── LIST ──────────────────────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, ListByPrefix) {
    m_store->put_object(prefixed("list/a"), make_blob("1")).get();
    m_store->put_object(prefixed("list/b"), make_blob("2")).get();
    m_store->put_object(prefixed("other/c"), make_blob("3")).get();

    auto lr = m_store->list_objects(m_prefix + "list/").get();
    ASSERT_TRUE(lr.result.ok()) << lr.result.error_message;
    EXPECT_EQ(lr.objects.size(), 2);
}

TEST_F(AwsS3ObjectStoreTest, ListPaginated) {
    for (int i = 0; i < 5; ++i) {
        m_store->put_object(prefixed("pg/key" + std::to_string(i)), make_blob("v")).get();
    }

    auto page1 = m_store->list_objects(m_prefix + "pg/", {}, 2).get();
    ASSERT_TRUE(page1.result.ok());
    EXPECT_EQ(page1.objects.size(), 2);
    EXPECT_TRUE(page1.truncated);

    auto page2 = m_store->list_objects(m_prefix + "pg/", page1.next_continuation_token, 2).get();
    ASSERT_TRUE(page2.result.ok());
    EXPECT_EQ(page2.objects.size(), 2);
    EXPECT_TRUE(page2.truncated);

    auto page3 = m_store->list_objects(m_prefix + "pg/", page2.next_continuation_token, 2).get();
    ASSERT_TRUE(page3.result.ok());
    EXPECT_EQ(page3.objects.size(), 1);
    EXPECT_FALSE(page3.truncated);
}

// ─── COPY ──────────────────────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, CopyObject) {
    auto src = prefixed("copy-src");
    auto dst = prefixed("copy-dst");
    m_store->put_object(src, make_blob("original")).get();

    auto res = m_store->copy_object(src, dst).get();
    ASSERT_TRUE(res.ok()) << res.error_message;

    auto [get_res, blob] = m_store->get_object(dst).get();
    ASSERT_TRUE(get_res.ok());
    EXPECT_EQ(blob_to_string(blob), "original");
}

// ─── Bucket name accessor ──────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, BucketName) {
    EXPECT_EQ(m_store->bucket_name(), "homestore-test");
}

// ─── Error: wrong bucket ───────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, WrongBucketError) {
    S3ObjectStoreConfig bad_cfg;
    bad_cfg.bucket = "nonexistent-bucket-xyz";
    bad_cfg.region = "us-east-1";
    bad_cfg.endpoint = "http://localhost:9000";
    bad_cfg.retry_count = 0;

    auto bad_store = std::make_unique< AwsS3ObjectStore >(bad_cfg);
    auto res = bad_store->put_object("key", make_blob("data")).get();
    EXPECT_FALSE(res.ok());
}

// ─── Overwrite ─────────────────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, PutOverwrite) {
    auto key = prefixed("overwrite.dat");
    m_store->put_object(key, make_blob("v1")).get();
    m_store->put_object(key, make_blob("v2-longer")).get();

    auto [res, blob] = m_store->get_object(key).get();
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(blob_to_string(blob), "v2-longer");
}

// ─── Large object (1 MB+) ─────────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, LargeObject) {
    auto key = prefixed("large.dat");
    constexpr size_t SIZE = 1u << 20; // 1 MB
    auto data = make_random_blob(SIZE);

    // Keep a copy for verification
    sisl::io_blob_safe expected(SIZE, 0);
    std::memcpy(expected.bytes(), data.cbytes(), SIZE);

    auto put_res = m_store->put_object(key, std::move(data)).get();
    ASSERT_TRUE(put_res.ok()) << put_res.error_message;
    EXPECT_EQ(put_res.content_length, SIZE);

    auto [get_res, got] = m_store->get_object(key).get();
    ASSERT_TRUE(get_res.ok()) << get_res.error_message;
    ASSERT_EQ(got.size(), SIZE);
    EXPECT_EQ(std::memcmp(got.cbytes(), expected.cbytes(), SIZE), 0);
}

// ─── Range read on large object ────────────────────────────────────────────

TEST_F(AwsS3ObjectStoreTest, LargeObjectRangeRead) {
    auto key = prefixed("large-range.dat");
    constexpr size_t SIZE = 1u << 20;
    auto data = make_random_blob(SIZE);

    sisl::io_blob_safe expected(SIZE, 0);
    std::memcpy(expected.bytes(), data.cbytes(), SIZE);

    m_store->put_object(key, std::move(data)).get();

    constexpr uint64_t OFFSET = 512 * 1024;
    constexpr uint64_t LENGTH = 4096;
    auto [res, blob] = m_store->get_object_range(key, OFFSET, LENGTH).get();
    ASSERT_TRUE(res.ok()) << res.error_message;
    ASSERT_EQ(blob.size(), LENGTH);
    EXPECT_EQ(std::memcmp(blob.cbytes(), expected.cbytes() + OFFSET, LENGTH), 0);
}

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_aws_s3_object_store");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%t] %v");

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
