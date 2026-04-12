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
#include <cstring>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/s3_object_store.h>
#include "s3/s3_object_store_impl.h"

using namespace homestore;

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

class MockS3ObjectStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        S3ObjectStoreConfig cfg;
        cfg.bucket = "test-bucket";
        cfg.region = "us-east-1";
        cfg.retry_count = 3;
        cfg.retry_backoff_ms = 100;
        m_store = std::make_unique< MockS3ObjectStore >(cfg);
    }

    void TearDown() override { m_store.reset(); }

    sisl::io_blob_safe make_blob(const std::string& content) {
        sisl::io_blob_safe blob(content.size(), 0);
        std::memcpy(blob.bytes(), content.data(), content.size());
        return blob;
    }

    std::string blob_to_string(const sisl::io_blob_safe& blob) {
        return std::string(reinterpret_cast< const char* >(blob.cbytes()), blob.size());
    }

    std::unique_ptr< MockS3ObjectStore > m_store;
};

// ─── Basic put / get round-trip ────────────────────────────────────────────

TEST_F(MockS3ObjectStoreTest, PutGetRoundTrip) {
    auto key = "vol1/chunks/42/data.dat";
    auto data = make_blob("hello world");

    auto put_res = m_store->put_object(key, std::move(data)).get();
    ASSERT_TRUE(put_res.ok()) << put_res.error_message;
    EXPECT_EQ(put_res.content_length, 11);

    auto [get_res, got] = m_store->get_object(key).get();
    ASSERT_TRUE(get_res.ok()) << get_res.error_message;
    EXPECT_EQ(blob_to_string(got), "hello world");
}

// ─── Get non-existent key returns 404 ──────────────────────────────────────

TEST_F(MockS3ObjectStoreTest, GetMissing) {
    auto [res, blob] = m_store->get_object("does-not-exist").get();
    EXPECT_FALSE(res.ok());
    EXPECT_EQ(res.status_code, 404);
}

// ─── Range read ────────────────────────────────────────────────────────────

TEST_F(MockS3ObjectStoreTest, RangeRead) {
    auto key = "range-test";
    m_store->put_object(key, make_blob("0123456789")).get();

    // Read bytes [3..7)
    auto [res, blob] = m_store->get_object_range(key, 3, 4).get();
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(blob_to_string(blob), "3456");
}

TEST_F(MockS3ObjectStoreTest, RangeReadBeyondEnd) {
    auto key = "range-clamp";
    m_store->put_object(key, make_blob("short")).get();

    // Request more than available
    auto [res, blob] = m_store->get_object_range(key, 2, 100).get();
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(blob_to_string(blob), "ort");
}

TEST_F(MockS3ObjectStoreTest, RangeReadInvalidOffset) {
    auto key = "range-invalid";
    m_store->put_object(key, make_blob("data")).get();

    auto [res, blob] = m_store->get_object_range(key, 999, 1).get();
    EXPECT_FALSE(res.ok());
    EXPECT_EQ(res.status_code, 416);
}

// ─── Delete (single and batch) ─────────────────────────────────────────────

TEST_F(MockS3ObjectStoreTest, DeleteSingle) {
    m_store->put_object("k1", make_blob("v1")).get();
    ASSERT_TRUE(m_store->has_object("k1"));

    auto res = m_store->delete_object("k1").get();
    ASSERT_TRUE(res.ok());
    EXPECT_FALSE(m_store->has_object("k1"));
}

TEST_F(MockS3ObjectStoreTest, DeleteBatch) {
    m_store->put_object("a", make_blob("1")).get();
    m_store->put_object("b", make_blob("2")).get();
    m_store->put_object("c", make_blob("3")).get();

    auto res = m_store->delete_objects({"a", "c"}).get();
    ASSERT_TRUE(res.ok());

    EXPECT_FALSE(m_store->has_object("a"));
    EXPECT_TRUE(m_store->has_object("b"));
    EXPECT_FALSE(m_store->has_object("c"));
}

// ─── Head ──────────────────────────────────────────────────────────────────

TEST_F(MockS3ObjectStoreTest, HeadExists) {
    m_store->put_object("exist", make_blob("payload")).get();
    auto res = m_store->head_object("exist").get();
    ASSERT_TRUE(res.ok());
    EXPECT_TRUE(res.exists);
    EXPECT_EQ(res.content_length, 7);
}

TEST_F(MockS3ObjectStoreTest, HeadMissing) {
    auto res = m_store->head_object("nope").get();
    EXPECT_FALSE(res.ok());
    EXPECT_FALSE(res.exists);
}

// ─── List ──────────────────────────────────────────────────────────────────

TEST_F(MockS3ObjectStoreTest, ListByPrefix) {
    m_store->put_object("vol1/chunks/1/data.dat", make_blob("a")).get();
    m_store->put_object("vol1/chunks/2/data.dat", make_blob("b")).get();
    m_store->put_object("vol2/chunks/1/data.dat", make_blob("c")).get();

    auto [res, objects] = m_store->list_objects("vol1/").get();
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(objects.size(), 2);
}

// ─── Copy ──────────────────────────────────────────────────────────────────

TEST_F(MockS3ObjectStoreTest, CopyObject) {
    m_store->put_object("src", make_blob("original")).get();

    auto res = m_store->copy_object("src", "dst").get();
    ASSERT_TRUE(res.ok());

    auto [get_res, blob] = m_store->get_object("dst").get();
    ASSERT_TRUE(get_res.ok());
    EXPECT_EQ(blob_to_string(blob), "original");
}

TEST_F(MockS3ObjectStoreTest, CopyMissingSrc) {
    auto res = m_store->copy_object("missing", "dst").get();
    EXPECT_FALSE(res.ok());
    EXPECT_EQ(res.status_code, 404);
}

// ─── Overwrite ─────────────────────────────────────────────────────────────

TEST_F(MockS3ObjectStoreTest, PutOverwrite) {
    m_store->put_object("key", make_blob("v1")).get();
    m_store->put_object("key", make_blob("v2-longer")).get();

    auto [res, blob] = m_store->get_object("key").get();
    ASSERT_TRUE(res.ok());
    EXPECT_EQ(blob_to_string(blob), "v2-longer");
    EXPECT_EQ(m_store->object_count(), 1);
}

// ─── Stats ─────────────────────────────────────────────────────────────────

TEST_F(MockS3ObjectStoreTest, Stats) {
    m_store->put_object("s1", make_blob("a")).get();
    m_store->put_object("s2", make_blob("b")).get();
    m_store->get_object("s1").get();
    m_store->delete_object("s2").get();

    EXPECT_EQ(m_store->total_puts(), 2);
    EXPECT_EQ(m_store->total_gets(), 1);
    EXPECT_EQ(m_store->total_deletes(), 1);
}

// ─── Clear ─────────────────────────────────────────────────────────────────

TEST_F(MockS3ObjectStoreTest, Clear) {
    m_store->put_object("a", make_blob("1")).get();
    m_store->put_object("b", make_blob("2")).get();
    EXPECT_EQ(m_store->object_count(), 2);

    m_store->clear();
    EXPECT_EQ(m_store->object_count(), 0);
}

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_object_store");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%t] %v");

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
