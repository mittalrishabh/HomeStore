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
#include <atomic>
#include <cstring>
#include <string>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/s3_physical_dev.h>
#include "s3/s3_object_store_impl.h"

using namespace homestore;

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

// ─── Helpers ────────────────────────────────────────────────────────────────

static sisl::byte_array make_data(const std::string& content) {
    auto blob = std::make_shared< sisl::io_blob_safe >(content.size(), 0);
    std::memcpy(blob->bytes(), content.data(), content.size());
    return blob;
}

static std::string data_to_string(const sisl::byte_array& data) {
    return std::string(reinterpret_cast< const char* >(data->cbytes()), data->size());
}

// ─── Test fixture ───────────────────────────────────────────────────────────

class S3PhysicalDevTest : public ::testing::Test {
protected:
    void SetUp() override {
        S3ObjectStoreConfig cfg;
        cfg.bucket = "test-bucket";
        cfg.region = "us-east-1";
        m_mock_s3 = std::make_shared< MockS3ObjectStore >(cfg);
        m_pdev = std::make_unique< S3PhysicalDev >(m_mock_s3, 64 /* 64MB dirty cache limit */);
    }

    void TearDown() override { m_pdev.reset(); }

    std::shared_ptr< MockS3ObjectStore > m_mock_s3;
    std::unique_ptr< S3PhysicalDev > m_pdev;
};

// ─── Basic write + drain round-trip ────────────────────────────────────────

TEST_F(S3PhysicalDevTest, WriteThenDrain) {
    const chunk_id_t cid = 42;
    m_pdev->create_chunk(cid, 1024 * 1024);

    // Write two blocks to the same chunk
    auto ec1 = m_pdev->write(cid, 0, make_data("block0")).get();
    EXPECT_FALSE(ec1);

    auto ec2 = m_pdev->write(cid, 4096, make_data("block1")).get();
    EXPECT_FALSE(ec2);

    EXPECT_TRUE(m_pdev->has_dirty_data());
    EXPECT_EQ(m_pdev->get_dirty_chunk_ids().size(), 1);
    EXPECT_TRUE(m_pdev->get_dirty_chunk_ids().count(cid));

    // Drain
    auto blocks = m_pdev->drain_dirty_cache(cid);
    EXPECT_EQ(blocks.size(), 2);
    EXPECT_EQ(blocks[0].offset, 0);
    EXPECT_EQ(data_to_string(blocks[0].data), "block0");
    EXPECT_EQ(blocks[1].offset, 4096);
    EXPECT_EQ(data_to_string(blocks[1].data), "block1");

    // After drain, no dirty data
    EXPECT_FALSE(m_pdev->has_dirty_data());
    EXPECT_EQ(m_pdev->dirty_cache_bytes(), 0);
}

// ─── Drain empty chunk returns empty ───────────────────────────────────────

TEST_F(S3PhysicalDevTest, DrainEmpty) {
    auto blocks = m_pdev->drain_dirty_cache(999);
    EXPECT_TRUE(blocks.empty());
}

// ─── Chunk registration ────────────────────────────────────────────────────

TEST_F(S3PhysicalDevTest, ChunkRegistration) {
    EXPECT_FALSE(m_pdev->has_chunk(10));
    EXPECT_EQ(m_pdev->num_chunks(), 0);

    m_pdev->create_chunk(10, 1024 * 1024);
    EXPECT_TRUE(m_pdev->has_chunk(10));
    EXPECT_EQ(m_pdev->num_chunks(), 1);

    m_pdev->create_chunk(20, 2 * 1024 * 1024);
    EXPECT_EQ(m_pdev->num_chunks(), 2);

    m_pdev->remove_chunk(10);
    EXPECT_FALSE(m_pdev->has_chunk(10));
    EXPECT_EQ(m_pdev->num_chunks(), 1);
}

// ─── Remove chunk clears its dirty cache ───────────────────────────────────

TEST_F(S3PhysicalDevTest, RemoveChunkClearsDirtyCache) {
    m_pdev->create_chunk(5, 1024);
    m_pdev->write(5, 0, make_data("dirty")).get();
    EXPECT_GT(m_pdev->dirty_cache_bytes(), 0);

    m_pdev->remove_chunk(5);
    EXPECT_EQ(m_pdev->dirty_cache_bytes(), 0);
    EXPECT_FALSE(m_pdev->has_dirty_data());
}

// ─── Dirty cache byte tracking ─────────────────────────────────────────────

TEST_F(S3PhysicalDevTest, DirtyCacheByteTracking) {
    m_pdev->create_chunk(1, 1024 * 1024);

    EXPECT_EQ(m_pdev->dirty_cache_bytes(), 0);

    m_pdev->write(1, 0, make_data("aaaa")).get();     // +4
    EXPECT_EQ(m_pdev->dirty_cache_bytes(), 4);

    m_pdev->write(1, 100, make_data("bbbbbb")).get();  // +6
    EXPECT_EQ(m_pdev->dirty_cache_bytes(), 10);

    auto blocks = m_pdev->drain_dirty_cache(1);
    EXPECT_EQ(m_pdev->dirty_cache_bytes(), 0);
}

// ─── Multiple chunks dirty ─────────────────────────────────────────────────

TEST_F(S3PhysicalDevTest, MultipleChunksDirty) {
    m_pdev->create_chunk(1, 1024);
    m_pdev->create_chunk(2, 1024);
    m_pdev->create_chunk(3, 1024);

    m_pdev->write(1, 0, make_data("a")).get();
    m_pdev->write(3, 0, make_data("c")).get();

    auto dirty_ids = m_pdev->get_dirty_chunk_ids();
    EXPECT_EQ(dirty_ids.size(), 2);
    EXPECT_TRUE(dirty_ids.count(1));
    EXPECT_FALSE(dirty_ids.count(2)); // chunk 2 not dirty
    EXPECT_TRUE(dirty_ids.count(3));

    // Drain chunk 1 only
    m_pdev->drain_dirty_cache(1);
    dirty_ids = m_pdev->get_dirty_chunk_ids();
    EXPECT_EQ(dirty_ids.size(), 1);
    EXPECT_TRUE(dirty_ids.count(3));
}

// ─── Early CP flush callback ───────────────────────────────────────────────

TEST_F(S3PhysicalDevTest, EarlyCPFlush) {
    // Create a pdev with a very small dirty cache limit (1 byte)
    m_pdev = std::make_unique< S3PhysicalDev >(m_mock_s3, 0 /* 0 MB = triggers on any write */);

    // Use atomic to avoid capture-by-ref issues.
    // Set limit to something extremely small — we can't set 0MB so let's use 1 byte effectively
    // Actually 0 MB = 0 bytes limit, so ANY write will trigger.
    std::atomic< int > flush_count{0};
    m_pdev->set_early_cp_flush_cb([&flush_count]() { flush_count.fetch_add(1); });

    m_pdev->create_chunk(1, 1024);
    m_pdev->write(1, 0, make_data("x")).get();

    EXPECT_GE(flush_count.load(), 1);
}

// ─── Read without ChunkStore attached ──────────────────────────────────────

TEST_F(S3PhysicalDevTest, ReadWithoutChunkStore) {
    m_pdev->create_chunk(1, 1024);

    auto [ec, data] = m_pdev->read(1, 0, 100).get();
    EXPECT_TRUE(ec.operator bool()); // should fail — no ChunkStore
}

// ─── Write is near-instant (no S3 I/O) ────────────────────────────────────

TEST_F(S3PhysicalDevTest, WriteDoesNotTouchS3) {
    m_pdev->create_chunk(1, 1024 * 1024);

    m_pdev->write(1, 0, make_data("no-s3-io")).get();
    m_pdev->write(1, 4096, make_data("still-no-s3")).get();

    // Mock S3 should have zero puts — writes only go to dirty cache
    EXPECT_EQ(m_mock_s3->total_puts(), 0);
    EXPECT_EQ(m_mock_s3->total_gets(), 0);
}

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_physical_dev");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%t] %v");

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
