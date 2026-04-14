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
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_gc.h>
#include <homestore/s3/s3_object_store.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////////

static S3ObjectStoreConfig make_test_config() {
    S3ObjectStoreConfig cfg;
    cfg.bucket = "homestore-test-gc";
    cfg.region = "us-east-1";
    cfg.retry_count = 1;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

static void upload_dummy(MockS3ObjectStore& store, const std::string& key) {
    sisl::io_blob_safe blob{64};
    std::memset(blob.bytes(), 0xAB, 64);
    auto result = store.put_object(key, std::move(blob)).get();
    ASSERT_TRUE(result.ok());
}

static PdevS3Superblock::SnapshotRecord make_snapshot(
    uint64_t snap_id, uint64_t gen,
    const std::vector< std::pair< uint64_t, std::string > >& chunk_keys) {

    PdevS3Superblock::SnapshotRecord snap;
    snap.header.snap_id = snap_id;
    snap.header.generation = gen;
    snap.header.btree_root_blkid = 0;
    snap.header.num_chunk_keys = static_cast< uint32_t >(chunk_keys.size());

    for (const auto& [cid, key] : chunk_keys) {
        s3_snapshot_chunk_key ck;
        ck.chunk_id = cid;
        ck.set_s3_key(key);
        snap.chunk_keys.push_back(ck);
    }
    return snap;
}

///////////////////////////////////////////////////////////////////////////////
// Test Fixture
///////////////////////////////////////////////////////////////////////////////
class S3GcTest : public ::testing::Test {
protected:
    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_superblock = std::make_unique< PdevS3Superblock >();
        m_superblock->set_pdev_id(1);
        m_superblock->set_generation(10);
    }

    std::unique_ptr< S3GarbageCollector > make_gc() {
        return std::make_unique< S3GarbageCollector >(m_superblock.get(), m_s3_store);
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::unique_ptr< PdevS3Superblock > m_superblock;
};

///////////////////////////////////////////////////////////////////////////////
// Tests
///////////////////////////////////////////////////////////////////////////////

TEST_F(S3GcTest, NoPendingCandidatesIsNoop) {
    auto gc = make_gc();
    EXPECT_EQ(gc->pending_count(), 0u);
    EXPECT_EQ(gc->run_gc(), 0u);
}

TEST_F(S3GcTest, DeletesUnpinnedObjects) {
    upload_dummy(*m_s3_store, "vol/chunks/1/data.dat");
    upload_dummy(*m_s3_store, "vol/chunks/2/data.dat");
    ASSERT_EQ(m_s3_store->object_count(), 2u);

    auto gc = make_gc();
    gc->on_generation_change({"vol/chunks/1/data.dat", "vol/chunks/2/data.dat"});
    EXPECT_EQ(gc->pending_count(), 2u);

    auto deleted = gc->run_gc();
    EXPECT_EQ(deleted, 2u);
    EXPECT_EQ(gc->pending_count(), 0u);

    EXPECT_FALSE(m_s3_store->has_object("vol/chunks/1/data.dat"));
    EXPECT_FALSE(m_s3_store->has_object("vol/chunks/2/data.dat"));
}

TEST_F(S3GcTest, RetainsPinnedObjects) {
    upload_dummy(*m_s3_store, "vol/chunks/1/data.dat");
    upload_dummy(*m_s3_store, "vol/chunks/2/data.dat");

    m_superblock->add_snapshot(make_snapshot(100, 5, {
        {1, "vol/chunks/1/data.dat"},
    }));

    auto gc = make_gc();
    gc->on_generation_change({"vol/chunks/1/data.dat", "vol/chunks/2/data.dat"});

    auto deleted = gc->run_gc();
    EXPECT_EQ(deleted, 1u);

    EXPECT_TRUE(m_s3_store->has_object("vol/chunks/1/data.dat"));
    EXPECT_FALSE(m_s3_store->has_object("vol/chunks/2/data.dat"));
}

TEST_F(S3GcTest, AllObjectsPinnedNothingDeleted) {
    upload_dummy(*m_s3_store, "vol/chunks/1/data.dat");
    upload_dummy(*m_s3_store, "vol/chunks/2/data.dat");

    m_superblock->add_snapshot(make_snapshot(100, 5, {
        {1, "vol/chunks/1/data.dat"},
        {2, "vol/chunks/2/data.dat"},
    }));

    auto gc = make_gc();
    gc->on_generation_change({"vol/chunks/1/data.dat", "vol/chunks/2/data.dat"});

    auto deleted = gc->run_gc();
    EXPECT_EQ(deleted, 0u);
    EXPECT_TRUE(m_s3_store->has_object("vol/chunks/1/data.dat"));
    EXPECT_TRUE(m_s3_store->has_object("vol/chunks/2/data.dat"));
}

TEST_F(S3GcTest, MultipleSnapshotsPinningDifferentKeys) {
    upload_dummy(*m_s3_store, "vol/chunks/1/data.dat");
    upload_dummy(*m_s3_store, "vol/chunks/1/data_gen5.dat");
    upload_dummy(*m_s3_store, "vol/chunks/1/data_gen6.dat");
    upload_dummy(*m_s3_store, "vol/chunks/2/data.dat");

    m_superblock->add_snapshot(make_snapshot(100, 5, {
        {1, "vol/chunks/1/data.dat"},
        {2, "vol/chunks/2/data.dat"},
    }));
    m_superblock->add_snapshot(make_snapshot(200, 6, {
        {1, "vol/chunks/1/data_gen5.dat"},
    }));

    auto gc = make_gc();
    gc->on_generation_change({
        "vol/chunks/1/data.dat",
        "vol/chunks/1/data_gen5.dat",
        "vol/chunks/1/data_gen6.dat",
        "vol/chunks/2/data.dat",
    });

    auto deleted = gc->run_gc();
    EXPECT_EQ(deleted, 1u);

    EXPECT_TRUE(m_s3_store->has_object("vol/chunks/1/data.dat"));
    EXPECT_TRUE(m_s3_store->has_object("vol/chunks/1/data_gen5.dat"));
    EXPECT_FALSE(m_s3_store->has_object("vol/chunks/1/data_gen6.dat"));
    EXPECT_TRUE(m_s3_store->has_object("vol/chunks/2/data.dat"));
}

TEST_F(S3GcTest, AddGcCandidateIndividually) {
    upload_dummy(*m_s3_store, "vol/chunks/5/data.dat");

    auto gc = make_gc();
    gc->add_gc_candidate("vol/chunks/5/data.dat");
    EXPECT_EQ(gc->pending_count(), 1u);

    auto deleted = gc->run_gc();
    EXPECT_EQ(deleted, 1u);
    EXPECT_FALSE(m_s3_store->has_object("vol/chunks/5/data.dat"));
}

TEST_F(S3GcTest, ClearPending) {
    auto gc = make_gc();
    gc->on_generation_change({"a.dat", "b.dat", "c.dat"});
    EXPECT_EQ(gc->pending_count(), 3u);

    gc->clear_pending();
    EXPECT_EQ(gc->pending_count(), 0u);

    EXPECT_EQ(gc->run_gc(), 0u);
}

TEST_F(S3GcTest, IsKeyPinned) {
    m_superblock->add_snapshot(make_snapshot(100, 5, {
        {1, "vol/chunks/1/data.dat"},
    }));

    auto gc = make_gc();
    EXPECT_TRUE(gc->is_key_pinned("vol/chunks/1/data.dat"));
    EXPECT_FALSE(gc->is_key_pinned("vol/chunks/2/data.dat"));
    EXPECT_FALSE(gc->is_key_pinned("nonexistent.dat"));
}

TEST_F(S3GcTest, CollectPinnedKeys) {
    m_superblock->add_snapshot(make_snapshot(100, 5, {
        {1, "vol/chunks/1/data.dat"},
        {2, "vol/chunks/2/data.dat"},
    }));
    m_superblock->add_snapshot(make_snapshot(200, 6, {
        {1, "vol/chunks/1/data_gen5.dat"},
        {3, "vol/chunks/3/data_gen5.dat"},
    }));

    auto gc = make_gc();
    auto pinned = gc->collect_pinned_keys();

    EXPECT_EQ(pinned.size(), 4u);
    EXPECT_TRUE(pinned.count("vol/chunks/1/data.dat"));
    EXPECT_TRUE(pinned.count("vol/chunks/2/data.dat"));
    EXPECT_TRUE(pinned.count("vol/chunks/1/data_gen5.dat"));
    EXPECT_TRUE(pinned.count("vol/chunks/3/data_gen5.dat"));
}

TEST_F(S3GcTest, NoSnapshotsDeletesEverything) {
    upload_dummy(*m_s3_store, "vol/chunks/1/data.dat");
    upload_dummy(*m_s3_store, "vol/chunks/2/data_gen3.dat");
    upload_dummy(*m_s3_store, "vol/chunks/3/data_gen7.dat");

    auto gc = make_gc();
    gc->on_generation_change({
        "vol/chunks/1/data.dat",
        "vol/chunks/2/data_gen3.dat",
        "vol/chunks/3/data_gen7.dat",
    });

    auto deleted = gc->run_gc();
    EXPECT_EQ(deleted, 3u);
    EXPECT_EQ(m_s3_store->object_count(), 0u);
}

TEST_F(S3GcTest, DuplicateCandidatesDeduped) {
    upload_dummy(*m_s3_store, "vol/chunks/1/data.dat");

    auto gc = make_gc();
    gc->add_gc_candidate("vol/chunks/1/data.dat");
    gc->add_gc_candidate("vol/chunks/1/data.dat");
    gc->add_gc_candidate("vol/chunks/1/data.dat");
    EXPECT_EQ(gc->pending_count(), 1u);

    auto deleted = gc->run_gc();
    EXPECT_EQ(deleted, 1u);
}

TEST_F(S3GcTest, ConsecutiveGcRunsWork) {
    upload_dummy(*m_s3_store, "vol/chunks/1/data.dat");
    upload_dummy(*m_s3_store, "vol/chunks/2/data.dat");

    auto gc = make_gc();

    gc->add_gc_candidate("vol/chunks/1/data.dat");
    EXPECT_EQ(gc->run_gc(), 1u);

    gc->add_gc_candidate("vol/chunks/2/data.dat");
    EXPECT_EQ(gc->run_gc(), 1u);

    EXPECT_EQ(m_s3_store->object_count(), 0u);
}

TEST_F(S3GcTest, SnapshotDeletedBetweenGcRuns) {
    upload_dummy(*m_s3_store, "vol/chunks/1/data.dat");

    m_superblock->add_snapshot(make_snapshot(100, 5, {
        {1, "vol/chunks/1/data.dat"},
    }));

    auto gc = make_gc();
    gc->add_gc_candidate("vol/chunks/1/data.dat");

    // First GC: key is pinned, nothing deleted
    EXPECT_EQ(gc->run_gc(), 0u);
    EXPECT_TRUE(m_s3_store->has_object("vol/chunks/1/data.dat"));

    // Remove snapshot, re-add candidate, run GC again
    m_superblock->remove_snapshot(100);
    gc->add_gc_candidate("vol/chunks/1/data.dat");

    EXPECT_EQ(gc->run_gc(), 1u);
    EXPECT_FALSE(m_s3_store->has_object("vol/chunks/1/data.dat"));
}

///////////////////////////////////////////////////////////////////////////////
// Main
///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
