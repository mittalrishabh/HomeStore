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
#include <unordered_map>

#include <gtest/gtest.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/blk.h>
#include <homestore/btree/detail/btree_node.hpp>
#include <homestore/btree/detail/btree_crc32c.hpp>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/snapshot_reader.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// B+tree Node Builder — constructs valid on-disk B+tree nodes for testing
///////////////////////////////////////////////////////////////////////////////

static constexpr uint32_t BLOCK_SIZE = 4096;
static constexpr uint32_t NODE_SIZE = 8192;
static constexpr uint32_t KEY_SIZE = sizeof(uint64_t);
static constexpr uint32_t INTERIOR_VALUE_SIZE = sizeof(BtreeLinkInfo::bnode_link_info);
static constexpr uint32_t LEAF_VALUE_SIZE = sizeof(uint64_t);

struct TestEntry {
    uint64_t key;
    uint64_t value;  // BlkId integer for leaf, bnodeid_t for interior (child node_id)
    uint64_t link_version{0};  // only for interior nodes
};

static sisl::byte_array build_btree_node(
    bnodeid_t node_id,
    bool is_leaf,
    const std::vector< TestEntry >& entries,
    bnodeid_t edge_bnodeid = empty_bnodeid,
    uint64_t edge_link_version = 0,
    uint16_t level = 0
) {
    auto buf = sisl::make_byte_array(NODE_SIZE, 0);
    auto* hdr = reinterpret_cast< persistent_hdr_t* >(buf->bytes());

    hdr->magic = BTREE_NODE_MAGIC;
    hdr->version = BTREE_NODE_VERSION;
    hdr->nentries = static_cast< uint32_t >(entries.size());
    hdr->leaf = is_leaf ? 1 : 0;
    hdr->node_deleted = 0;
    hdr->node_id = node_id;
    hdr->next_node = empty_bnodeid;
    hdr->node_gen = 1;
    hdr->link_version = 1;
    hdr->edge_info.m_bnodeid = edge_bnodeid;
    hdr->edge_info.m_link_version = edge_link_version;
    hdr->modified_cp_id = 1;
    hdr->level = level;
    hdr->node_size = static_cast< uint16_t >(NODE_SIZE - 1);
    hdr->node_type = 0;

    auto* data_area = buf->bytes() + sizeof(persistent_hdr_t);
    uint32_t value_size = is_leaf ? LEAF_VALUE_SIZE : INTERIOR_VALUE_SIZE;
    uint32_t entry_size = KEY_SIZE + value_size;

    for (size_t i = 0; i < entries.size(); ++i) {
        auto* entry_ptr = data_area + (i * entry_size);
        std::memcpy(entry_ptr, &entries[i].key, KEY_SIZE);

        if (is_leaf) {
            std::memcpy(entry_ptr + KEY_SIZE, &entries[i].value, LEAF_VALUE_SIZE);
        } else {
            BtreeLinkInfo::bnode_link_info link;
            link.m_bnodeid = entries[i].value;
            link.m_link_version = entries[i].link_version;
            std::memcpy(entry_ptr + KEY_SIZE, &link, INTERIOR_VALUE_SIZE);
        }
    }

    hdr->checksum = bt_crc32c(bt_init_crc32c,
                              buf->cbytes() + sizeof(persistent_hdr_t),
                              NODE_SIZE - sizeof(persistent_hdr_t));

    return buf;
}

static BlkId make_data_blkid(chunk_num_t chunk, blk_num_t blk, blk_count_t nblks) {
    return BlkId{blk, nblks, chunk};
}

///////////////////////////////////////////////////////////////////////////////
// Test Fixture
///////////////////////////////////////////////////////////////////////////////
class SnapshotReaderTest : public ::testing::Test {
protected:
    static constexpr chunk_num_t INDEX_CHUNK = 5;
    static constexpr chunk_num_t DATA_CHUNK = 10;

    void SetUp() override {
        S3ObjectStoreConfig cfg;
        cfg.bucket = "homestore-test-snap-read";
        cfg.region = "us-east-1";
        cfg.retry_count = 1;
        cfg.retry_backoff_ms = 0;
        m_s3_store = std::make_shared< MockS3ObjectStore >(cfg);

        m_superblock = std::make_unique< PdevS3Superblock >();
        m_superblock->set_pdev_id(1);
        m_superblock->set_generation(10);

        m_index_s3_key = "vol-test/chunks/5/data.dat";
        m_data_s3_key = "vol-test/chunks/10/data.dat";
    }

    void add_snapshot_with_single_leaf(uint64_t snap_id, uint64_t lba, const BlkId& data_blkid) {
        auto leaf_node_id = static_cast< bnodeid_t >(
            BlkId{0, 1, INDEX_CHUNK}.to_integer());

        auto leaf = build_btree_node(leaf_node_id, true,
            {{lba, data_blkid.to_integer()}},
            empty_bnodeid, 0, 0);

        auto index_chunk_data = sisl::make_byte_array(BLOCK_SIZE * 10, 0);
        std::memcpy(index_chunk_data->bytes(), leaf->cbytes(), NODE_SIZE);
        upload_s3(m_index_s3_key, std::move(index_chunk_data));

        auto data_chunk_data = sisl::make_byte_array(BLOCK_SIZE * 10, 0);
        uint64_t data_offset = static_cast< uint64_t >(data_blkid.blk_num()) * BLOCK_SIZE;
        uint64_t data_size = static_cast< uint64_t >(data_blkid.blk_count()) * BLOCK_SIZE;
        std::memset(data_chunk_data->bytes() + data_offset, 0xDA, data_size);
        upload_s3(m_data_s3_key, std::move(data_chunk_data));

        PdevS3Superblock::SnapshotRecord snap;
        snap.header.snap_id = snap_id;
        snap.header.generation = 10;
        snap.header.btree_root_blkid = leaf_node_id;
        snap.header.num_chunk_keys = 2;

        s3_snapshot_chunk_key ck1;
        ck1.chunk_id = INDEX_CHUNK;
        ck1.set_s3_key(m_index_s3_key);
        snap.chunk_keys.push_back(ck1);

        s3_snapshot_chunk_key ck2;
        ck2.chunk_id = DATA_CHUNK;
        ck2.set_s3_key(m_data_s3_key);
        snap.chunk_keys.push_back(ck2);

        m_superblock->add_snapshot(snap);
    }

    void add_snapshot_with_two_level_tree(uint64_t snap_id,
                                         const std::vector< std::pair< uint64_t, BlkId > >& lba_to_blkid) {
        auto left_leaf_id = static_cast< bnodeid_t >(
            BlkId{0, 1, INDEX_CHUNK}.to_integer());
        auto right_leaf_id = static_cast< bnodeid_t >(
            BlkId{static_cast< blk_num_t >(NODE_SIZE / BLOCK_SIZE), 1, INDEX_CHUNK}.to_integer());
        auto root_id = static_cast< bnodeid_t >(
            BlkId{static_cast< blk_num_t >(2 * NODE_SIZE / BLOCK_SIZE), 1, INDEX_CHUNK}.to_integer());

        size_t mid = lba_to_blkid.size() / 2;

        std::vector< TestEntry > left_entries, right_entries;
        for (size_t i = 0; i < mid; ++i) {
            left_entries.push_back({lba_to_blkid[i].first, lba_to_blkid[i].second.to_integer()});
        }
        for (size_t i = mid; i < lba_to_blkid.size(); ++i) {
            right_entries.push_back({lba_to_blkid[i].first, lba_to_blkid[i].second.to_integer()});
        }

        auto left_leaf = build_btree_node(left_leaf_id, true, left_entries, empty_bnodeid, 0, 0);
        auto right_leaf = build_btree_node(right_leaf_id, true, right_entries, empty_bnodeid, 0, 0);

        // In HomeStore interior nodes, value[i] handles keys <= key[i], edge handles keys > key[n-1].
        // Split key = last key of left subtree, so left_leaf handles keys <= split_key.
        uint64_t split_key = lba_to_blkid[mid - 1].first;
        auto root_fixed = build_btree_node(root_id, false,
            {{split_key, left_leaf_id, 1}},
            right_leaf_id, 1, 1);

        auto index_chunk_data = sisl::make_byte_array(BLOCK_SIZE * 20, 0);
        std::memcpy(index_chunk_data->bytes(), left_leaf->cbytes(), NODE_SIZE);
        std::memcpy(index_chunk_data->bytes() + NODE_SIZE, right_leaf->cbytes(), NODE_SIZE);
        std::memcpy(index_chunk_data->bytes() + 2 * NODE_SIZE, root_fixed->cbytes(), NODE_SIZE);
        upload_s3(m_index_s3_key, std::move(index_chunk_data));

        auto data_chunk_data = sisl::make_byte_array(BLOCK_SIZE * 40, 0);
        for (const auto& [lba, blkid] : lba_to_blkid) {
            uint64_t offset = static_cast< uint64_t >(blkid.blk_num()) * BLOCK_SIZE;
            uint64_t sz = static_cast< uint64_t >(blkid.blk_count()) * BLOCK_SIZE;
            std::memset(data_chunk_data->bytes() + offset, static_cast< uint8_t >(lba & 0xFF), sz);
        }
        upload_s3(m_data_s3_key, std::move(data_chunk_data));

        PdevS3Superblock::SnapshotRecord snap;
        snap.header.snap_id = snap_id;
        snap.header.generation = 10;
        snap.header.btree_root_blkid = root_id;
        snap.header.num_chunk_keys = 2;

        s3_snapshot_chunk_key ck1;
        ck1.chunk_id = INDEX_CHUNK;
        ck1.set_s3_key(m_index_s3_key);
        snap.chunk_keys.push_back(ck1);

        s3_snapshot_chunk_key ck2;
        ck2.chunk_id = DATA_CHUNK;
        ck2.set_s3_key(m_data_s3_key);
        snap.chunk_keys.push_back(ck2);

        m_superblock->add_snapshot(snap);
    }

    void upload_s3(const std::string& key, sisl::byte_array data) {
        sisl::io_blob_safe blob{data->size()};
        std::memcpy(blob.bytes(), data->cbytes(), data->size());
        auto result = m_s3_store->put_object(key, std::move(blob)).get();
        ASSERT_TRUE(result.ok());
    }

    std::unique_ptr< SnapshotReader > make_reader() {
        return std::make_unique< SnapshotReader >(
            m_superblock.get(), m_s3_store, BLOCK_SIZE, NODE_SIZE);
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::unique_ptr< PdevS3Superblock > m_superblock;
    std::string m_index_s3_key;
    std::string m_data_s3_key;
};

///////////////////////////////////////////////////////////////////////////////
// Tests
///////////////////////////////////////////////////////////////////////////////

TEST_F(SnapshotReaderTest, SnapshotNotFound) {
    auto reader = make_reader();
    auto result = reader->snapshot_read(999, 0);
    EXPECT_EQ(result.status, SnapshotReadStatus::SNAPSHOT_NOT_FOUND);
}

TEST_F(SnapshotReaderTest, SingleLeafReadSuccess) {
    auto data_blkid = make_data_blkid(DATA_CHUNK, 4, 1);
    add_snapshot_with_single_leaf(100, 42, data_blkid);

    auto reader = make_reader();
    auto result = reader->snapshot_read(100, 42);

    ASSERT_EQ(result.status, SnapshotReadStatus::SUCCESS);
    ASSERT_NE(result.data, nullptr);
    EXPECT_EQ(result.data_size, BLOCK_SIZE);

    for (uint32_t i = 0; i < BLOCK_SIZE; ++i) {
        ASSERT_EQ(result.data->cbytes()[i], 0xDA) << "Mismatch at byte " << i;
    }
}

TEST_F(SnapshotReaderTest, SingleLeafKeyNotFound) {
    auto data_blkid = make_data_blkid(DATA_CHUNK, 4, 1);
    add_snapshot_with_single_leaf(100, 42, data_blkid);

    auto reader = make_reader();
    auto result = reader->snapshot_read(100, 99);
    EXPECT_EQ(result.status, SnapshotReadStatus::KEY_NOT_FOUND);
}

TEST_F(SnapshotReaderTest, MultiBlockRead) {
    auto data_blkid = make_data_blkid(DATA_CHUNK, 2, 3);
    add_snapshot_with_single_leaf(100, 10, data_blkid);

    auto reader = make_reader();
    auto result = reader->snapshot_read(100, 10);

    ASSERT_EQ(result.status, SnapshotReadStatus::SUCCESS);
    EXPECT_EQ(result.data_size, 3 * BLOCK_SIZE);
}

TEST_F(SnapshotReaderTest, ReadWithExplicitSize) {
    auto data_blkid = make_data_blkid(DATA_CHUNK, 4, 2);
    add_snapshot_with_single_leaf(100, 42, data_blkid);

    auto reader = make_reader();
    auto result = reader->snapshot_read(100, 42, 512);

    ASSERT_EQ(result.status, SnapshotReadStatus::SUCCESS);
    EXPECT_EQ(result.data_size, 512u);
}

TEST_F(SnapshotReaderTest, TwoLevelTreeTraversal) {
    std::vector< std::pair< uint64_t, BlkId > > entries = {
        {10, make_data_blkid(DATA_CHUNK, 0, 1)},
        {20, make_data_blkid(DATA_CHUNK, 1, 1)},
        {30, make_data_blkid(DATA_CHUNK, 2, 1)},
        {40, make_data_blkid(DATA_CHUNK, 3, 1)},
        {50, make_data_blkid(DATA_CHUNK, 4, 1)},
        {60, make_data_blkid(DATA_CHUNK, 5, 1)},
    };
    add_snapshot_with_two_level_tree(200, entries);

    auto reader = make_reader();

    // Read from left subtree (lba < split_key)
    auto r1 = reader->snapshot_read(200, 10);
    ASSERT_EQ(r1.status, SnapshotReadStatus::SUCCESS);
    EXPECT_EQ(r1.data_size, BLOCK_SIZE);
    EXPECT_EQ(r1.data->cbytes()[0], 10u);

    // Read from right subtree (lba >= split_key)
    auto r2 = reader->snapshot_read(200, 40);
    ASSERT_EQ(r2.status, SnapshotReadStatus::SUCCESS);
    EXPECT_EQ(r2.data_size, BLOCK_SIZE);
    EXPECT_EQ(r2.data->cbytes()[0], 40u);
}

TEST_F(SnapshotReaderTest, TwoLevelKeyNotFound) {
    std::vector< std::pair< uint64_t, BlkId > > entries = {
        {10, make_data_blkid(DATA_CHUNK, 0, 1)},
        {20, make_data_blkid(DATA_CHUNK, 1, 1)},
        {30, make_data_blkid(DATA_CHUNK, 2, 1)},
        {40, make_data_blkid(DATA_CHUNK, 3, 1)},
    };
    add_snapshot_with_two_level_tree(200, entries);

    auto reader = make_reader();
    auto result = reader->snapshot_read(200, 25);
    EXPECT_EQ(result.status, SnapshotReadStatus::KEY_NOT_FOUND);
}

TEST_F(SnapshotReaderTest, CorruptNodeDetected) {
    auto data_blkid = make_data_blkid(DATA_CHUNK, 4, 1);
    add_snapshot_with_single_leaf(100, 42, data_blkid);

    // Corrupt the index chunk data
    auto corrupt_data = sisl::make_byte_array(BLOCK_SIZE * 10, 0xFF);
    sisl::io_blob_safe blob{corrupt_data->size()};
    std::memcpy(blob.bytes(), corrupt_data->cbytes(), corrupt_data->size());
    m_s3_store->put_object(m_index_s3_key, std::move(blob)).get();

    auto reader = make_reader();
    auto result = reader->snapshot_read(100, 42);
    EXPECT_EQ(result.status, SnapshotReadStatus::BTREE_NODE_CORRUPT);
}

TEST_F(SnapshotReaderTest, MissingS3ObjectReturnsError) {
    PdevS3Superblock::SnapshotRecord snap;
    snap.header.snap_id = 300;
    snap.header.generation = 10;
    snap.header.btree_root_blkid = BlkId{0, 1, INDEX_CHUNK}.to_integer();
    snap.header.num_chunk_keys = 1;

    s3_snapshot_chunk_key ck;
    ck.chunk_id = INDEX_CHUNK;
    ck.set_s3_key("nonexistent/key.dat");
    snap.chunk_keys.push_back(ck);

    m_superblock->add_snapshot(snap);

    auto reader = make_reader();
    auto result = reader->snapshot_read(300, 0);
    EXPECT_EQ(result.status, SnapshotReadStatus::S3_READ_FAILED);
}

TEST_F(SnapshotReaderTest, ChunkNotInSnapshotMap) {
    // Snapshot whose root blkid references a chunk not in the snapshot's key map
    PdevS3Superblock::SnapshotRecord snap;
    snap.header.snap_id = 400;
    snap.header.generation = 10;
    snap.header.btree_root_blkid = BlkId{0, 1, 99}.to_integer();  // chunk 99 not in map
    snap.header.num_chunk_keys = 0;
    m_superblock->add_snapshot(snap);

    auto reader = make_reader();
    auto result = reader->snapshot_read(400, 0);
    EXPECT_EQ(result.status, SnapshotReadStatus::S3_READ_FAILED);
}

TEST_F(SnapshotReaderTest, MultipleSnapshotsIndependent) {
    auto blkid_a = make_data_blkid(DATA_CHUNK, 0, 1);
    add_snapshot_with_single_leaf(100, 42, blkid_a);

    // Second snapshot with different data and different S3 keys
    auto blkid_b = make_data_blkid(DATA_CHUNK, 1, 1);
    auto leaf_node_id_b = static_cast< bnodeid_t >(
        BlkId{0, 1, INDEX_CHUNK}.to_integer());
    auto leaf_b = build_btree_node(leaf_node_id_b, true,
        {{77, blkid_b.to_integer()}},
        empty_bnodeid, 0, 0);

    std::string index_key_b = "vol-test/chunks/5/data_gen2.dat";
    std::string data_key_b = "vol-test/chunks/10/data_gen2.dat";

    auto index_data_b = sisl::make_byte_array(BLOCK_SIZE * 10, 0);
    std::memcpy(index_data_b->bytes(), leaf_b->cbytes(), NODE_SIZE);
    upload_s3(index_key_b, std::move(index_data_b));

    auto data_data_b = sisl::make_byte_array(BLOCK_SIZE * 10, 0);
    std::memset(data_data_b->bytes() + BLOCK_SIZE, 0xBB, BLOCK_SIZE);
    upload_s3(data_key_b, std::move(data_data_b));

    PdevS3Superblock::SnapshotRecord snap_b;
    snap_b.header.snap_id = 200;
    snap_b.header.generation = 11;
    snap_b.header.btree_root_blkid = leaf_node_id_b;
    snap_b.header.num_chunk_keys = 2;

    s3_snapshot_chunk_key ck1;
    ck1.chunk_id = INDEX_CHUNK;
    ck1.set_s3_key(index_key_b);
    snap_b.chunk_keys.push_back(ck1);

    s3_snapshot_chunk_key ck2;
    ck2.chunk_id = DATA_CHUNK;
    ck2.set_s3_key(data_key_b);
    snap_b.chunk_keys.push_back(ck2);

    m_superblock->add_snapshot(snap_b);

    auto reader = make_reader();

    auto r1 = reader->snapshot_read(100, 42);
    ASSERT_EQ(r1.status, SnapshotReadStatus::SUCCESS);
    EXPECT_EQ(r1.data->cbytes()[0], 0xDA);

    auto r2 = reader->snapshot_read(200, 77);
    ASSERT_EQ(r2.status, SnapshotReadStatus::SUCCESS);
    EXPECT_EQ(r2.data->cbytes()[0], 0xBB);

    // Snapshot 100 doesn't have LBA 77
    auto r3 = reader->snapshot_read(100, 77);
    EXPECT_EQ(r3.status, SnapshotReadStatus::KEY_NOT_FOUND);
}

TEST_F(SnapshotReaderTest, AsyncReadWorks) {
    auto data_blkid = make_data_blkid(DATA_CHUNK, 4, 1);
    add_snapshot_with_single_leaf(100, 42, data_blkid);

    auto reader = make_reader();
    auto future = reader->snapshot_read_async(100, 42);
    auto result = std::move(future).get();

    ASSERT_EQ(result.status, SnapshotReadStatus::SUCCESS);
    EXPECT_EQ(result.data_size, BLOCK_SIZE);
}

TEST_F(SnapshotReaderTest, MetricsTracked) {
    auto data_blkid = make_data_blkid(DATA_CHUNK, 4, 1);
    add_snapshot_with_single_leaf(100, 42, data_blkid);

    auto reader = make_reader();
    reader->snapshot_read(100, 42);
    reader->snapshot_read(100, 42);
    reader->snapshot_read(999, 0);

    // Just verify the reader doesn't crash on metrics access
    auto& metrics = reader->metrics();
    (void)metrics;
}

TEST_F(SnapshotReaderTest, SingleLeafMultipleEntries) {
    auto leaf_node_id = static_cast< bnodeid_t >(
        BlkId{0, 1, INDEX_CHUNK}.to_integer());

    auto blk1 = make_data_blkid(DATA_CHUNK, 0, 1);
    auto blk2 = make_data_blkid(DATA_CHUNK, 1, 1);
    auto blk3 = make_data_blkid(DATA_CHUNK, 2, 1);

    auto leaf = build_btree_node(leaf_node_id, true,
        {{10, blk1.to_integer()}, {20, blk2.to_integer()}, {30, blk3.to_integer()}},
        empty_bnodeid, 0, 0);

    auto index_data = sisl::make_byte_array(BLOCK_SIZE * 10, 0);
    std::memcpy(index_data->bytes(), leaf->cbytes(), NODE_SIZE);
    upload_s3(m_index_s3_key, std::move(index_data));

    auto data_data = sisl::make_byte_array(BLOCK_SIZE * 10, 0);
    std::memset(data_data->bytes() + 0 * BLOCK_SIZE, 0xAA, BLOCK_SIZE);
    std::memset(data_data->bytes() + 1 * BLOCK_SIZE, 0xBB, BLOCK_SIZE);
    std::memset(data_data->bytes() + 2 * BLOCK_SIZE, 0xCC, BLOCK_SIZE);
    upload_s3(m_data_s3_key, std::move(data_data));

    PdevS3Superblock::SnapshotRecord snap;
    snap.header.snap_id = 500;
    snap.header.generation = 10;
    snap.header.btree_root_blkid = leaf_node_id;
    snap.header.num_chunk_keys = 2;

    s3_snapshot_chunk_key ck1;
    ck1.chunk_id = INDEX_CHUNK;
    ck1.set_s3_key(m_index_s3_key);
    snap.chunk_keys.push_back(ck1);

    s3_snapshot_chunk_key ck2;
    ck2.chunk_id = DATA_CHUNK;
    ck2.set_s3_key(m_data_s3_key);
    snap.chunk_keys.push_back(ck2);

    m_superblock->add_snapshot(snap);

    auto reader = make_reader();

    auto r1 = reader->snapshot_read(500, 10);
    ASSERT_EQ(r1.status, SnapshotReadStatus::SUCCESS);
    EXPECT_EQ(r1.data->cbytes()[0], 0xAA);

    auto r2 = reader->snapshot_read(500, 20);
    ASSERT_EQ(r2.status, SnapshotReadStatus::SUCCESS);
    EXPECT_EQ(r2.data->cbytes()[0], 0xBB);

    auto r3 = reader->snapshot_read(500, 30);
    ASSERT_EQ(r3.status, SnapshotReadStatus::SUCCESS);
    EXPECT_EQ(r3.data->cbytes()[0], 0xCC);

    auto r4 = reader->snapshot_read(500, 15);
    EXPECT_EQ(r4.status, SnapshotReadStatus::KEY_NOT_FOUND);
}

///////////////////////////////////////////////////////////////////////////////
// Main
///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
