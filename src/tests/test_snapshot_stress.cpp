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
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/blk.h>
#include <homestore/btree/detail/btree_node.hpp>
#include <homestore/btree/detail/btree_crc32c.hpp>
#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_gc.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/snapshot_manager.h>
#include <homestore/s3/snapshot_reader.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// B+tree node builder (shared with test_snapshot_reader.cpp)
///////////////////////////////////////////////////////////////////////////////
static constexpr uint32_t BLOCK_SIZE = 4096;
static constexpr uint32_t NODE_SIZE = 8192;
static constexpr uint32_t KEY_SIZE = sizeof(uint64_t);
static constexpr uint32_t LEAF_VALUE_SIZE = sizeof(uint64_t);

static sisl::byte_array build_leaf_node(bnodeid_t node_id,
                                        const std::vector< std::pair< uint64_t, uint64_t > >& entries) {
    auto buf = sisl::make_byte_array(NODE_SIZE, 0);
    auto* hdr = reinterpret_cast< persistent_hdr_t* >(buf->bytes());

    hdr->magic = BTREE_NODE_MAGIC;
    hdr->version = BTREE_NODE_VERSION;
    hdr->nentries = static_cast< uint32_t >(entries.size());
    hdr->leaf = 1;
    hdr->node_deleted = 0;
    hdr->node_id = node_id;
    hdr->next_node = empty_bnodeid;
    hdr->node_gen = 1;
    hdr->link_version = 1;
    hdr->edge_info.m_bnodeid = empty_bnodeid;
    hdr->edge_info.m_link_version = 0;
    hdr->modified_cp_id = 1;
    hdr->level = 0;
    hdr->node_size = static_cast< uint16_t >(NODE_SIZE - 1);
    hdr->node_type = 0;

    auto* data_area = buf->bytes() + sizeof(persistent_hdr_t);
    uint32_t entry_size = KEY_SIZE + LEAF_VALUE_SIZE;

    for (size_t i = 0; i < entries.size(); ++i) {
        auto* entry_ptr = data_area + (i * entry_size);
        std::memcpy(entry_ptr, &entries[i].first, KEY_SIZE);
        std::memcpy(entry_ptr + KEY_SIZE, &entries[i].second, LEAF_VALUE_SIZE);
    }

    hdr->checksum = bt_crc32c(bt_init_crc32c, buf->cbytes() + sizeof(persistent_hdr_t),
                              NODE_SIZE - sizeof(persistent_hdr_t));
    return buf;
}

static BlkId make_data_blkid(chunk_num_t chunk, blk_num_t blk, blk_count_t nblks) {
    return BlkId{blk, nblks, chunk};
}

///////////////////////////////////////////////////////////////////////////////
// Mock NvmeChunkReader
///////////////////////////////////////////////////////////////////////////////
class MockNvmeChunkReader : public NvmeChunkReader {
public:
    void set_chunk_data(chunk_id_t chunk_id, sisl::byte_array data) { m_chunks[chunk_id] = std::move(data); }

    std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t chunk_id, uint64_t chunk_size) override {
        auto it = m_chunks.find(chunk_id);
        if (it == m_chunks.end()) { return {{.status_code = 404, .error_message = "Not on NVMe"}, {}}; }
        auto copy = sisl::make_byte_array(static_cast< uint32_t >(chunk_size), 0);
        auto sz = std::min(static_cast< uint64_t >(it->second->size()), chunk_size);
        std::memcpy(copy->bytes(), it->second->cbytes(), sz);
        return {{.status_code = 0}, std::move(copy)};
    }

private:
    std::unordered_map< chunk_id_t, sisl::byte_array > m_chunks;
};

///////////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////////
static S3ObjectStoreConfig make_test_config() {
    S3ObjectStoreConfig cfg;
    cfg.bucket = "homestore-stress-test";
    cfg.region = "us-east-1";
    cfg.retry_count = 1;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

///////////////////////////////////////////////////////////////////////////////
// Stress Test Fixture
///////////////////////////////////////////////////////////////////////////////
class SnapshotStressTest : public ::testing::Test {
protected:
    static constexpr uint64_t CHUNK_SIZE = 4096;
    static constexpr chunk_num_t INDEX_CHUNK = 5;
    static constexpr chunk_num_t DATA_CHUNK = 10;

    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();
        m_key_mapper = S3KeyMapper{.volume_id = m_volume_id};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, m_key_mapper);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(1, m_chunk_store, m_s3_store, m_volume_id, 1024, nullptr);

        auto& sb = m_s3_pdev->superblock_mutable();
        sb.set_pdev_id(1);
        sb.set_generation(5);

        auto add_chunk = [&](chunk_id_t cid, S3ChunkType type) {
            s3_chunk_entry e;
            e.chunk_id = cid;
            e.chunk_size = CHUNK_SIZE;
            e.chunk_type = type;
            e.vdev_id = 1;
            e.set_s3_key(m_key_mapper.chunk_data_key(cid));
            sb.add_chunk(e);
            m_nvme_reader->set_chunk_data(cid, sisl::make_byte_array(CHUNK_SIZE, 0));
            m_s3_pdev->create_chunk(cid, CHUNK_SIZE, type, 1);
        };

        add_chunk(1, S3ChunkType::METABLK);
        add_chunk(2, S3ChunkType::WAL);
        add_chunk(DATA_CHUNK, S3ChunkType::DATA);
        add_chunk(11, S3ChunkType::DATA);

        m_cp_flush_success.store(true);
        auto cp_cb = [this]() -> bool { return m_cp_flush_success.load(); };

        m_snap_mgr = std::make_unique< SnapshotManager >(m_s3_pdev.get(), m_chunk_store.get(), cp_cb, m_s3_store);
    }

    void upload_chunk_data_to_s3() {
        for (const auto& chunk : m_s3_pdev->superblock().chunks()) {
            auto blob = sisl::io_blob_safe{64};
            std::memset(blob.bytes(), 0xAB, 64);
            m_s3_store->put_object(chunk.get_s3_key(), std::move(blob)).get();
        }
    }

    void setup_readable_snapshot(uint64_t snap_id, uint64_t lba, uint8_t data_pattern) {
        auto leaf_node_id = static_cast< bnodeid_t >(BlkId{0, 1, INDEX_CHUNK}.to_integer());
        auto data_blkid = make_data_blkid(DATA_CHUNK, 4, 1);

        auto leaf = build_leaf_node(leaf_node_id, {{lba, data_blkid.to_integer()}});

        auto index_data = sisl::make_byte_array(BLOCK_SIZE * 10, 0);
        std::memcpy(index_data->bytes(), leaf->cbytes(), NODE_SIZE);

        auto data_data = sisl::make_byte_array(BLOCK_SIZE * 10, 0);
        uint64_t data_offset = static_cast< uint64_t >(data_blkid.blk_num()) * BLOCK_SIZE;
        std::memset(data_data->bytes() + data_offset, data_pattern, BLOCK_SIZE);

        std::string idx_key = "vol-stress/snap" + std::to_string(snap_id) + "/idx.dat";
        std::string dat_key = "vol-stress/snap" + std::to_string(snap_id) + "/data.dat";

        {
            sisl::io_blob_safe blob{index_data->size()};
            std::memcpy(blob.bytes(), index_data->cbytes(), index_data->size());
            m_s3_store->put_object(idx_key, std::move(blob)).get();
        }
        {
            sisl::io_blob_safe blob{data_data->size()};
            std::memcpy(blob.bytes(), data_data->cbytes(), data_data->size());
            m_s3_store->put_object(dat_key, std::move(blob)).get();
        }

        PdevS3Superblock::SnapshotRecord snap;
        snap.header.snap_id = snap_id;
        snap.header.generation = 10;
        snap.header.btree_root_blkid = leaf_node_id;
        snap.header.num_chunk_keys = 2;

        s3_snapshot_chunk_key ck1;
        ck1.chunk_id = INDEX_CHUNK;
        ck1.set_s3_key(idx_key);
        snap.chunk_keys.push_back(ck1);

        s3_snapshot_chunk_key ck2;
        ck2.chunk_id = DATA_CHUNK;
        ck2.set_s3_key(dat_key);
        snap.chunk_keys.push_back(ck2);

        m_superblock_for_reader.add_snapshot(snap);
    }

    std::string m_volume_id{"vol-stress-test"};
    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    S3KeyMapper m_key_mapper;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::unique_ptr< SnapshotManager > m_snap_mgr;
    std::atomic< bool > m_cp_flush_success{true};
    PdevS3Superblock m_superblock_for_reader;
};

///////////////////////////////////////////////////////////////////////////////
// Stress Tests
///////////////////////////////////////////////////////////////////////////////

TEST_F(SnapshotStressTest, ConcurrentSnapshotCreation) {
    static constexpr int NUM_THREADS = 8;
    static constexpr int SNAPS_PER_THREAD = 10;

    std::atomic< uint64_t > success_count{0};
    std::atomic< uint64_t > duplicate_count{0};
    std::atomic< uint64_t > next_snap_id{1000};

    std::vector< std::thread > threads;
    threads.reserve(NUM_THREADS);

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < SNAPS_PER_THREAD; ++i) {
                auto snap_id = next_snap_id.fetch_add(1);
                auto result = m_snap_mgr->create_snapshot(snap_id, snap_id * 100);
                if (result.status == SnapshotResult::SUCCESS) {
                    success_count.fetch_add(1);
                } else if (result.status == SnapshotResult::DUPLICATE_SNAP_ID) {
                    duplicate_count.fetch_add(1);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto total = success_count.load() + duplicate_count.load();
    EXPECT_EQ(total, NUM_THREADS * SNAPS_PER_THREAD);
    EXPECT_GT(success_count.load(), 0u);
    EXPECT_EQ(duplicate_count.load(), 0u);

    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(),
              static_cast< uint32_t >(success_count.load()));
}

TEST_F(SnapshotStressTest, ConcurrentSnapshotReads) {
    static constexpr int NUM_READERS = 8;
    static constexpr int READS_PER_READER = 50;
    static constexpr int NUM_SNAPSHOTS = 4;

    m_superblock_for_reader.set_pdev_id(1);
    m_superblock_for_reader.set_generation(10);

    for (int s = 0; s < NUM_SNAPSHOTS; ++s) {
        uint64_t snap_id = 100 + s;
        uint64_t lba = 42 + s;
        uint8_t pattern = static_cast< uint8_t >(0xA0 + s);
        setup_readable_snapshot(snap_id, lba, pattern);
    }

    auto reader = std::make_shared< SnapshotReader >(&m_superblock_for_reader, m_s3_store, BLOCK_SIZE, NODE_SIZE);

    std::atomic< uint64_t > reads_ok{0};
    std::atomic< uint64_t > reads_failed{0};

    std::vector< std::thread > threads;
    threads.reserve(NUM_READERS);

    for (int t = 0; t < NUM_READERS; ++t) {
        threads.emplace_back([&, t]() {
            std::mt19937 rng(42 + t);
            std::uniform_int_distribution< int > snap_dist(0, NUM_SNAPSHOTS - 1);

            for (int i = 0; i < READS_PER_READER; ++i) {
                int s = snap_dist(rng);
                uint64_t snap_id = 100 + s;
                uint64_t lba = 42 + s;
                uint8_t expected_pattern = static_cast< uint8_t >(0xA0 + s);

                auto result = reader->snapshot_read(snap_id, lba);
                if (result.status == SnapshotReadStatus::SUCCESS && result.data != nullptr &&
                    result.data->cbytes()[0] == expected_pattern) {
                    reads_ok.fetch_add(1);
                } else {
                    reads_failed.fetch_add(1);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(reads_ok.load(), static_cast< uint64_t >(NUM_READERS * READS_PER_READER));
    EXPECT_EQ(reads_failed.load(), 0u);
}

TEST_F(SnapshotStressTest, RapidCreateDeleteCycles) {
    static constexpr int NUM_CYCLES = 50;

    upload_chunk_data_to_s3();

    for (int i = 0; i < NUM_CYCLES; ++i) {
        uint64_t snap_id = 5000 + i;

        auto create_result = m_snap_mgr->create_snapshot(snap_id, i * 10);
        ASSERT_EQ(create_result.status, SnapshotResult::SUCCESS)
            << "Create failed at cycle " << i;

        EXPECT_GT(m_chunk_store->active_generation(), 0u);

        // Upload gen-stamped keys to simulate CP
        for (const auto& chunk : m_s3_pdev->superblock().chunks()) {
            auto blob = sisl::io_blob_safe{64};
            std::memset(blob.bytes(), static_cast< uint8_t >(i & 0xFF), 64);
            m_s3_store->put_object(chunk.get_s3_key(), std::move(blob)).get();
        }

        auto del_result = m_snap_mgr->delete_snapshot(snap_id);
        ASSERT_TRUE(del_result.success) << "Delete failed at cycle " << i;

        EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 0u);
        EXPECT_EQ(m_chunk_store->active_generation(), 0u);
    }

    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 0u);
    EXPECT_EQ(m_chunk_store->active_generation(), 0u);
}

TEST_F(SnapshotStressTest, GcUnderConcurrentSnapshotLifecycle) {
    upload_chunk_data_to_s3();

    auto gc = std::make_unique< S3GarbageCollector >(&m_s3_pdev->superblock(), m_s3_store);

    static constexpr int NUM_SNAPSHOTS = 10;
    std::vector< uint64_t > snap_ids;
    std::vector< std::vector< std::string > > pinned_keys_per_snap;

    for (int i = 0; i < NUM_SNAPSHOTS; ++i) {
        uint64_t snap_id = 7000 + i;
        auto result = m_snap_mgr->create_snapshot(snap_id, i);
        ASSERT_EQ(result.status, SnapshotResult::SUCCESS);
        snap_ids.push_back(snap_id);

        auto* snap = m_s3_pdev->superblock().find_snapshot(snap_id);
        ASSERT_NE(snap, nullptr);
        std::vector< std::string > keys;
        for (const auto& ck : snap->chunk_keys) {
            keys.push_back(ck.get_s3_key());
        }
        pinned_keys_per_snap.push_back(std::move(keys));

        for (const auto& chunk : m_s3_pdev->superblock().chunks()) {
            auto blob = sisl::io_blob_safe{64};
            std::memset(blob.bytes(), static_cast< uint8_t >(i), 64);
            m_s3_store->put_object(chunk.get_s3_key(), std::move(blob)).get();
        }

        // Add old keys as GC candidates
        if (i > 0) {
            gc->on_generation_change(pinned_keys_per_snap[i - 1]);
        }
    }

    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), static_cast< uint32_t >(NUM_SNAPSHOTS));

    auto pinned = gc->collect_pinned_keys();
    for (const auto& keys : pinned_keys_per_snap) {
        for (const auto& key : keys) {
            EXPECT_TRUE(pinned.count(key) > 0) << "Key should be pinned: " << key;
        }
    }

    auto deleted_before = gc->run_gc();
    EXPECT_EQ(deleted_before, 0u);

    for (int i = 0; i < NUM_SNAPSHOTS - 1; ++i) {
        auto del_result = m_snap_mgr->delete_snapshot(snap_ids[i]);
        ASSERT_TRUE(del_result.success);
    }

    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 1u);

    auto pinned_after = gc->collect_pinned_keys();
    for (const auto& key : pinned_keys_per_snap.back()) {
        EXPECT_TRUE(pinned_after.count(key) > 0) << "Last snapshot key should be pinned: " << key;
    }
}

TEST_F(SnapshotStressTest, ConcurrentCreateAndDelete) {
    static constexpr int NUM_CREATORS = 4;
    static constexpr int CREATES_PER_THREAD = 10;

    upload_chunk_data_to_s3();

    std::atomic< uint64_t > next_snap_id{9000};
    std::atomic< uint64_t > total_created{0};
    std::atomic< uint64_t > total_deleted{0};
    std::mutex created_ids_mutex;
    std::vector< uint64_t > created_ids;

    std::vector< std::thread > creators;
    creators.reserve(NUM_CREATORS);

    for (int t = 0; t < NUM_CREATORS; ++t) {
        creators.emplace_back([&]() {
            for (int i = 0; i < CREATES_PER_THREAD; ++i) {
                auto snap_id = next_snap_id.fetch_add(1);
                auto result = m_snap_mgr->create_snapshot(snap_id);
                if (result.status == SnapshotResult::SUCCESS) {
                    total_created.fetch_add(1);
                    std::lock_guard< std::mutex > lock(created_ids_mutex);
                    created_ids.push_back(snap_id);
                }

                // Upload data for the new gen to simulate CP
                for (const auto& chunk : m_s3_pdev->superblock().chunks()) {
                    auto blob = sisl::io_blob_safe{64};
                    std::memset(blob.bytes(), 0xEE, 64);
                    m_s3_store->put_object(chunk.get_s3_key(), std::move(blob)).get();
                }
            }
        });
    }

    for (auto& t : creators) {
        t.join();
    }

    auto created = total_created.load();
    EXPECT_GT(created, 0u);

    std::vector< uint64_t > ids_to_delete;
    {
        std::lock_guard< std::mutex > lock(created_ids_mutex);
        ids_to_delete = created_ids;
    }

    static constexpr int NUM_DELETERS = 4;
    std::atomic< size_t > delete_idx{0};

    std::vector< std::thread > deleters;
    deleters.reserve(NUM_DELETERS);

    for (int t = 0; t < NUM_DELETERS; ++t) {
        deleters.emplace_back([&]() {
            while (true) {
                auto idx = delete_idx.fetch_add(1);
                if (idx >= ids_to_delete.size()) break;
                auto result = m_snap_mgr->delete_snapshot(ids_to_delete[idx]);
                if (result.success) { total_deleted.fetch_add(1); }
            }
        });
    }

    for (auto& t : deleters) {
        t.join();
    }

    EXPECT_EQ(total_deleted.load(), created);
    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 0u);
    EXPECT_EQ(m_chunk_store->active_generation(), 0u);
}

TEST_F(SnapshotStressTest, ConcurrentReadsWhileDeleting) {
    static constexpr int NUM_SNAPSHOTS = 5;
    static constexpr int NUM_READERS = 4;
    static constexpr int READS_PER_READER = 100;

    m_superblock_for_reader.set_pdev_id(1);
    m_superblock_for_reader.set_generation(10);

    for (int s = 0; s < NUM_SNAPSHOTS; ++s) {
        setup_readable_snapshot(100 + s, 42 + s, static_cast< uint8_t >(0xB0 + s));
    }

    auto reader = std::make_shared< SnapshotReader >(&m_superblock_for_reader, m_s3_store, BLOCK_SIZE, NODE_SIZE);

    std::atomic< bool > deleting_done{false};
    std::atomic< uint64_t > reads_ok{0};
    std::atomic< uint64_t > reads_not_found{0};
    std::atomic< uint64_t > reads_other{0};

    std::vector< std::thread > readers;
    readers.reserve(NUM_READERS);

    for (int t = 0; t < NUM_READERS; ++t) {
        readers.emplace_back([&, t]() {
            std::mt19937 rng(100 + t);
            std::uniform_int_distribution< int > snap_dist(0, NUM_SNAPSHOTS - 1);

            for (int i = 0; i < READS_PER_READER; ++i) {
                int s = snap_dist(rng);
                uint64_t snap_id = 100 + s;
                uint64_t lba = 42 + s;

                auto result = reader->snapshot_read(snap_id, lba);
                if (result.status == SnapshotReadStatus::SUCCESS) {
                    reads_ok.fetch_add(1);
                } else if (result.status == SnapshotReadStatus::SNAPSHOT_NOT_FOUND) {
                    reads_not_found.fetch_add(1);
                } else {
                    reads_other.fetch_add(1);
                }
            }
        });
    }

    // Delete snapshots while readers are running
    std::thread deleter([&]() {
        for (int s = 0; s < NUM_SNAPSHOTS; ++s) {
            m_superblock_for_reader.remove_snapshot(100 + s);
        }
        deleting_done.store(true);
    });

    for (auto& t : readers) {
        t.join();
    }
    deleter.join();

    auto total = reads_ok.load() + reads_not_found.load() + reads_other.load();
    EXPECT_EQ(total, static_cast< uint64_t >(NUM_READERS * READS_PER_READER));
    EXPECT_GT(reads_ok.load(), 0u);
}

TEST_F(SnapshotStressTest, FullLifecycleUnderPressure) {
    static constexpr int NUM_ITERATIONS = 20;
    static constexpr int READERS_PER_ITERATION = 4;
    static constexpr int READS_PER_READER = 20;

    upload_chunk_data_to_s3();

    m_superblock_for_reader.set_pdev_id(1);
    m_superblock_for_reader.set_generation(10);

    uint64_t snap_counter = 10000;

    for (int iter = 0; iter < NUM_ITERATIONS; ++iter) {
        uint64_t snap_id = snap_counter++;
        uint64_t lba = 42;
        uint8_t pattern = static_cast< uint8_t >(iter & 0xFF);

        // 1. Create snapshot via manager
        auto create_result = m_snap_mgr->create_snapshot(snap_id, snap_id * 10);
        ASSERT_EQ(create_result.status, SnapshotResult::SUCCESS) << "iter=" << iter;

        // 2. Set up readable snapshot data
        setup_readable_snapshot(snap_id, lba, pattern);
        auto reader =
            std::make_shared< SnapshotReader >(&m_superblock_for_reader, m_s3_store, BLOCK_SIZE, NODE_SIZE);

        // 3. Concurrent reads
        std::atomic< uint64_t > reads_ok{0};
        std::vector< std::thread > readers;
        readers.reserve(READERS_PER_ITERATION);

        for (int t = 0; t < READERS_PER_ITERATION; ++t) {
            readers.emplace_back([&]() {
                for (int r = 0; r < READS_PER_READER; ++r) {
                    auto result = reader->snapshot_read(snap_id, lba);
                    if (result.status == SnapshotReadStatus::SUCCESS && result.data != nullptr &&
                        result.data->cbytes()[0] == pattern) {
                        reads_ok.fetch_add(1);
                    }
                }
            });
        }
        for (auto& t : readers) {
            t.join();
        }
        EXPECT_EQ(reads_ok.load(), static_cast< uint64_t >(READERS_PER_ITERATION * READS_PER_READER))
            << "iter=" << iter;

        // 4. Simulate CP with gen-stamped keys
        for (const auto& chunk : m_s3_pdev->superblock().chunks()) {
            auto blob = sisl::io_blob_safe{64};
            std::memset(blob.bytes(), pattern, 64);
            m_s3_store->put_object(chunk.get_s3_key(), std::move(blob)).get();
        }

        // 5. Delete snapshot
        auto del_result = m_snap_mgr->delete_snapshot(snap_id);
        ASSERT_TRUE(del_result.success) << "iter=" << iter;

        m_superblock_for_reader.remove_snapshot(snap_id);
    }

    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 0u);
    EXPECT_EQ(m_chunk_store->active_generation(), 0u);
}

TEST_F(SnapshotStressTest, GcConcurrentWithCandidateAddition) {
    auto gc = std::make_unique< S3GarbageCollector >(&m_s3_pdev->superblock(), m_s3_store);

    static constexpr int NUM_ADDERS = 4;
    static constexpr int KEYS_PER_ADDER = 50;

    // Upload objects that GC can delete
    for (int i = 0; i < NUM_ADDERS * KEYS_PER_ADDER; ++i) {
        std::string key = "gc-stress/obj_" + std::to_string(i) + ".dat";
        auto blob = sisl::io_blob_safe{64};
        std::memset(blob.bytes(), 0xCC, 64);
        m_s3_store->put_object(key, std::move(blob)).get();
    }

    std::atomic< bool > adding_done{false};
    std::atomic< uint64_t > total_gc_deleted{0};

    // Adder threads: add GC candidates
    std::vector< std::thread > adders;
    adders.reserve(NUM_ADDERS);
    for (int t = 0; t < NUM_ADDERS; ++t) {
        adders.emplace_back([&, t]() {
            for (int i = 0; i < KEYS_PER_ADDER; ++i) {
                int idx = t * KEYS_PER_ADDER + i;
                gc->add_gc_candidate("gc-stress/obj_" + std::to_string(idx) + ".dat");
            }
        });
    }

    // GC runner: runs concurrently with adders
    std::thread gc_runner([&]() {
        while (!adding_done.load() || gc->pending_count() > 0) {
            auto deleted = gc->run_gc();
            total_gc_deleted.fetch_add(deleted);
        }
    });

    for (auto& t : adders) {
        t.join();
    }
    adding_done.store(true);

    gc_runner.join();

    // Run one final GC to catch any stragglers
    total_gc_deleted.fetch_add(gc->run_gc());

    EXPECT_EQ(total_gc_deleted.load(), static_cast< uint64_t >(NUM_ADDERS * KEYS_PER_ADDER));
    EXPECT_EQ(gc->pending_count(), 0u);
    EXPECT_EQ(m_s3_store->object_count(), 0u);
}

TEST_F(SnapshotStressTest, ManySnapshotsAccumulateAndBulkDelete) {
    static constexpr int NUM_SNAPSHOTS = 50;

    upload_chunk_data_to_s3();

    for (int i = 0; i < NUM_SNAPSHOTS; ++i) {
        auto result = m_snap_mgr->create_snapshot(20000 + i, i);
        ASSERT_EQ(result.status, SnapshotResult::SUCCESS) << "Failed at snapshot " << i;

        for (const auto& chunk : m_s3_pdev->superblock().chunks()) {
            auto blob = sisl::io_blob_safe{64};
            std::memset(blob.bytes(), static_cast< uint8_t >(i), 64);
            m_s3_store->put_object(chunk.get_s3_key(), std::move(blob)).get();
        }
    }

    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), NUM_SNAPSHOTS);

    // Delete in reverse order
    for (int i = NUM_SNAPSHOTS - 1; i >= 0; --i) {
        auto result = m_snap_mgr->delete_snapshot(20000 + i);
        ASSERT_TRUE(result.success) << "Failed deleting snapshot " << i;
    }

    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 0u);
    EXPECT_EQ(m_chunk_store->active_generation(), 0u);
}

TEST_F(SnapshotStressTest, InterleavedCreateReadDelete) {
    upload_chunk_data_to_s3();

    m_superblock_for_reader.set_pdev_id(1);
    m_superblock_for_reader.set_generation(10);

    static constexpr int WINDOW = 5;
    static constexpr int TOTAL_OPS = 30;

    std::deque< uint64_t > active_snaps;
    uint64_t snap_counter = 30000;

    for (int op = 0; op < TOTAL_OPS; ++op) {
        uint64_t snap_id = snap_counter++;
        uint64_t lba = 42 + (op % 10);
        uint8_t pattern = static_cast< uint8_t >(op & 0xFF);

        auto create_result = m_snap_mgr->create_snapshot(snap_id, snap_id);
        ASSERT_EQ(create_result.status, SnapshotResult::SUCCESS) << "op=" << op;
        active_snaps.push_back(snap_id);

        // Set up readable version
        setup_readable_snapshot(snap_id, lba, pattern);
        auto reader =
            std::make_shared< SnapshotReader >(&m_superblock_for_reader, m_s3_store, BLOCK_SIZE, NODE_SIZE);
        auto read_result = reader->snapshot_read(snap_id, lba);
        ASSERT_EQ(read_result.status, SnapshotReadStatus::SUCCESS) << "op=" << op;
        EXPECT_EQ(read_result.data->cbytes()[0], pattern) << "op=" << op;

        for (const auto& chunk : m_s3_pdev->superblock().chunks()) {
            auto blob = sisl::io_blob_safe{64};
            std::memset(blob.bytes(), pattern, 64);
            m_s3_store->put_object(chunk.get_s3_key(), std::move(blob)).get();
        }

        // Evict oldest when window is full
        if (static_cast< int >(active_snaps.size()) > WINDOW) {
            auto old_id = active_snaps.front();
            active_snaps.pop_front();

            auto del_result = m_snap_mgr->delete_snapshot(old_id);
            ASSERT_TRUE(del_result.success) << "op=" << op << " delete=" << old_id;
            m_superblock_for_reader.remove_snapshot(old_id);
        }
    }

    EXPECT_LE(m_s3_pdev->superblock().num_snapshots(), static_cast< uint32_t >(WINDOW));

    // Clean up remaining
    while (!active_snaps.empty()) {
        auto id = active_snaps.front();
        active_snaps.pop_front();
        m_snap_mgr->delete_snapshot(id);
    }

    EXPECT_EQ(m_s3_pdev->superblock().num_snapshots(), 0u);
}

///////////////////////////////////////////////////////////////////////////////
// Main
///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
