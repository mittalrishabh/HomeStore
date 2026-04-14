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

/**
 * @file test_s3_integ_cp_recovery_integrity.cpp
 * @brief Integration tests — Groups 1, 3, 7
 *
 *   Group 1: Write → CP → S3 Upload
 *   Group 3: Bulk Recovery from S3
 *   Group 7: Superblock + Cross-Concern Integrity
 *
 * Wires up REAL: S3PhysicalDev, FullChunkStore, S3CpCallbacks, S3RecoveryManager,
 *               PdevS3Superblock, ChunkEvictionManager, ChunkHydrationManager,
 *               TieredReadHandler
 * Mocked:        S3 transport (FailureInjectableS3Store), NVMe I/O (MockNvmeDeviceIO)
 */

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/checkpoint/cp.hpp>
#include <homestore/checkpoint/cp_mgr.hpp>
#include <homestore/s3/chunk_eviction_manager.h>
#include <homestore/s3/chunk_hydration_manager.h>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_cp.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/s3_recovery.h>
#include <homestore/s3/tiered_read_handler.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Mocks
///////////////////////////////////////////////////////////////////////////////

class FailureInjectableS3Store : public MockS3ObjectStore {
public:
    using MockS3ObjectStore::MockS3ObjectStore;

    folly::Future< S3Result > put_object(const std::string& key, sisl::io_blob_safe data) override {
        {
            std::lock_guard< std::mutex > lock{m_order_mutex};
            m_put_order.push_back(key);
        }
        {
            std::lock_guard< std::mutex > lock{m_fail_mutex};
            auto it = m_put_fail_keys.find(key);
            if (it != m_put_fail_keys.end()) {
                if (it->second > 0) {
                    --it->second;
                    S3Result r;
                    r.status_code = 500;
                    r.error_message = "Injected PUT failure for " + key;
                    return folly::makeFuture(r);
                }
                m_put_fail_keys.erase(it);
            }
        }
        return MockS3ObjectStore::put_object(key, std::move(data));
    }

    folly::Future< std::pair< S3Result, sisl::io_blob_safe > > get_object(const std::string& key) override {
        {
            std::lock_guard< std::mutex > lock{m_fail_mutex};
            auto it = m_get_fail_keys.find(key);
            if (it != m_get_fail_keys.end()) {
                if (it->second > 0) {
                    --it->second;
                    S3Result r;
                    r.status_code = 500;
                    r.error_message = "Injected GET failure for " + key;
                    return folly::makeFuture(std::make_pair(std::move(r), sisl::io_blob_safe{}));
                }
                m_get_fail_keys.erase(it);
            }
        }
        return MockS3ObjectStore::get_object(key);
    }

    std::vector< std::string > put_order() const {
        std::lock_guard< std::mutex > lock{m_order_mutex};
        return m_put_order;
    }

    void clear_order() {
        std::lock_guard< std::mutex > lock{m_order_mutex};
        m_put_order.clear();
    }

    void inject_put_failure(const std::string& key, uint32_t times = 1) {
        std::lock_guard< std::mutex > lock{m_fail_mutex};
        m_put_fail_keys[key] = times;
    }

    void inject_get_failure(const std::string& key, uint32_t times = 1) {
        std::lock_guard< std::mutex > lock{m_fail_mutex};
        m_get_fail_keys[key] = times;
    }

    void clear_failures() {
        std::lock_guard< std::mutex > lock{m_fail_mutex};
        m_put_fail_keys.clear();
        m_get_fail_keys.clear();
    }

private:
    mutable std::mutex m_order_mutex;
    std::vector< std::string > m_put_order;

    mutable std::mutex m_fail_mutex;
    std::unordered_map< std::string, uint32_t > m_put_fail_keys;
    std::unordered_map< std::string, uint32_t > m_get_fail_keys;
};

class MockNvmeChunkReader : public NvmeChunkReader {
public:
    void set_chunk_data(chunk_id_t chunk_id, sisl::byte_array data) {
        m_chunks[chunk_id] = std::move(data);
    }

    void remove_chunk_data(chunk_id_t chunk_id) { m_chunks.erase(chunk_id); }

    std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t chunk_id, uint64_t chunk_size) override {
        auto it = m_chunks.find(chunk_id);
        if (it == m_chunks.end()) {
            return {{.status_code = 404, .error_message = "Not on NVMe"}, {}};
        }
        auto copy = sisl::make_byte_array(static_cast< uint32_t >(chunk_size), 0);
        auto sz = std::min(static_cast< uint64_t >(it->second->size()), chunk_size);
        std::memcpy(copy->bytes(), it->second->cbytes(), sz);
        return {{.status_code = 0}, std::move(copy)};
    }

private:
    std::unordered_map< chunk_id_t, sisl::byte_array > m_chunks;
};

class MockNvmeChunkManager : public NvmeChunkManager {
public:
    void set_on_nvme(chunk_id_t chunk_id, bool on) {
        std::lock_guard lock{m_mtx};
        if (on) m_on_nvme.insert(chunk_id);
        else m_on_nvme.erase(chunk_id);
    }

    bool is_chunk_on_nvme(chunk_id_t chunk_id) const override {
        std::lock_guard lock{m_mtx};
        return m_on_nvme.count(chunk_id) > 0;
    }

    std::error_code release_nvme_chunk(chunk_id_t chunk_id) override {
        std::lock_guard lock{m_mtx};
        m_on_nvme.erase(chunk_id);
        return {};
    }

private:
    mutable std::mutex m_mtx;
    std::set< chunk_id_t > m_on_nvme;
};

class MockNvmeDeviceIO : public NvmeDeviceIO {
public:
    void store(chunk_id_t chunk_id, uint64_t offset, const char* data, uint64_t size) {
        std::lock_guard lock{m_mtx};
        auto key = make_key(chunk_id, offset);
        auto buf = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(buf->bytes(), data, size);
        m_storage[key] = std::move(buf);
        m_on_nvme.insert(chunk_id);
    }

    void set_on_nvme(chunk_id_t chunk_id, bool on) {
        std::lock_guard lock{m_mtx};
        if (on) m_on_nvme.insert(chunk_id);
        else m_on_nvme.erase(chunk_id);
    }

    void wipe_all() {
        std::lock_guard lock{m_mtx};
        m_storage.clear();
        m_on_nvme.clear();
    }

    std::error_code nvme_read(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                              char* buf, uint64_t size) override {
        std::lock_guard lock{m_mtx};
        if (m_on_nvme.find(chunk_id) == m_on_nvme.end()) {
            return std::make_error_code(std::errc::no_such_device);
        }
        auto key = make_key(chunk_id, offset_in_chunk);
        auto it = m_storage.find(key);
        if (it != m_storage.end()) {
            auto to_copy = std::min(size, static_cast< uint64_t >(it->second->size()));
            std::memcpy(buf, it->second->cbytes(), to_copy);
            return {};
        }
        return std::make_error_code(std::errc::no_such_file_or_directory);
    }

    std::error_code nvme_write(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                               const char* buf, uint64_t size) override {
        std::lock_guard lock{m_mtx};
        auto key = make_key(chunk_id, offset_in_chunk);
        auto data = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(data->bytes(), buf, size);
        m_storage[key] = std::move(data);
        m_on_nvme.insert(chunk_id);
        return {};
    }

    bool is_chunk_on_nvme(chunk_id_t chunk_id) const override {
        std::lock_guard lock{m_mtx};
        return m_on_nvme.count(chunk_id) > 0;
    }

private:
    static std::string make_key(chunk_id_t cid, uint64_t offset) {
        return std::to_string(cid) + ":" + std::to_string(offset);
    }

    mutable std::mutex m_mtx;
    std::unordered_map< std::string, sisl::byte_array > m_storage;
    std::set< chunk_id_t > m_on_nvme;
};

class MockNvmeChunkAllocator : public NvmeChunkAllocator {
public:
    explicit MockNvmeChunkAllocator(uint64_t total_space = 64 * 1024 * 1024)
        : m_total_space{total_space}, m_used_space{0} {}

    std::error_code allocate_nvme_chunk(chunk_id_t, uint64_t chunk_size) override {
        if (m_used_space + chunk_size > m_total_space) {
            return std::make_error_code(std::errc::no_space_on_device);
        }
        m_used_space += chunk_size;
        return {};
    }

    void release_nvme_chunk(chunk_id_t, uint64_t chunk_size) override {
        m_used_space = m_used_space > chunk_size ? m_used_space - chunk_size : 0;
    }

    uint64_t free_nvme_space_bytes() const override {
        return m_total_space > m_used_space ? m_total_space - m_used_space : 0;
    }

private:
    uint64_t m_total_space;
    uint64_t m_used_space;
};

class MockNvmeRecoveryWriter : public NvmeRecoveryWriter {
public:
    void set_nvme_state(NvmeState state) { m_state = state; }

    NvmeState check_nvme_state() override { return m_state; }

    std::error_code write_chunk(chunk_id_t chunk_id, const sisl::byte_array& data) override {
        if (m_fail_chunks.count(chunk_id)) {
            return std::make_error_code(std::errc::io_error);
        }
        m_written_chunks[chunk_id] = data;
        return {};
    }

    void set_chunk_fail(chunk_id_t chunk_id) { m_fail_chunks.insert(chunk_id); }

    bool has_chunk(chunk_id_t chunk_id) const { return m_written_chunks.count(chunk_id) > 0; }

    const sisl::byte_array& get_chunk(chunk_id_t chunk_id) const { return m_written_chunks.at(chunk_id); }

    size_t written_count() const { return m_written_chunks.size(); }

    void clear_written() { m_written_chunks.clear(); }

private:
    NvmeState m_state{NvmeState::EMPTY};
    std::unordered_map< chunk_id_t, sisl::byte_array > m_written_chunks;
    std::set< chunk_id_t > m_fail_chunks;
};

///////////////////////////////////////////////////////////////////////////////
// Minimal CP stub
///////////////////////////////////////////////////////////////////////////////
class TestCP : public CP {
public:
    TestCP() : CP{nullptr} {
        m_cp_id = 1;
        m_cp_status.store(cp_status_t::cp_flushing);
    }
};

///////////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////////

static sisl::byte_array make_test_data(uint32_t size, uint8_t pattern) {
    auto buf = sisl::make_byte_array(size, 0);
    std::memset(buf->bytes(), pattern, size);
    return buf;
}

static sisl::byte_array make_patterned_data(uint32_t size, uint8_t seed) {
    auto buf = sisl::make_byte_array(size, 0);
    for (uint32_t i = 0; i < size; ++i) {
        buf->bytes()[i] = static_cast< uint8_t >((seed + i) & 0xFF);
    }
    return buf;
}

static S3ObjectStoreConfig make_test_config() {
    S3ObjectStoreConfig cfg;
    cfg.bucket = "homestore-integ-test";
    cfg.region = "us-east-1";
    cfg.retry_count = 3;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

static void wait_for_condition(std::function< bool() > cond, int max_ms = 2000) {
    for (int elapsed = 0; elapsed < max_ms; elapsed += 10) {
        if (cond()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

///////////////////////////////////////////////////////////////////////////////
// Group 1 Fixture: Write → CP → S3 Upload
///////////////////////////////////////////////////////////////////////////////
class CpIntegTest : public ::testing::Test {
protected:
    static constexpr uint64_t CHUNK_SIZE = 4096;
    static constexpr const char* VOLUME_ID = "vol-integ-cp";

    void SetUp() override {
        m_s3_store = std::make_shared< FailureInjectableS3Store >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();

        S3KeyMapper key_mapper{.volume_id = VOLUME_ID};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, key_mapper);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, VOLUME_ID, 1024, nullptr);

        m_callbacks = std::make_unique< S3CpCallbacks >(
            std::vector< S3PhysicalDev* >{m_s3_pdev.get()}, 2);

    }

    void create_chunk_with_data(chunk_id_t chunk_id, S3ChunkType type, uint8_t pattern) {
        m_s3_pdev->create_chunk(chunk_id, CHUNK_SIZE, type, 1);
        m_nvme_reader->set_chunk_data(chunk_id, make_test_data(CHUNK_SIZE, pattern));
        m_s3_pdev->write(chunk_id, 0, make_test_data(512, pattern));
    }

    bool run_cp_flush() {
        auto cur_cp = std::make_unique< TestCP >();
        auto new_cp = std::make_unique< TestCP >();
        auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
        new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx_ptr));
        return m_callbacks->cp_flush(new_cp.get()).get();
    }

    std::string chunk_s3_key(chunk_id_t chunk_id) const {
        return std::string(VOLUME_ID) + "/chunks/" + std::to_string(chunk_id) + "/data.dat";
    }

    std::string superblock_key() const {
        return std::string(VOLUME_ID) + "/pdev_superblock.bin";
    }

    std::shared_ptr< FailureInjectableS3Store > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::unique_ptr< S3CpCallbacks > m_callbacks;
};

///////////////////////////////////////////////////////////////////////////////
// Test 1.1 — WriteAndCpUploadsToS3
///////////////////////////////////////////////////////////////////////////////
TEST_F(CpIntegTest, WriteAndCpUploadsToS3) {
    constexpr chunk_id_t CID = 10;
    create_chunk_with_data(CID, S3ChunkType::DATA, 0xAA);

    ASSERT_TRUE(m_s3_pdev->is_chunk_dirty(CID));

    auto result = run_cp_flush();
    ASSERT_TRUE(result);

    ASSERT_TRUE(m_s3_store->has_object(chunk_s3_key(CID)));
    ASSERT_TRUE(m_s3_store->has_object(superblock_key()));
    ASSERT_EQ(m_s3_pdev->superblock().generation(), 1u);
    ASSERT_FALSE(m_s3_pdev->is_chunk_dirty(CID));
    ASSERT_EQ(m_s3_pdev->dirty_cache_size_bytes(), 0u);
}

///////////////////////////////////////////////////////////////////////////////
// Test 1.2 — MultipleCPsIncrementGeneration
///////////////////////////////////////////////////////////////////////////////
TEST_F(CpIntegTest, MultipleCPsIncrementGeneration) {
    constexpr chunk_id_t CID = 20;
    m_s3_pdev->create_chunk(CID, CHUNK_SIZE, S3ChunkType::DATA, 1);
    m_nvme_reader->set_chunk_data(CID, make_test_data(CHUNK_SIZE, 0xBB));

    m_s3_pdev->write(CID, 0, make_test_data(256, 0x01));
    ASSERT_TRUE(run_cp_flush());
    ASSERT_EQ(m_s3_pdev->superblock().generation(), 1u);

    m_s3_pdev->write(CID, 256, make_test_data(256, 0x02));
    ASSERT_TRUE(run_cp_flush());
    ASSERT_EQ(m_s3_pdev->superblock().generation(), 2u);

    ASSERT_TRUE(m_s3_store->has_object(chunk_s3_key(CID)));
    ASSERT_TRUE(m_s3_store->has_object(superblock_key()));
}

///////////////////////////////////////////////////////////////////////////////
// Test 1.3 — CPFlushOrderIsCorrect
//   Verify: DATA flushed before METABLK, superblock last
///////////////////////////////////////////////////////////////////////////////
TEST_F(CpIntegTest, CPFlushOrderIsCorrect) {
    constexpr chunk_id_t CID_DATA = 30;
    constexpr chunk_id_t CID_WAL = 31;
    constexpr chunk_id_t CID_INDEX = 32;
    constexpr chunk_id_t CID_META = 33;

    create_chunk_with_data(CID_DATA, S3ChunkType::DATA, 0x01);
    create_chunk_with_data(CID_WAL, S3ChunkType::WAL, 0x02);
    create_chunk_with_data(CID_INDEX, S3ChunkType::INDEX, 0x03);
    create_chunk_with_data(CID_META, S3ChunkType::METABLK, 0x04);

    m_s3_store->clear_order();
    ASSERT_TRUE(run_cp_flush());

    auto order = m_s3_store->put_order();
    ASSERT_GE(order.size(), 5u);

    std::vector< std::string > chunk_puts;
    for (const auto& key : order) {
        if (key.find("/chunks/") != std::string::npos) {
            chunk_puts.push_back(key);
        }
    }
    ASSERT_EQ(chunk_puts.size(), 4u);

    // DATA(30) → WAL(31) → INDEX(32) → METABLK(33)
    EXPECT_NE(chunk_puts[0].find("/chunks/30/"), std::string::npos) << "First should be DATA";
    EXPECT_NE(chunk_puts[1].find("/chunks/31/"), std::string::npos) << "Second should be WAL";
    EXPECT_NE(chunk_puts[2].find("/chunks/32/"), std::string::npos) << "Third should be INDEX";
    EXPECT_NE(chunk_puts[3].find("/chunks/33/"), std::string::npos) << "Fourth should be METABLK";

    EXPECT_NE(order.back().find("pdev_superblock"), std::string::npos)
        << "Superblock must be written last";
}

///////////////////////////////////////////////////////////////////////////////
// Test 1.4 — CPWithPartialS3FailureRetries
///////////////////////////////////////////////////////////////////////////////
TEST_F(CpIntegTest, CPWithPartialS3FailureRetries) {
    constexpr chunk_id_t CID_OK = 40;
    constexpr chunk_id_t CID_FAIL = 41;

    create_chunk_with_data(CID_OK, S3ChunkType::DATA, 0xCC);
    create_chunk_with_data(CID_FAIL, S3ChunkType::DATA, 0xDD);

    m_s3_store->inject_put_failure(chunk_s3_key(CID_FAIL), 1);

    // First CP: partial failure
    auto result1 = run_cp_flush();
    ASSERT_FALSE(result1);

    ASSERT_TRUE(m_s3_store->has_object(chunk_s3_key(CID_OK)));
    ASSERT_FALSE(m_s3_store->has_object(chunk_s3_key(CID_FAIL)));
    ASSERT_FALSE(m_s3_store->has_object(superblock_key()));

    auto failed = m_callbacks->failed_chunks(m_s3_pdev->pdev_id());
    ASSERT_EQ(failed.size(), 1u);
    ASSERT_TRUE(failed.count(CID_FAIL));

    // Second CP: retry succeeds (failure was single-shot)
    auto result2 = run_cp_flush();
    ASSERT_TRUE(result2);

    ASSERT_TRUE(m_s3_store->has_object(chunk_s3_key(CID_FAIL)));
    ASSERT_TRUE(m_s3_store->has_object(superblock_key()));
    ASSERT_TRUE(m_callbacks->failed_chunks(m_s3_pdev->pdev_id()).empty());
}

///////////////////////////////////////////////////////////////////////////////
// Test 1.5 — DirtyCacheThresholdTriggersEarlyCP
///////////////////////////////////////////////////////////////////////////////
TEST_F(CpIntegTest, DirtyCacheThresholdTriggersEarlyCP) {
    // Track whether early CP was triggered
    struct EarlyCpTracker : public S3CpFlushCallback {
        std::atomic< int > trigger_count{0};
        void trigger_early_cp_flush() override { trigger_count.fetch_add(1); }
    };
    auto tracker = std::make_shared< EarlyCpTracker >();

    // Create pdev with very small dirty cache limit (1 KB)
    S3KeyMapper key_mapper{.volume_id = "vol-early-cp"};
    auto chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, key_mapper);
    constexpr uint64_t TINY_CACHE_MB = 0; // 0 MB → threshold = 0 bytes
    auto pdev = std::make_unique< S3PhysicalDev >(
        99, chunk_store, m_s3_store, "vol-early-cp", TINY_CACHE_MB, tracker);

    pdev->create_chunk(50, CHUNK_SIZE, S3ChunkType::DATA, 1);
    m_nvme_reader->set_chunk_data(50, make_test_data(CHUNK_SIZE, 0xEE));

    pdev->write(50, 0, make_test_data(2048, 0xEE));

    ASSERT_GT(tracker->trigger_count.load(), 0)
        << "Early CP should have been triggered when dirty cache exceeded threshold";
}

///////////////////////////////////////////////////////////////////////////////
// Group 3 Fixture: Bulk Recovery
///////////////////////////////////////////////////////////////////////////////
class BulkRecoveryIntegTest : public ::testing::Test {
protected:
    static constexpr uint64_t CHUNK_SIZE = 4096;
    static constexpr const char* VOLUME_ID = "vol-integ-recovery";

    void SetUp() override {
        m_s3_store = std::make_shared< FailureInjectableS3Store >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();
        m_nvme_writer = std::make_shared< MockNvmeRecoveryWriter >();
        m_nvme_writer->set_nvme_state(NvmeState::EMPTY);

        S3KeyMapper key_mapper{.volume_id = VOLUME_ID};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, key_mapper);
    }

    void populate_s3_with_superblock_and_chunks(
        const std::vector< std::pair< chunk_id_t, S3ChunkType > >& chunks) {
        PdevS3Superblock sb;
        sb.set_pdev_id(1);
        sb.set_generation(1);

        S3KeyMapper key_mapper{.volume_id = VOLUME_ID};

        for (const auto& [cid, ctype] : chunks) {
            auto pattern = static_cast< uint8_t >(cid & 0xFF);
            auto data = make_patterned_data(CHUNK_SIZE, pattern);

            auto s3_key = key_mapper.chunk_data_key(cid);
            sisl::io_blob_safe blob(data->size(), 0);
            std::memcpy(blob.bytes(), data->cbytes(), data->size());
            m_s3_store->put_object(s3_key, std::move(blob)).get();

            s3_chunk_entry entry;
            entry.chunk_id = cid;
            entry.chunk_size = CHUNK_SIZE;
            entry.chunk_type = ctype;
            entry.vdev_id = 1;
            entry.generation = 1;
            entry.set_s3_key(s3_key);
            sb.add_chunk(entry);
        }

        sb.write_to_s3(*m_s3_store, VOLUME_ID);
    }

    std::string chunk_s3_key(chunk_id_t chunk_id) const {
        return std::string(VOLUME_ID) + "/chunks/" + std::to_string(chunk_id) + "/data.dat";
    }

    std::shared_ptr< FailureInjectableS3Store > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< MockNvmeRecoveryWriter > m_nvme_writer;
    std::shared_ptr< FullChunkStore > m_chunk_store;
};

///////////////////////////////////////////////////////////////////////////////
// Test 3.1 — BulkRecoveryFromS3
///////////////////////////////////////////////////////////////////////////////
TEST_F(BulkRecoveryIntegTest, BulkRecoveryFromS3) {
    std::vector< std::pair< chunk_id_t, S3ChunkType > > chunks = {
        {1, S3ChunkType::METABLK},
        {2, S3ChunkType::WAL},
        {3, S3ChunkType::INDEX},
        {4, S3ChunkType::DATA},
        {5, S3ChunkType::DATA},
    };
    populate_s3_with_superblock_and_chunks(chunks);

    S3RecoveryManager recovery(m_chunk_store, m_s3_store, m_nvme_writer, VOLUME_ID,
                               2, 3, 0);

    ASSERT_TRUE(recovery.needs_recovery());

    auto result = recovery.recover_from_s3();
    ASSERT_TRUE(result.success) << result.error_message;
    ASSERT_EQ(result.total_chunks, 5u);
    ASSERT_EQ(result.chunks_recovered, 5u);
    ASSERT_EQ(result.chunks_failed, 0u);

    for (const auto& [cid, _] : chunks) {
        ASSERT_TRUE(m_nvme_writer->has_chunk(cid)) << "Chunk " << cid << " not recovered";
        auto pattern = static_cast< uint8_t >(cid & 0xFF);
        auto expected = make_patterned_data(CHUNK_SIZE, pattern);
        ASSERT_EQ(std::memcmp(m_nvme_writer->get_chunk(cid)->cbytes(),
                              expected->cbytes(), CHUNK_SIZE), 0)
            << "Data mismatch for chunk " << cid;
    }
}

///////////////////////////////////////////////////////////////////////////////
// Test 3.2 — BulkRecoveryAbortsOnEssentialFailure
///////////////////////////////////////////////////////////////////////////////
TEST_F(BulkRecoveryIntegTest, BulkRecoveryAbortsOnEssentialFailure) {
    std::vector< std::pair< chunk_id_t, S3ChunkType > > chunks = {
        {1, S3ChunkType::METABLK},
        {2, S3ChunkType::WAL},
        {3, S3ChunkType::DATA},
        {4, S3ChunkType::DATA},
    };
    populate_s3_with_superblock_and_chunks(chunks);

    // Delete the METABLK chunk from S3 to simulate essential failure
    m_s3_store->delete_object(chunk_s3_key(1)).get();

    S3RecoveryManager recovery(m_chunk_store, m_s3_store, m_nvme_writer, VOLUME_ID,
                               2, 1, 0);

    auto result = recovery.recover_from_s3();
    ASSERT_FALSE(result.success);
    ASSERT_GT(result.chunks_failed, 0u);

    // DATA chunks should NOT have been downloaded (fail-fast on essential)
    ASSERT_FALSE(m_nvme_writer->has_chunk(3));
    ASSERT_FALSE(m_nvme_writer->has_chunk(4));
}

///////////////////////////////////////////////////////////////////////////////
// Test 3.3 — RecoveryWithRetry
///////////////////////////////////////////////////////////////////////////////
TEST_F(BulkRecoveryIntegTest, RecoveryWithRetry) {
    std::vector< std::pair< chunk_id_t, S3ChunkType > > chunks = {
        {1, S3ChunkType::METABLK},
        {2, S3ChunkType::DATA},
        {3, S3ChunkType::DATA},
    };
    populate_s3_with_superblock_and_chunks(chunks);

    // Fail GET for chunk 2 once — should succeed on retry
    m_s3_store->inject_get_failure(chunk_s3_key(2), 1);

    S3RecoveryManager recovery(m_chunk_store, m_s3_store, m_nvme_writer, VOLUME_ID,
                               2, 3, 0);

    auto result = recovery.recover_from_s3();
    ASSERT_TRUE(result.success) << result.error_message;
    ASSERT_EQ(result.chunks_recovered, 3u);

    for (const auto& [cid, _] : chunks) {
        ASSERT_TRUE(m_nvme_writer->has_chunk(cid)) << "Chunk " << cid << " not recovered";
    }
}

///////////////////////////////////////////////////////////////////////////////
// Group 7 Fixture: Superblock + Cross-Concern Integrity
///////////////////////////////////////////////////////////////////////////////
class IntegrityIntegTest : public ::testing::Test {
protected:
    static constexpr uint64_t CHUNK_SIZE = 4096;
    static constexpr const char* VOLUME_ID = "vol-integ-integrity";

    void SetUp() override {
        m_s3_store = std::make_shared< FailureInjectableS3Store >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();
        m_nvme_io = std::make_shared< MockNvmeDeviceIO >();
        m_nvme_mgr = std::make_shared< MockNvmeChunkManager >();
        m_nvme_alloc = std::make_shared< MockNvmeChunkAllocator >();

        S3KeyMapper key_mapper{.volume_id = VOLUME_ID};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, key_mapper);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, VOLUME_ID, 4096, nullptr);

        m_eviction_mgr = std::make_shared< ChunkEvictionManager >(m_s3_pdev.get(), m_nvme_mgr);

        m_hydration_mgr = std::make_shared< ChunkHydrationManager >(
            m_chunk_store, m_nvme_io, m_nvme_alloc, m_eviction_mgr, nullptr, 2);

        m_handler = std::make_unique< TieredReadHandler >(
            m_s3_pdev.get(), m_nvme_io,
            TieredReadConfig{.s3_read_fallback_enabled = true, .s3_hydrate_on_read = true},
            m_hydration_mgr);

        m_callbacks = std::make_unique< S3CpCallbacks >(
            std::vector< S3PhysicalDev* >{m_s3_pdev.get()}, 2);
    }

    void TearDown() override {
        m_hydration_mgr->shutdown();
    }

    void create_chunk_with_data(chunk_id_t chunk_id, S3ChunkType type, uint8_t pattern) {
        m_s3_pdev->create_chunk(chunk_id, CHUNK_SIZE, type, 1);
        auto data = make_patterned_data(CHUNK_SIZE, pattern);
        m_nvme_reader->set_chunk_data(chunk_id, data);
        m_nvme_io->store(chunk_id, 0,
                         reinterpret_cast< const char* >(data->cbytes()), CHUNK_SIZE);
        m_nvme_mgr->set_on_nvme(chunk_id, true);
        m_s3_pdev->write(chunk_id, 0, make_test_data(512, pattern));
    }

    bool run_cp_flush() {
        auto cur_cp = std::make_unique< TestCP >();
        auto new_cp = std::make_unique< TestCP >();
        auto ctx_ptr = m_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
        new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx_ptr));
        return m_callbacks->cp_flush(new_cp.get()).get();
    }

    void evict_chunk(chunk_id_t chunk_id) {
        auto result = m_eviction_mgr->evict_chunk(chunk_id, CHUNK_SIZE);
        ASSERT_EQ(result, EvictionResult::SUCCESS);
        m_nvme_io->set_on_nvme(chunk_id, false);
    }

    std::shared_ptr< FailureInjectableS3Store > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< MockNvmeDeviceIO > m_nvme_io;
    std::shared_ptr< MockNvmeChunkManager > m_nvme_mgr;
    std::shared_ptr< MockNvmeChunkAllocator > m_nvme_alloc;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;
    std::shared_ptr< ChunkHydrationManager > m_hydration_mgr;
    std::unique_ptr< TieredReadHandler > m_handler;
    std::unique_ptr< S3CpCallbacks > m_callbacks;
};

///////////////////////////////////////////////////////////////////////////////
// Test 7.1 — SuperblockReflectsAllChunkStates
///////////////////////////////////////////////////////////////////////////////
TEST_F(IntegrityIntegTest, SuperblockReflectsAllChunkStates) {
    create_chunk_with_data(1, S3ChunkType::METABLK, 0x10);
    create_chunk_with_data(2, S3ChunkType::DATA, 0x20);
    create_chunk_with_data(3, S3ChunkType::DATA, 0x30);

    ASSERT_TRUE(run_cp_flush());

    evict_chunk(2);

    // Read the superblock from S3 and verify chunk list
    PdevS3Superblock sb;
    auto sb_result = sb.read_from_s3(*m_s3_store, VOLUME_ID);
    ASSERT_TRUE(sb_result.ok()) << sb_result.error_message;

    ASSERT_EQ(sb.num_chunks(), 3u);
    ASSERT_EQ(sb.generation(), 1u);

    auto* entry1 = sb.find_chunk(1);
    auto* entry2 = sb.find_chunk(2);
    auto* entry3 = sb.find_chunk(3);
    ASSERT_NE(entry1, nullptr);
    ASSERT_NE(entry2, nullptr);
    ASSERT_NE(entry3, nullptr);

    ASSERT_TRUE(entry1->is_metablk());
    ASSERT_TRUE(entry2->is_data());
    ASSERT_TRUE(entry3->is_data());

    // Evicted chunk still in superblock (S3 data still exists)
    ASSERT_TRUE(m_eviction_mgr->is_chunk_evicted(2));
    ASSERT_TRUE(m_s3_store->has_object(entry2->get_s3_key()));
}

///////////////////////////////////////////////////////////////////////////////
// Test 7.2 — RecoveryAfterEvictionCycle
///////////////////////////////////////////////////////////////////////////////
TEST_F(IntegrityIntegTest, RecoveryAfterEvictionCycle) {
    for (uint32_t i = 1; i <= 5; ++i) {
        create_chunk_with_data(i, S3ChunkType::DATA, static_cast< uint8_t >(0x10 * i));
    }
    ASSERT_TRUE(run_cp_flush());

    evict_chunk(2);
    evict_chunk(4);

    // Run another CP to update superblock after eviction state
    m_s3_pdev->write(1, 0, make_test_data(128, 0x10));
    ASSERT_TRUE(run_cp_flush());

    // Simulate NVMe wipe
    auto nvme_writer = std::make_shared< MockNvmeRecoveryWriter >();
    nvme_writer->set_nvme_state(NvmeState::EMPTY);

    S3RecoveryManager recovery(m_chunk_store, m_s3_store, nvme_writer, VOLUME_ID,
                               2, 3, 0);

    ASSERT_TRUE(recovery.needs_recovery());
    auto result = recovery.recover_from_s3();
    ASSERT_TRUE(result.success) << result.error_message;
    ASSERT_EQ(result.chunks_recovered, 5u);

    for (uint32_t i = 1; i <= 5; ++i) {
        ASSERT_TRUE(nvme_writer->has_chunk(i)) << "Chunk " << i << " not recovered";
    }
}

///////////////////////////////////////////////////////////////////////////////
// Test 7.3 — DataIntegrityEndToEnd
//   Full lifecycle: write → CP → S3 → evict → recover → read → verify
///////////////////////////////////////////////////////////////////////////////
TEST_F(IntegrityIntegTest, DataIntegrityEndToEnd) {
    constexpr uint32_t NUM_CHUNKS = 20;

    // Write 20 chunks with unique patterned data
    for (uint32_t i = 1; i <= NUM_CHUNKS; ++i) {
        create_chunk_with_data(i, S3ChunkType::DATA, static_cast< uint8_t >(i));
    }

    ASSERT_TRUE(run_cp_flush());

    // Evict 10 chunks
    for (uint32_t i = 1; i <= 10; ++i) {
        evict_chunk(i);
    }

    // Hydrate 5 of the evicted chunks via tiered read
    for (uint32_t i = 1; i <= 5; ++i) {
        auto [ec, data] = m_handler->async_read(i, 0, CHUNK_SIZE).get();
        ASSERT_FALSE(ec) << "Read failed for chunk " << i << ": " << ec.message();
    }

    // Wait for hydration to complete
    wait_for_condition([this]() {
        for (uint32_t i = 1; i <= 5; ++i) {
            if (!m_nvme_io->is_chunk_on_nvme(i)) return false;
        }
        return true;
    }, 5000);

    // Run another CP
    for (uint32_t i = 1; i <= 5; ++i) {
        m_s3_pdev->write(i, 0, make_test_data(128, static_cast< uint8_t >(i)));
    }
    ASSERT_TRUE(run_cp_flush());

    // Read ALL 20 chunks and verify byte-for-byte data integrity
    for (uint32_t i = 1; i <= NUM_CHUNKS; ++i) {
        auto [ec, data] = m_handler->async_read(i, 0, CHUNK_SIZE).get();
        ASSERT_FALSE(ec) << "Read failed for chunk " << i << ": " << ec.message();
        ASSERT_NE(data, nullptr) << "Null data for chunk " << i;

        // Verify the patterned data matches the original seed
        auto expected = make_patterned_data(CHUNK_SIZE, static_cast< uint8_t >(i));
        ASSERT_EQ(std::memcmp(data->cbytes(), expected->cbytes(), CHUNK_SIZE), 0)
            << "Data integrity failure for chunk " << i;
    }
}

///////////////////////////////////////////////////////////////////////////////
// main
///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_integ_cp_recovery_integrity");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
