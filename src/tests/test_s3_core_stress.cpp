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
 * @file test_s3_core_stress.cpp
 * @brief S3 Core Stress Test — Phases 1-3 full lifecycle under concurrent
 *        pressure against MinIO (or MockS3ObjectStore where MinIO isn't practical).
 *
 * Scenarios:
 *   1. Concurrent writes + CP flush to S3
 *   2. Concurrent tiered reads during eviction/hydration
 *   3. Dirty cache pressure (forced early CP)
 *   4. LRU eviction churn under continuous writes
 *   5. Recovery after simulated crash-during-CP
 *
 * Configurable via command-line:
 *   --num_threads       Writer/reader thread count (default 8)
 *   --num_chunks        Number of chunks to operate on (default 32)
 *   --chunk_size        Per-chunk size in bytes (default 65536)
 *   --stress_duration   Duration per scenario in seconds (default 5)
 */

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <numeric>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
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
#include <homestore/s3/eviction_policy.h>
#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_cp.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/s3_recovery.h>
#include <homestore/s3/tiered_read_handler.h>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#include <folly/init/Init.h>
#pragma GCC diagnostic pop
#include "s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;
using steady_clock = std::chrono::steady_clock;

///////////////////////////////////////////////////////////////////////////////
// Command-line configurable parameters
///////////////////////////////////////////////////////////////////////////////

static uint32_t g_num_threads = 8;
static uint32_t g_num_chunks = 32;
static uint64_t g_chunk_size = 65536;
static uint32_t g_stress_duration_secs = 5;

///////////////////////////////////////////////////////////////////////////////
// FailureInjectableS3Store — extends MockS3ObjectStore with put/get failure
// injection and put ordering tracking.
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
        m_put_count.fetch_add(1, std::memory_order_relaxed);
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
        m_get_count.fetch_add(1, std::memory_order_relaxed);
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

    uint64_t stress_put_count() const { return m_put_count.load(std::memory_order_relaxed); }
    uint64_t stress_get_count() const { return m_get_count.load(std::memory_order_relaxed); }

private:
    mutable std::mutex m_order_mutex;
    std::vector< std::string > m_put_order;

    mutable std::mutex m_fail_mutex;
    std::unordered_map< std::string, uint32_t > m_put_fail_keys;
    std::unordered_map< std::string, uint32_t > m_get_fail_keys;

    std::atomic< uint64_t > m_put_count{0};
    std::atomic< uint64_t > m_get_count{0};
};

///////////////////////////////////////////////////////////////////////////////
// Mock NVMe classes
///////////////////////////////////////////////////////////////////////////////

class MockNvmeChunkReader : public NvmeChunkReader {
public:
    void set_chunk_data(chunk_id_t chunk_id, sisl::byte_array data) {
        std::lock_guard lock{m_mtx};
        m_chunks[chunk_id] = std::move(data);
    }

    void remove_chunk_data(chunk_id_t chunk_id) {
        std::lock_guard lock{m_mtx};
        m_chunks.erase(chunk_id);
    }

    void update_chunk_data(chunk_id_t chunk_id, uint64_t offset, const uint8_t* buf, uint64_t size) {
        std::lock_guard lock{m_mtx};
        auto it = m_chunks.find(chunk_id);
        if (it != m_chunks.end()) {
            auto end = std::min(offset + size, static_cast< uint64_t >(it->second->size()));
            std::memcpy(it->second->bytes() + offset, buf, end - offset);
        }
    }

    std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t chunk_id, uint64_t chunk_size) override {
        std::lock_guard lock{m_mtx};
        auto it = m_chunks.find(chunk_id);
        if (it == m_chunks.end()) {
            return {{.status_code = 404, .error_message = "Not on NVMe", .content_length = 0, .etag = {}, .exists = false}, {}};
        }
        auto copy = sisl::make_byte_array(static_cast< uint32_t >(chunk_size), 0);
        auto sz = std::min(static_cast< uint64_t >(it->second->size()), chunk_size);
        std::memcpy(copy->bytes(), it->second->cbytes(), sz);
        return {{.status_code = 0, .error_message = {}, .content_length = 0, .etag = {}, .exists = false}, std::move(copy)};
    }

private:
    mutable std::mutex m_mtx;
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

    void clear_chunk(chunk_id_t chunk_id) {
        std::lock_guard lock{m_mtx};
        m_on_nvme.erase(chunk_id);
        for (auto it = m_storage.begin(); it != m_storage.end();) {
            if (it->first.substr(0, it->first.find(':')) == std::to_string(chunk_id)) {
                it = m_storage.erase(it);
            } else {
                ++it;
            }
        }
    }

    void wipe_all() {
        std::lock_guard lock{m_mtx};
        m_storage.clear();
        m_on_nvme.clear();
    }

    std::error_code nvme_read(chunk_id_t chunk_id, uint64_t offset,
                              char* buf, uint64_t size) override {
        std::lock_guard lock{m_mtx};
        if (m_on_nvme.find(chunk_id) == m_on_nvme.end()) {
            return std::make_error_code(std::errc::no_such_device);
        }
        auto key = make_key(chunk_id, offset);
        auto it = m_storage.find(key);
        if (it != m_storage.end()) {
            auto to_copy = std::min(size, static_cast< uint64_t >(it->second->size()));
            std::memcpy(buf, it->second->cbytes(), to_copy);
            return {};
        }
        return std::make_error_code(std::errc::no_such_file_or_directory);
    }

    std::error_code nvme_write(chunk_id_t chunk_id, uint64_t offset,
                               const char* buf, uint64_t size) override {
        std::lock_guard lock{m_mtx};
        auto key = make_key(chunk_id, offset);
        auto b = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(b->bytes(), buf, size);
        m_storage[key] = std::move(b);
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
    explicit MockNvmeChunkAllocator(uint64_t total = 64 * 1024 * 1024)
        : m_total{total}, m_used{0} {}

    std::error_code allocate_nvme_chunk(chunk_id_t, uint64_t chunk_size) override {
        std::lock_guard lock{m_mtx};
        if (m_used + chunk_size > m_total) {
            return std::make_error_code(std::errc::no_space_on_device);
        }
        m_used += chunk_size;
        return {};
    }

    void release_nvme_chunk(chunk_id_t, uint64_t chunk_size) override {
        std::lock_guard lock{m_mtx};
        m_used = m_used > chunk_size ? m_used - chunk_size : 0;
    }

    uint64_t free_nvme_space_bytes() const override {
        std::lock_guard lock{m_mtx};
        return m_total > m_used ? m_total - m_used : 0;
    }

    void set_used(uint64_t used) {
        std::lock_guard lock{m_mtx};
        m_used = used;
    }

private:
    mutable std::mutex m_mtx;
    uint64_t m_total;
    uint64_t m_used;
};

class MockEvictionCandidateSelector : public EvictionCandidateSelector {
public:
    void add_candidate(chunk_id_t id, uint64_t size) {
        std::lock_guard lock{m_mtx};
        m_candidates.push_back({id, size});
    }

    bool select_eviction_candidate(chunk_id_t& out_chunk_id, uint64_t& out_chunk_size) override {
        std::lock_guard lock{m_mtx};
        if (m_candidates.empty()) return false;
        auto [id, size] = m_candidates.front();
        m_candidates.erase(m_candidates.begin());
        out_chunk_id = id;
        out_chunk_size = size;
        return true;
    }

private:
    std::mutex m_mtx;
    std::vector< std::pair< chunk_id_t, uint64_t > > m_candidates;
};

class MockNvmeRecoveryWriter : public NvmeRecoveryWriter {
public:
    void set_nvme_state(NvmeState state) { m_state = state; }

    NvmeState check_nvme_state() override { return m_state; }

    std::error_code write_chunk(chunk_id_t chunk_id, const sisl::byte_array& data) override {
        std::lock_guard lock{m_mtx};
        m_written_chunks[chunk_id] = data;
        return {};
    }

    bool has_chunk(chunk_id_t chunk_id) const {
        std::lock_guard lock{m_mtx};
        return m_written_chunks.count(chunk_id) > 0;
    }

    const sisl::byte_array& get_chunk(chunk_id_t chunk_id) const {
        std::lock_guard lock{m_mtx};
        return m_written_chunks.at(chunk_id);
    }

    size_t written_count() const {
        std::lock_guard lock{m_mtx};
        return m_written_chunks.size();
    }

    void clear_written() {
        std::lock_guard lock{m_mtx};
        m_written_chunks.clear();
    }

private:
    NvmeState m_state{NvmeState::EMPTY};
    mutable std::mutex m_mtx;
    std::unordered_map< chunk_id_t, sisl::byte_array > m_written_chunks;
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
    cfg.bucket = "homestore-stress-test";
    cfg.region = "us-east-1";
    cfg.retry_count = 3;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

static void wait_for_condition(std::function< bool() > cond, int max_ms = 5000) {
    for (int elapsed = 0; elapsed < max_ms; elapsed += 10) {
        if (cond()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

static bool is_deadline_passed(steady_clock::time_point deadline) {
    return steady_clock::now() >= deadline;
}

///////////////////////////////////////////////////////////////////////////////
// Stress Test Fixture
///////////////////////////////////////////////////////////////////////////////

class S3CoreStressTest : public ::testing::Test {
protected:
    static constexpr const char* VOLUME_ID = "vol-stress";

    void SetUp() override {
        m_s3_store = std::make_shared< FailureInjectableS3Store >(make_test_config());
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();

        S3KeyMapper key_mapper{.volume_id = VOLUME_ID};
        m_chunk_store = std::make_shared< FullChunkStore >(
            m_s3_store, m_nvme_reader, key_mapper, g_chunk_size * g_num_chunks);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, VOLUME_ID, 1024, nullptr);

        m_nvme_mgr = std::make_shared< MockNvmeChunkManager >();
        m_nvme_io = std::make_shared< MockNvmeDeviceIO >();
        m_nvme_alloc = std::make_shared< MockNvmeChunkAllocator >(g_chunk_size * g_num_chunks * 2);
        m_eviction_selector = std::make_shared< MockEvictionCandidateSelector >();

        m_eviction_mgr = std::make_shared< ChunkEvictionManager >(m_s3_pdev.get(), m_nvme_mgr);

        m_hydration_mgr = std::make_shared< ChunkHydrationManager >(
            m_chunk_store, m_nvme_io, m_nvme_alloc, m_eviction_mgr,
            m_eviction_selector, std::max(2u, g_num_threads / 2));

        m_callbacks = std::make_unique< S3CpCallbacks >(
            std::vector< S3PhysicalDev* >{m_s3_pdev.get()},
            std::max(2u, g_num_threads / 2));

        m_deadline = steady_clock::now() + std::chrono::seconds(g_stress_duration_secs);
    }

    void TearDown() override {
        m_hydration_mgr->shutdown();
    }

    void setup_chunk(chunk_id_t chunk_id, uint8_t pattern) {
        m_s3_pdev->create_chunk(chunk_id, g_chunk_size, S3ChunkType::DATA, 1);
        auto data = make_patterned_data(static_cast< uint32_t >(g_chunk_size), pattern);
        m_nvme_reader->set_chunk_data(chunk_id, data);
        m_chunk_store->put(chunk_id, {}, g_chunk_size);
        m_nvme_io->store(chunk_id, 0,
                         reinterpret_cast< const char* >(data->cbytes()), g_chunk_size);
        m_nvme_mgr->set_on_nvme(chunk_id, true);
    }

    void setup_all_chunks() {
        for (uint32_t i = 0; i < g_num_chunks; ++i) {
            setup_chunk(base_chunk_id + i, static_cast< uint8_t >(i & 0xFF));
        }
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

    static constexpr chunk_id_t base_chunk_id = 100;

    std::shared_ptr< FailureInjectableS3Store > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::shared_ptr< MockNvmeChunkManager > m_nvme_mgr;
    std::shared_ptr< MockNvmeDeviceIO > m_nvme_io;
    std::shared_ptr< MockNvmeChunkAllocator > m_nvme_alloc;
    std::shared_ptr< MockEvictionCandidateSelector > m_eviction_selector;
    std::shared_ptr< ChunkEvictionManager > m_eviction_mgr;
    std::shared_ptr< ChunkHydrationManager > m_hydration_mgr;
    std::unique_ptr< S3CpCallbacks > m_callbacks;
    steady_clock::time_point m_deadline;
};

///////////////////////////////////////////////////////////////////////////////
// Scenario 1 — Concurrent Writes + CP Flush to S3
//
// Multiple writer threads bombard S3PhysicalDev::write() while a separate
// thread periodically triggers CP flush. Verify no data corruption, no
// missed uploads, and superblock generation monotonically increases.
///////////////////////////////////////////////////////////////////////////////

TEST_F(S3CoreStressTest, ConcurrentWritesPlusCpFlush) {
    setup_all_chunks();

    std::atomic< uint64_t > total_writes{0};
    std::atomic< uint64_t > total_cps{0};
    std::atomic< int > errors{0};
    std::atomic< bool > done{false};

    std::vector< std::thread > writers;
    for (uint32_t t = 0; t < g_num_threads; ++t) {
        writers.emplace_back([&, t]() {
            std::mt19937 rng{t + 42};
            std::uniform_int_distribution< uint32_t > chunk_dist(0, g_num_chunks - 1);
            std::uniform_int_distribution< uint32_t > offset_dist(0, static_cast< uint32_t >(g_chunk_size) - 256);

            while (!done.load(std::memory_order_relaxed)) {
                chunk_id_t cid = base_chunk_id + chunk_dist(rng);
                uint32_t offset = offset_dist(rng);
                uint8_t pattern = static_cast< uint8_t >(t + total_writes.load(std::memory_order_relaxed));

                auto data = make_test_data(256, pattern);
                m_s3_pdev->write(cid, offset, data);
                m_nvme_reader->update_chunk_data(cid, offset, data->cbytes(), 256);
                total_writes.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::thread cp_thread([&]() {
        uint64_t last_gen = 0;
        while (!done.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            bool ok = run_cp_flush();
            if (!ok) {
                errors.fetch_add(1, std::memory_order_relaxed);
            }
            total_cps.fetch_add(1, std::memory_order_relaxed);

            auto gen = m_s3_pdev->superblock().generation();
            if (gen < last_gen) {
                GTEST_NONFATAL_FAILURE_("Superblock generation went backwards");
                errors.fetch_add(1, std::memory_order_relaxed);
            }
            last_gen = gen;
        }
    });

    std::this_thread::sleep_until(m_deadline);
    done.store(true, std::memory_order_relaxed);

    for (auto& w : writers) { w.join(); }
    cp_thread.join();

    bool final_flush = run_cp_flush();
    ASSERT_TRUE(final_flush) << "Final CP flush must succeed";

    ASSERT_EQ(errors.load(), 0) << "No errors during concurrent writes + CP";
    EXPECT_GT(total_writes.load(), 0u);
    EXPECT_GT(total_cps.load(), 0u);
    EXPECT_GT(m_s3_pdev->superblock().generation(), 0u);
    EXPECT_TRUE(m_s3_store->has_object(superblock_key()));

    for (uint32_t i = 0; i < g_num_chunks; ++i) {
        EXPECT_TRUE(m_s3_store->has_object(chunk_s3_key(base_chunk_id + i)))
            << "Chunk " << (base_chunk_id + i) << " must be on S3 after final flush";
    }

    LOGINFOMOD(s3,"Scenario 1 complete: {} writes, {} CPs, final gen={}",
                  total_writes.load(), total_cps.load(), m_s3_pdev->superblock().generation());
}

///////////////////////////////////////////////////////////////////////////////
// Scenario 2 — Concurrent Tiered Reads During Eviction/Hydration
//
// Half the chunks start evicted. Reader threads issue tiered reads while
// hydration and eviction run concurrently. All reads must succeed (from
// either NVMe or S3 fallback).
///////////////////////////////////////////////////////////////////////////////

TEST_F(S3CoreStressTest, ConcurrentTieredReadsDuringEvictionHydration) {
    setup_all_chunks();

    for (uint32_t i = 0; i < g_num_chunks / 2; ++i) {
        chunk_id_t cid = base_chunk_id + i;
        auto r = m_eviction_mgr->evict_chunk(cid, g_chunk_size);
        ASSERT_EQ(r, EvictionResult::SUCCESS);
        m_nvme_io->set_on_nvme(cid, false);
    }

    TieredReadHandler handler{m_s3_pdev.get(), m_nvme_io,
                              {.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false},
                              m_hydration_mgr};

    std::atomic< uint64_t > total_reads{0};
    std::atomic< uint64_t > total_hydrations{0};
    std::atomic< uint64_t > total_evictions{0};
    std::atomic< int > read_errors{0};
    std::atomic< bool > done{false};

    std::vector< std::thread > readers;
    for (uint32_t t = 0; t < g_num_threads; ++t) {
        readers.emplace_back([&, t]() {
            std::mt19937 rng{t + 100};
            std::uniform_int_distribution< uint32_t > chunk_dist(0, g_num_chunks - 1);

            while (!done.load(std::memory_order_relaxed)) {
                chunk_id_t cid = base_chunk_id + chunk_dist(rng);
                auto [ec, data] = handler.async_read(cid, 0, 64).get();
                if (ec) {
                    read_errors.fetch_add(1, std::memory_order_relaxed);
                }
                total_reads.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::thread hydrator([&]() {
        std::mt19937 rng{200};
        std::uniform_int_distribution< uint32_t > chunk_dist(0, g_num_chunks / 2 - 1);
        while (!done.load(std::memory_order_relaxed)) {
            chunk_id_t cid = base_chunk_id + chunk_dist(rng);
            if (m_eviction_mgr->is_chunk_evicted(cid)) {
                auto r = m_hydration_mgr->hydrate_sync(cid, g_chunk_size);
                if (r == HydrationResult::SUCCESS) {
                    m_nvme_mgr->set_on_nvme(cid, true);
                    total_hydrations.fetch_add(1, std::memory_order_relaxed);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
    });

    std::thread evictor([&]() {
        std::mt19937 rng{300};
        std::uniform_int_distribution< uint32_t > chunk_dist(0, g_num_chunks - 1);
        while (!done.load(std::memory_order_relaxed)) {
            chunk_id_t cid = base_chunk_id + chunk_dist(rng);
            if (!m_eviction_mgr->is_chunk_evicted(cid) && m_nvme_mgr->is_chunk_on_nvme(cid)) {
                auto r = m_eviction_mgr->evict_chunk(cid, g_chunk_size);
                if (r == EvictionResult::SUCCESS) {
                    m_nvme_io->set_on_nvme(cid, false);
                    total_evictions.fetch_add(1, std::memory_order_relaxed);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    });

    std::this_thread::sleep_until(m_deadline);
    done.store(true, std::memory_order_relaxed);

    for (auto& r : readers) { r.join(); }
    hydrator.join();
    evictor.join();

    ASSERT_EQ(read_errors.load(), 0) << "All tiered reads must succeed";
    EXPECT_GT(total_reads.load(), 0u);
    EXPECT_GT(total_hydrations.load(), 0u);
    EXPECT_GT(total_evictions.load(), 0u);

    LOGINFOMOD(s3,"Scenario 2 complete: {} reads, {} hydrations, {} evictions",
                  total_reads.load(), total_hydrations.load(), total_evictions.load());
}

///////////////////////////////////////////////////////////////////////////////
// Scenario 3 — Dirty Cache Pressure (Forced Early CP)
//
// Use a small dirty_cache_max_mb to force S3PhysicalDev to trigger early CP
// callbacks. Multiple writers flood the dirty cache while we count how many
// early CP triggers fire.
///////////////////////////////////////////////////////////////////////////////

class EarlyCpCounter : public S3CpFlushCallback {
public:
    void trigger_early_cp_flush() override {
        m_count.fetch_add(1, std::memory_order_relaxed);
    }
    uint64_t count() const { return m_count.load(std::memory_order_relaxed); }

private:
    std::atomic< uint64_t > m_count{0};
};

TEST_F(S3CoreStressTest, DirtyCachePressureForcesEarlyCp) {
    auto early_cp_counter = std::make_shared< EarlyCpCounter >();

    auto small_pdev = std::make_unique< S3PhysicalDev >(
        2, m_chunk_store, m_s3_store, VOLUME_ID,
        1, // 1 MB dirty cache max — very small to force triggers
        early_cp_counter);

    auto small_callbacks = std::make_unique< S3CpCallbacks >(
        std::vector< S3PhysicalDev* >{small_pdev.get()}, 4);

    for (uint32_t i = 0; i < g_num_chunks; ++i) {
        chunk_id_t cid = base_chunk_id + i;
        small_pdev->create_chunk(cid, g_chunk_size, S3ChunkType::DATA, 1);
        m_nvme_reader->set_chunk_data(cid, make_test_data(static_cast< uint32_t >(g_chunk_size),
                                                          static_cast< uint8_t >(i)));
    }

    std::atomic< uint64_t > total_writes{0};
    std::atomic< bool > done{false};

    std::vector< std::thread > writers;
    for (uint32_t t = 0; t < g_num_threads; ++t) {
        writers.emplace_back([&, t]() {
            std::mt19937 rng{t + 500};
            std::uniform_int_distribution< uint32_t > chunk_dist(0, g_num_chunks - 1);

            while (!done.load(std::memory_order_relaxed)) {
                chunk_id_t cid = base_chunk_id + chunk_dist(rng);
                uint32_t write_size = std::min(4096u, static_cast< uint32_t >(g_chunk_size));
                auto data = make_test_data(write_size, static_cast< uint8_t >(t));
                small_pdev->write(cid, 0, data);
                total_writes.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::thread drain_thread([&]() {
        while (!done.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            auto cur_cp = std::make_unique< TestCP >();
            auto new_cp = std::make_unique< TestCP >();
            auto ctx = small_callbacks->on_switchover_cp(cur_cp.get(), new_cp.get());
            new_cp->set_context(cp_consumer_t::S3_SVC, std::move(ctx));
            small_callbacks->cp_flush(new_cp.get()).get();
        }
    });

    std::this_thread::sleep_until(m_deadline);
    done.store(true, std::memory_order_relaxed);

    for (auto& w : writers) { w.join(); }
    drain_thread.join();

    EXPECT_GT(early_cp_counter->count(), 0u)
        << "Small dirty cache should have triggered at least one early CP";
    EXPECT_GT(total_writes.load(), 0u);

    LOGINFOMOD(s3,"Scenario 3 complete: {} writes, {} early CP triggers, final dirty_cache={}B",
                  total_writes.load(), early_cp_counter->count(),
                  small_pdev->dirty_cache_size_bytes());
}

///////////////////////////////////////////////////////////////////////////////
// Scenario 4 — LRU Eviction Churn Under Continuous Writes
//
// Set NVMe capacity to hold only half the chunks. Run the EvictionPolicyEngine
// monitor while writers continuously generate dirty data and trigger CP flushes.
// The eviction engine must keep NVMe usage under the threshold despite churn.
///////////////////////////////////////////////////////////////////////////////

TEST_F(S3CoreStressTest, LruEvictionChurnUnderContinuousWrites) {
    setup_all_chunks();

    uint64_t nvme_capacity = g_chunk_size * (g_num_chunks / 2);
    auto tight_alloc = std::make_shared< MockNvmeChunkAllocator >(nvme_capacity);
    tight_alloc->set_used(g_chunk_size * g_num_chunks);

    EvictionPolicyConfig policy_cfg;
    policy_cfg.nvme_capacity_threshold = 0.8f;
    policy_cfg.cooldown_secs = 0;
    policy_cfg.monitor_interval_secs = 1;

    auto policy_engine = std::make_unique< EvictionPolicyEngine >(
        m_eviction_mgr, tight_alloc, nvme_capacity, policy_cfg);

    for (uint32_t i = 0; i < g_num_chunks; ++i) {
        policy_engine->record_access(base_chunk_id + i, g_chunk_size);
        policy_engine->mark_on_s3(base_chunk_id + i);
    }

    policy_engine->start_monitor();

    std::atomic< uint64_t > total_writes{0};
    std::atomic< uint64_t > total_accesses{0};
    std::atomic< bool > done{false};

    std::vector< std::thread > writers;
    for (uint32_t t = 0; t < g_num_threads; ++t) {
        writers.emplace_back([&, t]() {
            std::mt19937 rng{t + 700};
            std::uniform_int_distribution< uint32_t > chunk_dist(0, g_num_chunks - 1);

            while (!done.load(std::memory_order_relaxed)) {
                chunk_id_t cid = base_chunk_id + chunk_dist(rng);
                policy_engine->record_access(cid, g_chunk_size);
                total_accesses.fetch_add(1, std::memory_order_relaxed);

                if (!m_eviction_mgr->is_chunk_evicted(cid)) {
                    auto data = make_test_data(256, static_cast< uint8_t >(t));
                    m_s3_pdev->write(cid, 0, data);
                    m_nvme_reader->update_chunk_data(cid, 0, data->cbytes(), 256);
                    total_writes.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    std::thread cp_thread([&]() {
        while (!done.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            run_cp_flush();
        }
    });

    std::this_thread::sleep_until(m_deadline);
    done.store(true, std::memory_order_relaxed);

    for (auto& w : writers) { w.join(); }
    cp_thread.join();
    policy_engine->stop_monitor();

    uint32_t evicted_count = 0;
    for (uint32_t i = 0; i < g_num_chunks; ++i) {
        if (m_eviction_mgr->is_chunk_evicted(base_chunk_id + i)) {
            evicted_count++;
        }
    }

    EXPECT_GT(evicted_count, 0u) << "Eviction monitor should have evicted at least some chunks";
    EXPECT_GT(total_writes.load(), 0u);
    EXPECT_GT(total_accesses.load(), 0u);

    LOGINFOMOD(s3,"Scenario 4 complete: {} writes, {} accesses, {} chunks evicted out of {}",
                  total_writes.load(), total_accesses.load(), evicted_count, g_num_chunks);
}

///////////////////////////////////////////////////////////////////////////////
// Scenario 5 — Recovery After Simulated Crash-During-CP
//
// 1. Write data to chunks and flush a successful CP (gen 1)
// 2. Write more data, start a CP flush but inject failures on some chunks
//    so the CP partially fails (superblock not written for gen 2)
// 3. Simulate crash by wiping NVMe state
// 4. Recover from S3 — should recover to gen 1 state since gen 2 CP
//    was incomplete
///////////////////////////////////////////////////////////////////////////////

TEST_F(S3CoreStressTest, RecoveryAfterCrashDuringCp) {
    uint32_t recovery_chunks = std::min(g_num_chunks, 16u);

    for (uint32_t i = 0; i < recovery_chunks; ++i) {
        chunk_id_t cid = base_chunk_id + i;
        S3ChunkType type = (i == 0) ? S3ChunkType::METABLK :
                           (i == 1) ? S3ChunkType::WAL :
                           (i == 2) ? S3ChunkType::INDEX : S3ChunkType::DATA;

        m_s3_pdev->create_chunk(cid, g_chunk_size, type, 1);

        uint8_t pattern = static_cast< uint8_t >(0x10 + i);
        auto data = make_patterned_data(static_cast< uint32_t >(g_chunk_size), pattern);
        m_nvme_reader->set_chunk_data(cid, data);

        m_s3_pdev->write(cid, 0, make_test_data(static_cast< uint32_t >(g_chunk_size / 2), pattern));
    }

    ASSERT_TRUE(run_cp_flush()) << "Gen 1 CP must succeed";
    uint64_t gen1 = m_s3_pdev->superblock().generation();
    ASSERT_GE(gen1, 1u);

    for (uint32_t i = 0; i < recovery_chunks; ++i) {
        ASSERT_TRUE(m_s3_store->has_object(chunk_s3_key(base_chunk_id + i)))
            << "All chunks should be on S3 after gen 1";
    }
    ASSERT_TRUE(m_s3_store->has_object(superblock_key()));

    for (uint32_t i = 0; i < recovery_chunks; ++i) {
        chunk_id_t cid = base_chunk_id + i;
        uint8_t new_pattern = static_cast< uint8_t >(0x80 + i);
        auto new_data = make_test_data(static_cast< uint32_t >(g_chunk_size / 4), new_pattern);
        m_s3_pdev->write(cid, 0, new_data);
        m_nvme_reader->update_chunk_data(cid, 0, new_data->cbytes(),
                                         static_cast< uint32_t >(g_chunk_size / 4));
    }

    uint32_t half = recovery_chunks / 2;
    for (uint32_t i = half; i < recovery_chunks; ++i) {
        m_s3_store->inject_put_failure(chunk_s3_key(base_chunk_id + i), 100);
    }

    bool cp2_result = run_cp_flush();
    EXPECT_FALSE(cp2_result) << "CP 2 should fail due to injected put failures";

    m_s3_store->clear_failures();

    auto nvme_writer = std::make_shared< MockNvmeRecoveryWriter >();
    nvme_writer->set_nvme_state(NvmeState::EMPTY);

    S3RecoveryManager recovery_mgr(
        m_chunk_store, m_s3_store, nvme_writer, VOLUME_ID, 4, 1, 0);

    ASSERT_TRUE(recovery_mgr.needs_recovery());

    auto result = recovery_mgr.recover_from_s3();
    ASSERT_TRUE(result.success) << "Recovery must succeed: " << result.error_message;
    ASSERT_EQ(result.chunks_recovered, recovery_chunks);

    auto& recovered_sb = recovery_mgr.superblock();
    EXPECT_EQ(recovered_sb.generation(), gen1)
        << "Recovered superblock should be gen 1 (crash prevented gen 2 superblock write)";

    for (uint32_t i = 0; i < recovery_chunks; ++i) {
        chunk_id_t cid = base_chunk_id + i;
        ASSERT_TRUE(nvme_writer->has_chunk(cid))
            << "Chunk " << cid << " must be recovered to NVMe";

        auto& recovered_data = nvme_writer->get_chunk(cid);
        ASSERT_NE(recovered_data, nullptr);
        ASSERT_GT(recovered_data->size(), 0u);
    }

    LOGINFOMOD(s3,"Scenario 5 complete: recovered {} chunks at gen {}, "
                  "total bytes={}",
                  result.chunks_recovered, recovered_sb.generation(),
                  result.total_bytes);
}

///////////////////////////////////////////////////////////////////////////////
// Scenario 5b — Multi-threaded concurrent writes → crash → recovery integrity
//
// Same as Scenario 5 but with concurrent writers during the successful CP
// to stress the dirty cache drain under load.
///////////////////////////////////////////////////////////////////////////////

TEST_F(S3CoreStressTest, ConcurrentWritesCrashRecoveryIntegrity) {
    uint32_t recovery_chunks = std::min(g_num_chunks, 16u);

    for (uint32_t i = 0; i < recovery_chunks; ++i) {
        chunk_id_t cid = base_chunk_id + i;
        m_s3_pdev->create_chunk(cid, g_chunk_size, S3ChunkType::DATA, 1);

        uint8_t pattern = static_cast< uint8_t >(0x30 + i);
        auto data = make_patterned_data(static_cast< uint32_t >(g_chunk_size), pattern);
        m_nvme_reader->set_chunk_data(cid, data);
    }

    std::atomic< bool > done{false};
    std::atomic< uint64_t > writes{0};

    std::vector< std::thread > writers;
    for (uint32_t t = 0; t < std::min(g_num_threads, 4u); ++t) {
        writers.emplace_back([&, t]() {
            std::mt19937 rng{t + 900};
            std::uniform_int_distribution< uint32_t > chunk_dist(0, recovery_chunks - 1);
            while (!done.load(std::memory_order_relaxed)) {
                chunk_id_t cid = base_chunk_id + chunk_dist(rng);
                auto data = make_test_data(128, static_cast< uint8_t >(t));
                m_s3_pdev->write(cid, 0, data);
                writes.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    ASSERT_TRUE(run_cp_flush()) << "CP under concurrent writes must succeed";
    uint64_t gen = m_s3_pdev->superblock().generation();

    done.store(true, std::memory_order_relaxed);
    for (auto& w : writers) { w.join(); }

    auto nvme_writer = std::make_shared< MockNvmeRecoveryWriter >();
    nvme_writer->set_nvme_state(NvmeState::EMPTY);

    S3RecoveryManager recovery_mgr(
        m_chunk_store, m_s3_store, nvme_writer, VOLUME_ID, 4, 1, 0);

    auto result = recovery_mgr.recover_from_s3();
    ASSERT_TRUE(result.success);
    EXPECT_EQ(result.chunks_recovered, recovery_chunks);
    EXPECT_EQ(recovery_mgr.superblock().generation(), gen);

    LOGINFOMOD(s3,"Scenario 5b complete: {} writes during CP, recovered {} chunks at gen {}",
                  writes.load(), result.chunks_recovered, gen);
}

///////////////////////////////////////////////////////////////////////////////
// main
///////////////////////////////////////////////////////////////////////////////

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    folly::Init follyInit(&argc, &argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_core_stress");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");

    for (int i = 1; i < argc; ++i) {
        std::string arg{argv[i]};
        if (arg.find("--num_threads=") == 0) {
            g_num_threads = static_cast< uint32_t >(std::stoul(arg.substr(14)));
        } else if (arg.find("--num_chunks=") == 0) {
            g_num_chunks = static_cast< uint32_t >(std::stoul(arg.substr(13)));
        } else if (arg.find("--chunk_size=") == 0) {
            g_chunk_size = std::stoull(arg.substr(13));
        } else if (arg.find("--stress_duration=") == 0) {
            g_stress_duration_secs = static_cast< uint32_t >(std::stoul(arg.substr(18)));
        }
    }

    LOGINFOMOD(s3,"S3 Core Stress Test configuration: threads={}, chunks={}, "
                  "chunk_size={}, duration={}s",
                  g_num_threads, g_num_chunks, g_chunk_size, g_stress_duration_secs);

    return RUN_ALL_TESTS();
}
