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
 * @file test_s3_minio_stress.cpp
 * @brief Stress/load tests for the core S3 pipeline (Phases 1-3) against real MinIO.
 *
 * Exercises: concurrent chunk uploads, CP flush under pressure,
 *            superblock integrity, recovery, tiered read under concurrent writes.
 *
 * Requires: MinIO running on localhost:9000
 *           AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
 * Gated behind: -DENABLE_MINIO_TESTS=ON
 */

#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
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

#include <homestore/s3/aws_s3_object_store.h>

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Constants
///////////////////////////////////////////////////////////////////////////////
static constexpr const char* MINIO_ENDPOINT = "http://localhost:9000";
static constexpr const char* MINIO_REGION = "us-east-1";
static constexpr uint64_t TEST_CHUNK_SIZE = 64 * 1024; // 64 KB

///////////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////////
static sisl::byte_array make_patterned_data(uint32_t size, uint8_t seed = 0) {
    auto buf = sisl::make_byte_array(size, 0);
    for (uint32_t i = 0; i < size; ++i) {
        buf->bytes()[i] = static_cast< uint8_t >((seed + i) & 0xFF);
    }
    return buf;
}

static S3ObjectStoreConfig make_minio_config(const std::string& bucket) {
    S3ObjectStoreConfig cfg;
    cfg.bucket = bucket;
    cfg.region = MINIO_REGION;
    cfg.endpoint = MINIO_ENDPOINT;
    cfg.retry_count = 3;
    cfg.retry_backoff_ms = 100;
    return cfg;
}

static std::string make_unique_volume_id(const std::string& test_name) {
    static std::atomic< uint32_t > counter{0};
    auto now = std::chrono::system_clock::now().time_since_epoch();
    auto ms = std::chrono::duration_cast< std::chrono::milliseconds >(now).count();
    return "stress-" + test_name + "-" + std::to_string(ms) + "-" + std::to_string(counter.fetch_add(1));
}

static std::shared_ptr< S3ObjectStore > create_minio_store(const std::string& bucket) {
    return std::make_shared< AwsS3ObjectStore >(make_minio_config(bucket));
}

///////////////////////////////////////////////////////////////////////////////
// Mock NVMe layer
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

    std::pair< S3Result, sisl::byte_array > read_full_chunk(chunk_id_t chunk_id, uint64_t chunk_size) override {
        std::lock_guard lock{m_mtx};
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
    mutable std::mutex m_mtx;
    std::unordered_map< chunk_id_t, sisl::byte_array > m_chunks;
};

class MockNvmeDeviceIO : public NvmeDeviceIO {
public:
    void store(chunk_id_t chunk_id, uint64_t offset, const char* data, uint64_t size) {
        std::lock_guard lock{m_mtx};
        auto key = std::to_string(chunk_id) + ":" + std::to_string(offset);
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

    std::error_code nvme_read(chunk_id_t chunk_id, uint64_t offset_in_chunk, char* buf,
                              uint64_t size) override {
        std::lock_guard lock{m_mtx};
        if (m_on_nvme.count(chunk_id) == 0) {
            return std::make_error_code(std::errc::no_such_file_or_directory);
        }
        auto key = std::to_string(chunk_id) + ":" + std::to_string(offset_in_chunk);
        auto it = m_storage.find(key);
        if (it == m_storage.end()) {
            std::memset(buf, 0, size);
            return {};
        }
        auto sz = std::min(static_cast< uint64_t >(it->second->size()), size);
        std::memcpy(buf, it->second->cbytes(), sz);
        return {};
    }

    std::error_code nvme_write(chunk_id_t chunk_id, uint64_t offset_in_chunk, const char* data,
                               uint64_t size) override {
        store(chunk_id, offset_in_chunk, data, size);
        return {};
    }

    bool is_chunk_on_nvme(chunk_id_t chunk_id) const override {
        std::lock_guard lock{m_mtx};
        return m_on_nvme.count(chunk_id) > 0;
    }

private:
    mutable std::mutex m_mtx;
    std::unordered_map< std::string, sisl::byte_array > m_storage;
    std::set< chunk_id_t > m_on_nvme;
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

class MockNvmeChunkAllocator : public NvmeChunkAllocator {
public:
    std::error_code allocate_nvme_chunk(chunk_id_t, uint64_t) override { return {}; }
    void free_nvme_chunk(chunk_id_t) override {}
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

    sisl::byte_array get_chunk(chunk_id_t chunk_id) const {
        std::lock_guard lock{m_mtx};
        auto it = m_written_chunks.find(chunk_id);
        return (it != m_written_chunks.end()) ? it->second : nullptr;
    }

    size_t chunk_count() const {
        std::lock_guard lock{m_mtx};
        return m_written_chunks.size();
    }

private:
    mutable std::mutex m_mtx;
    NvmeState m_state{NvmeState::EMPTY};
    std::unordered_map< chunk_id_t, sisl::byte_array > m_written_chunks;
};

class MockCpFlushCallback : public S3CpFlushCallback {
public:
    void trigger_early_cp_flush() override { m_triggered.fetch_add(1); }
    uint32_t trigger_count() const { return m_triggered.load(); }

private:
    std::atomic< uint32_t > m_triggered{0};
};

///////////////////////////////////////////////////////////////////////////////
// Test Fixture
///////////////////////////////////////////////////////////////////////////////
class S3MinioStressTest : public ::testing::Test {
protected:
    static constexpr const char* SHARED_BUCKET = "homestore-test";

    void SetUp() override { m_s3_store = create_minio_store(SHARED_BUCKET); }

    void TearDown() override {
        if (m_s3_store && !m_volume_id.empty()) {
            cleanup_volume(m_volume_id);
        }
    }

    void cleanup_volume(const std::string& vol_id) {
        auto list_result = m_s3_store->list_objects(vol_id + "/").get();
        if (list_result.result.ok()) {
            std::vector< std::string > keys;
            for (const auto& obj : list_result.objects) {
                keys.push_back(obj.key);
            }
            if (!keys.empty()) {
                m_s3_store->delete_objects(keys).get();
            }
        }
    }

    void init_test(const std::string& test_name) {
        m_volume_id = make_unique_volume_id(test_name);
        m_key_mapper = S3KeyMapper{m_volume_id};
        m_nvme_reader = std::make_shared< MockNvmeChunkReader >();
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_reader, m_key_mapper);
        m_cp_flush_cb = std::make_shared< MockCpFlushCallback >();
    }

    struct ChunkInfo {
        chunk_id_t id;
        S3ChunkType type;
        sisl::byte_array data;
    };

    void create_chunks(S3PhysicalDev& pdev, const std::vector< ChunkInfo >& chunks) {
        for (const auto& c : chunks) {
            pdev.create_chunk(c.id, TEST_CHUNK_SIZE, c.type, 0);
            m_nvme_reader->set_chunk_data(c.id, c.data);
        }
    }

    std::string m_volume_id;
    S3KeyMapper m_key_mapper;
    std::shared_ptr< S3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_reader;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::shared_ptr< MockCpFlushCallback > m_cp_flush_cb;
};

///////////////////////////////////////////////////////////////////////////////
// Stress Test 1: Concurrent Chunk Uploads
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioStressTest, ConcurrentChunkUploads) {
    init_test("concurrent-uploads");

    static constexpr int NUM_THREADS = 8;
    static constexpr int CHUNKS_PER_THREAD = 5;
    static constexpr int TOTAL_CHUNKS = NUM_THREADS * CHUNKS_PER_THREAD;

    S3PhysicalDev pdev(1, m_chunk_store, m_s3_store, m_volume_id, 4096, m_cp_flush_cb);

    std::vector< ChunkInfo > all_chunks;
    for (int i = 0; i < TOTAL_CHUNKS; ++i) {
        chunk_id_t cid = static_cast< chunk_id_t >(100 + i);
        auto data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), static_cast< uint8_t >(i));
        all_chunks.push_back({cid, S3ChunkType::DATA, data});
    }
    create_chunks(pdev, all_chunks);

    std::atomic< uint64_t > success_count{0};
    std::atomic< uint64_t > fail_count{0};

    std::vector< std::thread > threads;
    threads.reserve(NUM_THREADS);

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < CHUNKS_PER_THREAD; ++i) {
                int idx = t * CHUNKS_PER_THREAD + i;
                auto cid = all_chunks[idx].id;
                auto result = m_chunk_store->put(cid, TEST_CHUNK_SIZE, {}).get();
                if (result.ok()) {
                    success_count.fetch_add(1);
                } else {
                    fail_count.fetch_add(1);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), static_cast< uint64_t >(TOTAL_CHUNKS));
    EXPECT_EQ(fail_count.load(), 0u);

    // Verify all chunks are readable from S3
    for (const auto& c : all_chunks) {
        m_nvme_reader->remove_chunk_data(c.id);
        auto [result, data] = m_chunk_store->get(c.id, 0, TEST_CHUNK_SIZE).get();
        ASSERT_TRUE(result.ok()) << "Failed to read chunk " << c.id;
        ASSERT_NE(data, nullptr);
        EXPECT_EQ(std::memcmp(c.data->cbytes(), data->cbytes(), TEST_CHUNK_SIZE), 0)
            << "Data mismatch for chunk " << c.id;
    }
}

///////////////////////////////////////////////////////////////////////////////
// Stress Test 2: Rapid CP Flush Cycles
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioStressTest, RapidCpFlushCycles) {
    init_test("rapid-cp");

    static constexpr int NUM_CHUNKS = 8;
    static constexpr int NUM_CP_CYCLES = 10;

    S3PhysicalDev pdev(1, m_chunk_store, m_s3_store, m_volume_id, 4096, m_cp_flush_cb);

    std::vector< ChunkInfo > chunks;
    for (int i = 0; i < NUM_CHUNKS; ++i) {
        chunk_id_t cid = static_cast< chunk_id_t >(200 + i);
        auto data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), static_cast< uint8_t >(i));
        chunks.push_back({cid, (i < 2) ? S3ChunkType::METABLK : S3ChunkType::DATA, data});
    }
    create_chunks(pdev, chunks);

    std::vector< S3PhysicalDev* > pdevs = {&pdev};
    S3CpCallbacks cp_callbacks(pdevs);

    for (int cycle = 0; cycle < NUM_CP_CYCLES; ++cycle) {
        // Mutate chunk data each cycle
        for (auto& c : chunks) {
            c.data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE),
                                         static_cast< uint8_t >(cycle * 10 + c.id));
            m_nvme_reader->set_chunk_data(c.id, c.data);

            pdev.write(c.id, 0,
                       std::make_shared< sisl::byte_array_impl >(
                           c.data->bytes(), static_cast< uint32_t >(TEST_CHUNK_SIZE), false));
        }

        auto ctx = cp_callbacks.on_switchover_cp(nullptr, nullptr);
        ASSERT_NE(ctx, nullptr) << "switchover failed at cycle " << cycle;

        auto flush_ok = cp_callbacks.cp_flush(nullptr).get();
        EXPECT_TRUE(flush_ok) << "CP flush failed at cycle " << cycle;
    }

    // Verify final data in S3 matches last mutation
    for (const auto& c : chunks) {
        m_nvme_reader->remove_chunk_data(c.id);
        auto [result, data] = m_chunk_store->get(c.id, 0, TEST_CHUNK_SIZE).get();
        ASSERT_TRUE(result.ok()) << "Failed to read chunk " << c.id << " after " << NUM_CP_CYCLES << " cycles";
        ASSERT_NE(data, nullptr);
        EXPECT_EQ(std::memcmp(c.data->cbytes(), data->cbytes(), TEST_CHUNK_SIZE), 0)
            << "Data mismatch for chunk " << c.id << " after " << NUM_CP_CYCLES << " cycles";
    }

    // Verify superblock is valid
    auto sb_key = PdevS3Superblock::s3_key(m_volume_id);
    auto [sb_result, sb_data] = m_s3_store->get_object(sb_key).get();
    ASSERT_TRUE(sb_result.ok()) << "Superblock not found after " << NUM_CP_CYCLES << " CP cycles";
}

///////////////////////////////////////////////////////////////////////////////
// Stress Test 3: Concurrent Reads and Writes
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioStressTest, ConcurrentReadsAndWrites) {
    init_test("concurrent-rw");

    static constexpr int NUM_CHUNKS = 4;
    static constexpr int NUM_WRITERS = 4;
    static constexpr int NUM_READERS = 4;
    static constexpr int WRITES_PER_WRITER = 5;
    static constexpr int READS_PER_READER = 10;

    S3PhysicalDev pdev(1, m_chunk_store, m_s3_store, m_volume_id, 4096, m_cp_flush_cb);

    std::vector< ChunkInfo > chunks;
    for (int i = 0; i < NUM_CHUNKS; ++i) {
        chunk_id_t cid = static_cast< chunk_id_t >(300 + i);
        auto data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), static_cast< uint8_t >(i));
        chunks.push_back({cid, S3ChunkType::DATA, data});
    }
    create_chunks(pdev, chunks);

    // Initial upload so readers have something to read
    for (const auto& c : chunks) {
        auto result = m_chunk_store->put(c.id, TEST_CHUNK_SIZE, {}).get();
        ASSERT_TRUE(result.ok()) << "Initial upload failed for chunk " << c.id;
    }

    std::atomic< uint64_t > write_ok{0};
    std::atomic< uint64_t > read_ok{0};
    std::atomic< uint64_t > read_fail{0};
    std::atomic< bool > writers_done{false};

    // Writer threads: keep uploading new versions
    std::vector< std::thread > writers;
    writers.reserve(NUM_WRITERS);
    for (int t = 0; t < NUM_WRITERS; ++t) {
        writers.emplace_back([&, t]() {
            std::mt19937 rng(42 + t);
            std::uniform_int_distribution< int > chunk_dist(0, NUM_CHUNKS - 1);

            for (int w = 0; w < WRITES_PER_WRITER; ++w) {
                int idx = chunk_dist(rng);
                auto cid = chunks[idx].id;
                auto new_data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE),
                                                    static_cast< uint8_t >(t * 100 + w));
                m_nvme_reader->set_chunk_data(cid, new_data);
                auto result = m_chunk_store->put(cid, TEST_CHUNK_SIZE, {}).get();
                if (result.ok()) { write_ok.fetch_add(1); }
            }
        });
    }

    // Reader threads: read concurrently with writers
    std::vector< std::thread > readers;
    readers.reserve(NUM_READERS);
    for (int t = 0; t < NUM_READERS; ++t) {
        readers.emplace_back([&, t]() {
            std::mt19937 rng(1000 + t);
            std::uniform_int_distribution< int > chunk_dist(0, NUM_CHUNKS - 1);

            for (int r = 0; r < READS_PER_READER; ++r) {
                int idx = chunk_dist(rng);
                auto cid = chunks[idx].id;
                auto [result, data] = m_s3_store->get_object(m_key_mapper.chunk_data_key(cid)).get();
                if (result.ok() && data.size() == TEST_CHUNK_SIZE) {
                    read_ok.fetch_add(1);
                } else {
                    read_fail.fetch_add(1);
                }
            }
        });
    }

    for (auto& t : writers) { t.join(); }
    writers_done.store(true);
    for (auto& t : readers) { t.join(); }

    EXPECT_EQ(write_ok.load(), static_cast< uint64_t >(NUM_WRITERS * WRITES_PER_WRITER));
    EXPECT_GT(read_ok.load(), 0u);
    EXPECT_EQ(read_fail.load(), 0u);
}

///////////////////////////////////////////////////////////////////////////////
// Stress Test 4: Superblock Integrity Under Repeated Writes
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioStressTest, SuperblockIntegrityUnderRepeatedWrites) {
    init_test("sb-integrity");

    static constexpr int NUM_CHUNKS = 6;
    static constexpr int NUM_ITERATIONS = 20;

    S3PhysicalDev pdev(1, m_chunk_store, m_s3_store, m_volume_id, 4096, m_cp_flush_cb);

    for (int i = 0; i < NUM_CHUNKS; ++i) {
        chunk_id_t cid = static_cast< chunk_id_t >(400 + i);
        auto data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), static_cast< uint8_t >(i));
        pdev.create_chunk(cid, TEST_CHUNK_SIZE, S3ChunkType::DATA, 0);
        m_nvme_reader->set_chunk_data(cid, data);
    }

    for (int iter = 0; iter < NUM_ITERATIONS; ++iter) {
        auto sb_result = pdev.write_superblock();
        ASSERT_TRUE(sb_result.ok()) << "Superblock write failed at iteration " << iter;

        // Read it back and verify deserialization
        auto sb_key = PdevS3Superblock::s3_key(m_volume_id);
        auto [get_result, get_data] = m_s3_store->get_object(sb_key).get();
        ASSERT_TRUE(get_result.ok()) << "Superblock read failed at iteration " << iter;

        PdevS3Superblock deserialized;
        sisl::byte_array sb_buf = sisl::make_byte_array(static_cast< uint32_t >(get_data.size()), 0);
        std::memcpy(sb_buf->bytes(), get_data.cbytes(), get_data.size());
        ASSERT_TRUE(deserialized.deserialize(sb_buf))
            << "Superblock deserialization failed at iteration " << iter;

        EXPECT_EQ(deserialized.num_chunks(), static_cast< uint32_t >(NUM_CHUNKS))
            << "Chunk count mismatch at iteration " << iter;
    }
}

///////////////////////////////////////////////////////////////////////////////
// Stress Test 5: Recovery After Multiple CP Cycles
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioStressTest, RecoveryAfterMultipleCpCycles) {
    init_test("recovery-stress");

    static constexpr int NUM_CHUNKS = 6;
    static constexpr int NUM_CP_CYCLES = 5;

    S3PhysicalDev pdev(1, m_chunk_store, m_s3_store, m_volume_id, 4096, m_cp_flush_cb);

    std::vector< ChunkInfo > chunks;
    for (int i = 0; i < NUM_CHUNKS; ++i) {
        chunk_id_t cid = static_cast< chunk_id_t >(500 + i);
        S3ChunkType type;
        if (i == 0)
            type = S3ChunkType::METABLK;
        else if (i == 1)
            type = S3ChunkType::WAL;
        else if (i == 2)
            type = S3ChunkType::INDEX;
        else
            type = S3ChunkType::DATA;

        auto data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), static_cast< uint8_t >(i));
        chunks.push_back({cid, type, data});
    }
    create_chunks(pdev, chunks);

    std::vector< S3PhysicalDev* > pdevs = {&pdev};
    S3CpCallbacks cp_callbacks(pdevs);

    for (int cycle = 0; cycle < NUM_CP_CYCLES; ++cycle) {
        for (auto& c : chunks) {
            c.data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE),
                                         static_cast< uint8_t >(cycle * 20 + c.id));
            m_nvme_reader->set_chunk_data(c.id, c.data);
            pdev.write(c.id, 0,
                       std::make_shared< sisl::byte_array_impl >(
                           c.data->bytes(), static_cast< uint32_t >(TEST_CHUNK_SIZE), false));
        }

        cp_callbacks.on_switchover_cp(nullptr, nullptr);
        auto flush_ok = cp_callbacks.cp_flush(nullptr).get();
        ASSERT_TRUE(flush_ok) << "CP flush failed at cycle " << cycle;
    }

    // Simulate NVMe loss — wipe all NVMe data
    for (const auto& c : chunks) {
        m_nvme_reader->remove_chunk_data(c.id);
    }

    auto nvme_writer = std::make_shared< MockNvmeRecoveryWriter >();
    nvme_writer->set_nvme_state(NvmeState::EMPTY);

    S3RecoveryManager recovery(m_chunk_store, m_s3_store, nvme_writer, m_volume_id);
    ASSERT_TRUE(recovery.needs_recovery());

    auto result = recovery.recover_from_s3();
    ASSERT_TRUE(result.success) << "Recovery failed after " << NUM_CP_CYCLES << " CP cycles";

    // Verify all chunks recovered with correct final data
    for (const auto& c : chunks) {
        ASSERT_TRUE(nvme_writer->has_chunk(c.id))
            << "Chunk " << c.id << " not recovered";

        auto recovered = nvme_writer->get_chunk(c.id);
        ASSERT_NE(recovered, nullptr);
        EXPECT_EQ(std::memcmp(recovered->cbytes(), c.data->cbytes(), TEST_CHUNK_SIZE), 0)
            << "Data mismatch for recovered chunk " << c.id;
    }
}

///////////////////////////////////////////////////////////////////////////////
// Stress Test 6: Many Chunks Flush at Once
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioStressTest, ManyChunksFlushAtOnce) {
    init_test("many-chunks");

    static constexpr int NUM_CHUNKS = 32;

    S3PhysicalDev pdev(1, m_chunk_store, m_s3_store, m_volume_id, 4096, m_cp_flush_cb);

    std::vector< ChunkInfo > chunks;
    for (int i = 0; i < NUM_CHUNKS; ++i) {
        chunk_id_t cid = static_cast< chunk_id_t >(600 + i);
        S3ChunkType type = (i < 4) ? S3ChunkType::METABLK
                           : (i < 8) ? S3ChunkType::WAL
                           : (i < 12) ? S3ChunkType::INDEX
                                      : S3ChunkType::DATA;

        auto data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), static_cast< uint8_t >(i));
        chunks.push_back({cid, type, data});
    }
    create_chunks(pdev, chunks);

    // Dirty all chunks
    for (const auto& c : chunks) {
        pdev.write(c.id, 0,
                   std::make_shared< sisl::byte_array_impl >(
                       c.data->bytes(), static_cast< uint32_t >(TEST_CHUNK_SIZE), false));
    }

    std::vector< S3PhysicalDev* > pdevs = {&pdev};
    S3CpCallbacks cp_callbacks(pdevs);

    auto start = std::chrono::steady_clock::now();

    cp_callbacks.on_switchover_cp(nullptr, nullptr);
    auto flush_ok = cp_callbacks.cp_flush(nullptr).get();

    auto elapsed_ms = std::chrono::duration_cast< std::chrono::milliseconds >(
                          std::chrono::steady_clock::now() - start)
                          .count();

    EXPECT_TRUE(flush_ok) << "Batch flush of " << NUM_CHUNKS << " chunks failed";

    LOGINFO("Batch flush of {} chunks took {}ms", NUM_CHUNKS, elapsed_ms);

    // Verify all chunks in S3
    for (const auto& c : chunks) {
        auto key = m_key_mapper.chunk_data_key(c.id);
        auto [result, _] = m_s3_store->get_object(key).get();
        EXPECT_TRUE(result.ok()) << "Chunk " << c.id << " not found in S3 after batch flush";
    }
}

///////////////////////////////////////////////////////////////////////////////
// Stress Test 7: Concurrent CP Flush with Concurrent Direct Reads
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioStressTest, CpFlushWithConcurrentReads) {
    init_test("cp-with-reads");

    static constexpr int NUM_CHUNKS = 8;
    static constexpr int NUM_READERS = 4;
    static constexpr int READS_PER_READER = 20;

    S3PhysicalDev pdev(1, m_chunk_store, m_s3_store, m_volume_id, 4096, m_cp_flush_cb);

    std::vector< ChunkInfo > chunks;
    for (int i = 0; i < NUM_CHUNKS; ++i) {
        chunk_id_t cid = static_cast< chunk_id_t >(700 + i);
        auto data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), static_cast< uint8_t >(i));
        chunks.push_back({cid, S3ChunkType::DATA, data});
    }
    create_chunks(pdev, chunks);

    // Do initial upload
    for (const auto& c : chunks) {
        auto result = m_chunk_store->put(c.id, TEST_CHUNK_SIZE, {}).get();
        ASSERT_TRUE(result.ok());
    }

    std::atomic< uint64_t > read_ok{0};
    std::atomic< bool > flush_done{false};

    // Spawn readers
    std::vector< std::thread > readers;
    readers.reserve(NUM_READERS);
    for (int t = 0; t < NUM_READERS; ++t) {
        readers.emplace_back([&, t]() {
            std::mt19937 rng(200 + t);
            std::uniform_int_distribution< int > dist(0, NUM_CHUNKS - 1);

            for (int r = 0; r < READS_PER_READER; ++r) {
                auto cid = chunks[dist(rng)].id;
                auto key = m_key_mapper.chunk_data_key(cid);
                auto [result, data] = m_s3_store->get_object(key).get();
                if (result.ok()) { read_ok.fetch_add(1); }
            }
        });
    }

    // Meanwhile, run CP flush with mutated data
    for (auto& c : chunks) {
        c.data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE),
                                     static_cast< uint8_t >(c.id + 100));
        m_nvme_reader->set_chunk_data(c.id, c.data);
        pdev.write(c.id, 0,
                   std::make_shared< sisl::byte_array_impl >(
                       c.data->bytes(), static_cast< uint32_t >(TEST_CHUNK_SIZE), false));
    }

    std::vector< S3PhysicalDev* > pdevs = {&pdev};
    S3CpCallbacks cp_callbacks(pdevs);
    cp_callbacks.on_switchover_cp(nullptr, nullptr);
    auto flush_ok = cp_callbacks.cp_flush(nullptr).get();
    flush_done.store(true);
    EXPECT_TRUE(flush_ok);

    for (auto& t : readers) { t.join(); }

    EXPECT_EQ(read_ok.load(), static_cast< uint64_t >(NUM_READERS * READS_PER_READER));
}

///////////////////////////////////////////////////////////////////////////////
// Stress Test 8: Full Pipeline Lifecycle (Write → CP → Read → Recover)
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioStressTest, FullPipelineLifecycle) {
    init_test("full-pipeline");

    static constexpr int NUM_CHUNKS = 8;
    static constexpr int NUM_CYCLES = 5;

    S3PhysicalDev pdev(1, m_chunk_store, m_s3_store, m_volume_id, 4096, m_cp_flush_cb);

    std::vector< ChunkInfo > chunks;
    for (int i = 0; i < NUM_CHUNKS; ++i) {
        chunk_id_t cid = static_cast< chunk_id_t >(800 + i);
        S3ChunkType type = (i == 0) ? S3ChunkType::METABLK
                           : (i == 1) ? S3ChunkType::WAL
                           : (i == 2) ? S3ChunkType::INDEX
                                      : S3ChunkType::DATA;
        auto data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), static_cast< uint8_t >(i));
        chunks.push_back({cid, type, data});
    }
    create_chunks(pdev, chunks);

    std::vector< S3PhysicalDev* > pdevs = {&pdev};
    S3CpCallbacks cp_callbacks(pdevs);

    for (int cycle = 0; cycle < NUM_CYCLES; ++cycle) {
        // Phase 1: Write new data
        for (auto& c : chunks) {
            c.data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE),
                                         static_cast< uint8_t >(cycle * 30 + c.id));
            m_nvme_reader->set_chunk_data(c.id, c.data);
            pdev.write(c.id, 0,
                       std::make_shared< sisl::byte_array_impl >(
                           c.data->bytes(), static_cast< uint32_t >(TEST_CHUNK_SIZE), false));
        }

        // Phase 2: CP flush
        cp_callbacks.on_switchover_cp(nullptr, nullptr);
        auto flush_ok = cp_callbacks.cp_flush(nullptr).get();
        ASSERT_TRUE(flush_ok) << "CP flush failed at cycle " << cycle;

        // Phase 3: Verify reads match
        for (const auto& c : chunks) {
            m_nvme_reader->remove_chunk_data(c.id);
            auto [result, data] = m_chunk_store->get(c.id, 0, TEST_CHUNK_SIZE).get();
            ASSERT_TRUE(result.ok()) << "Read failed for chunk " << c.id << " cycle " << cycle;
            ASSERT_NE(data, nullptr);
            EXPECT_EQ(std::memcmp(c.data->cbytes(), data->cbytes(), TEST_CHUNK_SIZE), 0)
                << "Data mismatch for chunk " << c.id << " cycle " << cycle;
            m_nvme_reader->set_chunk_data(c.id, c.data);
        }

        // Phase 4: Recovery (simulate NVMe loss every other cycle)
        if (cycle % 2 == 1) {
            for (const auto& c : chunks) {
                m_nvme_reader->remove_chunk_data(c.id);
            }

            auto nvme_writer = std::make_shared< MockNvmeRecoveryWriter >();
            nvme_writer->set_nvme_state(NvmeState::EMPTY);

            S3RecoveryManager recovery(m_chunk_store, m_s3_store, nvme_writer, m_volume_id);
            auto rec_result = recovery.recover_from_s3();
            ASSERT_TRUE(rec_result.success) << "Recovery failed at cycle " << cycle;

            for (const auto& c : chunks) {
                ASSERT_TRUE(nvme_writer->has_chunk(c.id))
                    << "Chunk " << c.id << " not recovered at cycle " << cycle;
                auto recovered = nvme_writer->get_chunk(c.id);
                EXPECT_EQ(std::memcmp(recovered->cbytes(), c.data->cbytes(), TEST_CHUNK_SIZE), 0)
                    << "Recovered data mismatch for chunk " << c.id << " cycle " << cycle;
                m_nvme_reader->set_chunk_data(c.id, recovered);
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// Stress Test 9: Concurrent Uploads to Multiple Pdevs
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioStressTest, MultiplePdevsConcurrentFlush) {
    static constexpr int NUM_PDEVS = 3;
    static constexpr int CHUNKS_PER_PDEV = 4;

    std::vector< std::string > vol_ids;
    std::vector< std::shared_ptr< FullChunkStore > > chunk_stores;
    std::vector< std::unique_ptr< S3PhysicalDev > > pdevs;
    std::vector< std::vector< ChunkInfo > > all_chunks;

    for (int p = 0; p < NUM_PDEVS; ++p) {
        auto vol = make_unique_volume_id("multi-pdev-" + std::to_string(p));
        vol_ids.push_back(vol);

        auto mapper = S3KeyMapper{vol};
        auto nvme = std::make_shared< MockNvmeChunkReader >();
        auto cs = std::make_shared< FullChunkStore >(m_s3_store, nvme, mapper);
        chunk_stores.push_back(cs);

        auto cb = std::make_shared< MockCpFlushCallback >();
        auto pd = std::make_unique< S3PhysicalDev >(
            static_cast< uint32_t >(p + 1), cs, m_s3_store, vol, 4096, cb);

        std::vector< ChunkInfo > chunks;
        for (int i = 0; i < CHUNKS_PER_PDEV; ++i) {
            chunk_id_t cid = static_cast< chunk_id_t >(p * 100 + i);
            auto data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE),
                                            static_cast< uint8_t >(p * 10 + i));
            pd->create_chunk(cid, TEST_CHUNK_SIZE, S3ChunkType::DATA, 0);
            nvme->set_chunk_data(cid, data);
            pd->write(cid, 0,
                      std::make_shared< sisl::byte_array_impl >(
                          data->bytes(), static_cast< uint32_t >(TEST_CHUNK_SIZE), false));
            chunks.push_back({cid, S3ChunkType::DATA, data});
        }

        all_chunks.push_back(std::move(chunks));
        pdevs.push_back(std::move(pd));
    }

    // Set m_volume_id to empty to prevent TearDown from cleaning up (we clean up manually)
    m_volume_id = "";

    std::vector< S3PhysicalDev* > pdev_ptrs;
    for (auto& pd : pdevs) { pdev_ptrs.push_back(pd.get()); }

    S3CpCallbacks cp_callbacks(pdev_ptrs);
    cp_callbacks.on_switchover_cp(nullptr, nullptr);
    auto flush_ok = cp_callbacks.cp_flush(nullptr).get();
    EXPECT_TRUE(flush_ok);

    // Verify each pdev's superblock exists
    for (int p = 0; p < NUM_PDEVS; ++p) {
        auto sb_key = PdevS3Superblock::s3_key(vol_ids[p]);
        auto [result, _] = m_s3_store->get_object(sb_key).get();
        EXPECT_TRUE(result.ok()) << "Superblock not found for pdev " << p;
    }

    // Cleanup all volumes
    for (const auto& vol : vol_ids) {
        cleanup_volume(vol);
    }
}

///////////////////////////////////////////////////////////////////////////////
// main
///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_minio_stress");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
