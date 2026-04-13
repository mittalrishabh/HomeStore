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
 * @file test_s3_minio_smoke.cpp
 * @brief End-to-end smoke tests against a real MinIO instance.
 *
 * Requires: MinIO running on localhost:9000
 *           AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin
 * Gated behind: -DENABLE_MINIO_TESTS=ON
 *
 * Wires up REAL: AwsS3ObjectStore, FullChunkStore, S3PhysicalDev,
 *                S3CpCallbacks, S3RecoveryManager, TieredReadHandler
 * Mocked:        NVMe layer (MockNvmeChunkReader, MockNvmeDeviceIO, etc.)
 */

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
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
static sisl::byte_array make_test_data(uint32_t size, uint8_t pattern = 0xAB) {
    auto buf = sisl::make_byte_array(size, 0);
    std::memset(buf->bytes(), pattern, size);
    return buf;
}

static sisl::byte_array make_patterned_data(uint32_t size, uint8_t seed = 0) {
    auto buf = sisl::make_byte_array(size, 0);
    for (uint32_t i = 0; i < size; ++i) {
        buf->bytes()[i] = static_cast< uint8_t >((seed + i) & 0xFF);
    }
    return buf;
}

static void wait_for_condition(std::function< bool() > cond, int max_ms = 5000) {
    for (int elapsed = 0; elapsed < max_ms; elapsed += 10) {
        if (cond()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
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
    return "smoke-" + test_name + "-" + std::to_string(ms) + "-" + std::to_string(counter.fetch_add(1));
}

///////////////////////////////////////////////////////////////////////////////
// Mock NVMe layer — same pattern as existing integration tests
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

    std::error_code nvme_read(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                              char* buf, uint64_t size) override {
        std::lock_guard lock{m_mtx};
        if (m_on_nvme.count(chunk_id) == 0) {
            return std::make_error_code(std::errc::no_such_file_or_directory);
        }
        auto key = make_key(chunk_id, offset_in_chunk);
        auto it = m_storage.find(key);
        if (it == m_storage.end()) {
            std::memset(buf, 0, size);
            return {};
        }
        auto sz = std::min(static_cast< uint64_t >(it->second->size()), size);
        std::memcpy(buf, it->second->cbytes(), sz);
        return {};
    }

    std::error_code nvme_write(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                               const char* data, uint64_t size) override {
        store(chunk_id, offset_in_chunk, data, size);
        return {};
    }

    bool is_chunk_on_nvme(chunk_id_t chunk_id) const override {
        std::lock_guard lock{m_mtx};
        return m_on_nvme.count(chunk_id) > 0;
    }

private:
    static std::string make_key(chunk_id_t cid, uint64_t off) {
        return std::to_string(cid) + ":" + std::to_string(off);
    }

    mutable std::mutex m_mtx;
    std::unordered_map< std::string, sisl::byte_array > m_storage;
    std::set< chunk_id_t > m_on_nvme;
};

class MockNvmeChunkAllocator : public NvmeChunkAllocator {
public:
    std::error_code allocate_nvme_chunk(chunk_id_t chunk_id, uint64_t /*chunk_size*/) override {
        std::lock_guard lock{m_mtx};
        m_allocated.insert(chunk_id);
        return {};
    }

    void free_nvme_chunk(chunk_id_t chunk_id) override {
        std::lock_guard lock{m_mtx};
        m_allocated.erase(chunk_id);
    }

    bool is_allocated(chunk_id_t chunk_id) const {
        std::lock_guard lock{m_mtx};
        return m_allocated.count(chunk_id) > 0;
    }

private:
    mutable std::mutex m_mtx;
    std::set< chunk_id_t > m_allocated;
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
// Factory: create the real AwsS3ObjectStore for MinIO
///////////////////////////////////////////////////////////////////////////////
static std::shared_ptr< S3ObjectStore > create_minio_store(const std::string& bucket) {
    auto cfg = make_minio_config(bucket);
    return std::make_shared< AwsS3ObjectStore >(cfg);
}

///////////////////////////////////////////////////////////////////////////////
// Test Fixture
///////////////////////////////////////////////////////////////////////////////
class S3MinioSmokeTest : public ::testing::Test {
protected:
    static constexpr const char* SHARED_BUCKET = "homestore-test";

    void SetUp() override {
        m_s3_store = create_minio_store(SHARED_BUCKET);
    }

    void TearDown() override {
        if (m_s3_store && !m_volume_id.empty()) {
            auto list_result = m_s3_store->list_objects(m_volume_id + "/").get();
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
    }

    void init_test(const std::string& test_name) {
        m_volume_id = make_unique_volume_id(test_name);
        m_key_mapper = S3KeyMapper{m_volume_id};
    }

    std::string m_volume_id;
    S3KeyMapper m_key_mapper;
    std::shared_ptr< S3ObjectStore > m_s3_store;
};

///////////////////////////////////////////////////////////////////////////////
// Test 1: FullChunkStore Round-Trip via real S3
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioSmokeTest, FullChunkStoreRoundTrip) {
    init_test("roundtrip");

    auto nvme_reader = std::make_shared< MockNvmeChunkReader >();
    auto chunk_store = std::make_shared< FullChunkStore >(m_s3_store, nvme_reader, m_key_mapper);

    const chunk_id_t chunk_id = 42;
    auto test_data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), 0xDE);

    // Simulate NVMe having the chunk data (FullChunkStore reads from NVMe for upload)
    nvme_reader->set_chunk_data(chunk_id, test_data);

    // PUT: upload chunk to MinIO
    auto put_result = chunk_store->put(chunk_id, TEST_CHUNK_SIZE, {}).get();
    ASSERT_TRUE(put_result.ok()) << "PUT failed: " << put_result.error_message;

    // Remove from NVMe to force S3 read path
    nvme_reader->remove_chunk_data(chunk_id);

    // GET: read back from MinIO
    auto [get_result, read_data] = chunk_store->get(chunk_id, 0, TEST_CHUNK_SIZE).get();
    ASSERT_TRUE(get_result.ok()) << "GET failed: " << get_result.error_message;
    ASSERT_NE(read_data, nullptr);
    ASSERT_EQ(read_data->size(), TEST_CHUNK_SIZE);

    // Verify byte-for-byte match
    EXPECT_EQ(std::memcmp(test_data->cbytes(), read_data->cbytes(), TEST_CHUNK_SIZE), 0);
}

///////////////////////////////////////////////////////////////////////////////
// Test 2: CP Flush to MinIO
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioSmokeTest, CpFlushToMinIO) {
    init_test("cpflush");

    auto nvme_reader = std::make_shared< MockNvmeChunkReader >();
    auto chunk_store = std::make_shared< FullChunkStore >(m_s3_store, nvme_reader, m_key_mapper);
    auto cp_flush_cb = std::make_shared< MockCpFlushCallback >();

    const uint32_t pdev_id = 1;
    S3PhysicalDev pdev(pdev_id, chunk_store, m_s3_store, m_volume_id, 4096, cp_flush_cb);

    const chunk_id_t chunk_id = 100;
    pdev.create_chunk(chunk_id, TEST_CHUNK_SIZE, S3ChunkType::DATA, /*vdev_id=*/0);

    // Write data into dirty cache
    auto test_data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), 0xBE);
    nvme_reader->set_chunk_data(chunk_id, test_data);
    pdev.write(chunk_id, 0, std::make_shared< sisl::byte_array_impl >(
        test_data->bytes(), static_cast< uint32_t >(TEST_CHUNK_SIZE), false));

    // Wire up S3CpCallbacks and flush
    std::vector< S3PhysicalDev* > pdevs = {&pdev};
    S3CpCallbacks cp_callbacks(pdevs);

    // Simulate a CP switchover + flush cycle
    // switchover drains dirty cache, flush uploads to S3
    auto ctx = cp_callbacks.on_switchover_cp(nullptr, nullptr);
    ASSERT_NE(ctx, nullptr);

    auto flush_ok = cp_callbacks.cp_flush(nullptr).get();
    EXPECT_TRUE(flush_ok);

    // Verify superblock exists in MinIO (key format: <volume_id>/pdev_superblock.bin)
    auto sb_key = PdevS3Superblock::s3_key(m_volume_id);
    auto [head_result, _] = m_s3_store->get_object(sb_key).get();
    EXPECT_TRUE(head_result.ok()) << "Superblock not found in MinIO at key: " << sb_key;

    // Verify chunk data exists in MinIO
    auto chunk_key = m_key_mapper.chunk_data_key(chunk_id);
    auto [chunk_head, chunk_data] = m_s3_store->get_object(chunk_key).get();
    EXPECT_TRUE(chunk_head.ok()) << "Chunk not found in MinIO: " << chunk_key;
}

///////////////////////////////////////////////////////////////////////////////
// Test 3: Recovery from MinIO
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioSmokeTest, RecoveryFromMinIO) {
    init_test("recovery");

    auto nvme_reader = std::make_shared< MockNvmeChunkReader >();
    auto chunk_store = std::make_shared< FullChunkStore >(m_s3_store, nvme_reader, m_key_mapper);
    auto cp_flush_cb = std::make_shared< MockCpFlushCallback >();

    const uint32_t pdev_id = 1;
    S3PhysicalDev pdev(pdev_id, chunk_store, m_s3_store, m_volume_id, 4096, cp_flush_cb);

    // Create chunks and upload via normal CP path (builds proper superblock)
    const std::vector< chunk_id_t > chunk_ids = {10, 20, 30};
    std::unordered_map< chunk_id_t, sisl::byte_array > expected_data;

    for (auto cid : chunk_ids) {
        auto data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE),
                                        static_cast< uint8_t >(cid));
        expected_data[cid] = data;

        pdev.create_chunk(cid, TEST_CHUNK_SIZE, S3ChunkType::DATA, /*vdev_id=*/0);
        nvme_reader->set_chunk_data(cid, data);

        auto put_r = chunk_store->put(cid, TEST_CHUNK_SIZE, {}).get();
        ASSERT_TRUE(put_r.ok()) << "Failed to upload chunk " << cid << ": " << put_r.error_message;
    }

    // Write superblock to S3 (serializes chunk table → pdev_superblock.bin)
    auto sb_result = pdev.write_superblock();
    ASSERT_TRUE(sb_result.ok()) << "Superblock write failed: " << sb_result.error_message;

    // Simulate NVMe loss
    for (auto cid : chunk_ids) {
        nvme_reader->remove_chunk_data(cid);
    }

    auto nvme_writer = std::make_shared< MockNvmeRecoveryWriter >();
    nvme_writer->set_nvme_state(NvmeState::EMPTY);

    S3RecoveryManager recovery(chunk_store, m_s3_store, nvme_writer, m_volume_id);

    EXPECT_TRUE(recovery.needs_recovery());

    auto result = recovery.recover_from_s3();
    EXPECT_TRUE(result.success) << "Recovery failed";

    for (auto cid : chunk_ids) {
        EXPECT_TRUE(nvme_writer->has_chunk(cid))
            << "Chunk " << cid << " not recovered to NVMe";

        auto recovered = nvme_writer->get_chunk(cid);
        if (recovered) {
            EXPECT_EQ(std::memcmp(recovered->cbytes(), expected_data[cid]->cbytes(),
                                  TEST_CHUNK_SIZE), 0)
                << "Chunk " << cid << " data mismatch after recovery";
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// Test 4: Tiered Read Fallback to S3
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioSmokeTest, TieredReadFallback) {
    init_test("tiered");

    auto nvme_reader = std::make_shared< MockNvmeChunkReader >();
    auto nvme_io = std::make_shared< MockNvmeDeviceIO >();
    auto nvme_mgr = std::make_shared< MockNvmeChunkManager >();
    auto nvme_alloc = std::make_shared< MockNvmeChunkAllocator >();
    auto chunk_store = std::make_shared< FullChunkStore >(m_s3_store, nvme_reader, m_key_mapper);
    auto cp_flush_cb = std::make_shared< MockCpFlushCallback >();

    const uint32_t pdev_id = 1;
    S3PhysicalDev pdev(pdev_id, chunk_store, m_s3_store, m_volume_id, 4096, cp_flush_cb);

    const chunk_id_t chunk_id = 55;
    pdev.create_chunk(chunk_id, TEST_CHUNK_SIZE, S3ChunkType::DATA, /*vdev_id=*/0);

    // Upload chunk data to MinIO
    auto test_data = make_patterned_data(static_cast< uint32_t >(TEST_CHUNK_SIZE), 0xCC);
    nvme_reader->set_chunk_data(chunk_id, test_data);
    auto put_r = chunk_store->put(chunk_id, TEST_CHUNK_SIZE, {}).get();
    ASSERT_TRUE(put_r.ok());

    // Evict from NVMe — chunk only exists in MinIO now
    nvme_reader->remove_chunk_data(chunk_id);
    nvme_io->set_on_nvme(chunk_id, false);
    nvme_mgr->set_on_nvme(chunk_id, false);

    // Wire up eviction + hydration for TieredReadHandler
    auto eviction_mgr = std::make_shared< ChunkEvictionManager >(&pdev, nvme_mgr);
    auto hydration_mgr = std::make_shared< ChunkHydrationManager >(
        chunk_store, nvme_io, nvme_alloc, eviction_mgr);

    TieredReadConfig read_cfg{.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false};
    TieredReadHandler tiered(&pdev, nvme_io, read_cfg, hydration_mgr);

    // Read via tiered handler — should fall back to S3 over real HTTP
    auto [ec, read_data] = tiered.async_read(chunk_id, 0, TEST_CHUNK_SIZE).get();
    ASSERT_FALSE(ec) << "Tiered read failed: " << ec.message();
    ASSERT_NE(read_data, nullptr);
    ASSERT_EQ(read_data->size(), TEST_CHUNK_SIZE);

    // Verify byte-for-byte match
    EXPECT_EQ(std::memcmp(test_data->cbytes(), read_data->cbytes(), TEST_CHUNK_SIZE), 0);
}

///////////////////////////////////////////////////////////////////////////////
// Test 5: Error Handling
///////////////////////////////////////////////////////////////////////////////
TEST_F(S3MinioSmokeTest, ErrorHandling) {
    // Sub-test A: Wrong bucket name → clean error
    {
        auto bad_store = create_minio_store("nonexistent-bucket-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));

        auto [result, data] = bad_store->get_object("some/key").get();
        EXPECT_FALSE(result.ok()) << "Expected error for nonexistent bucket";
        EXPECT_FALSE(result.error_message.empty()) << "Expected non-empty error message";
    }

    // Sub-test B: Missing key in valid bucket → 404
    {
        auto [result, data] = m_s3_store->get_object("this/key/does/not/exist").get();
        EXPECT_FALSE(result.ok()) << "Expected error for nonexistent key";
        // 404 or NoSuchKey — exact code depends on AwsS3ObjectStore mapping
        EXPECT_TRUE(result.status_code == 404 || !result.error_message.empty())
            << "Expected 404 or error message for missing key, got status=" << result.status_code;
    }
}

///////////////////////////////////////////////////////////////////////////////
// main
///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_minio_smoke");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
