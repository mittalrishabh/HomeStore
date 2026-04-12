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
#include <set>
#include <string>

#include <gtest/gtest.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/chunk_store.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/s3/full_chunk_store.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/tiered_read_handler.h>
#include "lib/s3/s3_object_store_impl.h"

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Mock NVMe Reader (for FullChunkStore)
///////////////////////////////////////////////////////////////////////////////
class MockNvmeChunkReader : public NvmeChunkReader {
public:
    void set_chunk_data(chunk_id_t chunk_id, sisl::byte_array data) {
        m_chunks[chunk_id] = std::move(data);
    }

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

///////////////////////////////////////////////////////////////////////////////
// Mock NVMe Device I/O (for TieredReadHandler)
///////////////////////////////////////////////////////////////////////////////
class MockNvmeDeviceIO : public NvmeDeviceIO {
public:
    /// Simulate local NVMe storage
    void store(chunk_id_t chunk_id, uint64_t offset, const char* data, uint64_t size) {
        auto key = make_key(chunk_id, offset);
        auto buf = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(buf->bytes(), data, size);
        m_storage[key] = std::move(buf);
        m_on_nvme.insert(chunk_id);
    }

    void set_on_nvme(chunk_id_t chunk_id, bool on) {
        if (on) m_on_nvme.insert(chunk_id);
        else m_on_nvme.erase(chunk_id);
    }

    bool was_written(chunk_id_t chunk_id) const {
        return m_writes.count(chunk_id) > 0;
    }

    std::error_code nvme_read(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                              char* buf, uint64_t size) override {
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
        auto key = make_key(chunk_id, offset_in_chunk);
        auto data = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        std::memcpy(data->bytes(), buf, size);
        m_storage[key] = std::move(data);
        m_on_nvme.insert(chunk_id);
        m_writes.insert(chunk_id);
        return {};
    }

    bool is_chunk_on_nvme(chunk_id_t chunk_id) const override {
        return m_on_nvme.count(chunk_id) > 0;
    }

private:
    static std::string make_key(chunk_id_t cid, uint64_t offset) {
        return std::to_string(cid) + ":" + std::to_string(offset);
    }

    std::unordered_map< std::string, sisl::byte_array > m_storage;
    std::set< chunk_id_t > m_on_nvme;
    std::set< chunk_id_t > m_writes;
};

///////////////////////////////////////////////////////////////////////////////
// Helpers
///////////////////////////////////////////////////////////////////////////////
static sisl::byte_array make_test_data(uint32_t size, uint8_t pattern = 0xAB) {
    auto buf = sisl::make_byte_array(size, 0);
    std::memset(buf->bytes(), pattern, size);
    return buf;
}

static S3ObjectStoreConfig make_test_config() {
    S3ObjectStoreConfig cfg;
    cfg.bucket = "homestore-test";
    cfg.region = "us-east-1";
    cfg.retry_count = 1;
    cfg.retry_backoff_ms = 0;
    return cfg;
}

///////////////////////////////////////////////////////////////////////////////
// Tests
///////////////////////////////////////////////////////////////////////////////
class TieredReadTest : public ::testing::Test {
protected:
    void SetUp() override {
        m_s3_store = std::make_shared< MockS3ObjectStore >(make_test_config());
        m_nvme_chunk_reader = std::make_shared< MockNvmeChunkReader >();

        S3KeyMapper key_mapper{.volume_id = "vol-001"};
        m_chunk_store = std::make_shared< FullChunkStore >(m_s3_store, m_nvme_chunk_reader, key_mapper);

        m_s3_pdev = std::make_unique< S3PhysicalDev >(
            1, m_chunk_store, m_s3_store, "vol-001", 1024, nullptr);

        m_nvme_io = std::make_shared< MockNvmeDeviceIO >();

        m_handler = std::make_unique< TieredReadHandler >(
            m_s3_pdev.get(), m_nvme_io, TieredReadConfig{.s3_read_fallback_enabled = true,
                                                          .s3_hydrate_on_read = true});
    }

    /// Helper: set up a chunk on S3 (put data via ChunkStore)
    void setup_chunk_on_s3(chunk_id_t chunk_id, uint64_t chunk_size, uint8_t pattern) {
        m_s3_pdev->create_chunk(chunk_id, chunk_size, S3ChunkType::DATA, 1);
        auto data = make_test_data(chunk_size, pattern);
        m_nvme_chunk_reader->set_chunk_data(chunk_id, data);
        m_chunk_store->put(chunk_id, {}, chunk_size);
    }

    std::shared_ptr< MockS3ObjectStore > m_s3_store;
    std::shared_ptr< MockNvmeChunkReader > m_nvme_chunk_reader;
    std::shared_ptr< FullChunkStore > m_chunk_store;
    std::unique_ptr< S3PhysicalDev > m_s3_pdev;
    std::shared_ptr< MockNvmeDeviceIO > m_nvme_io;
    std::unique_ptr< TieredReadHandler > m_handler;
};

TEST_F(TieredReadTest, NvmeFastPath) {
    constexpr chunk_id_t CHUNK_ID = 10;
    constexpr uint64_t SIZE = 256;

    // Put data on NVMe
    auto data = make_test_data(SIZE, 0xAA);
    m_nvme_io->store(CHUNK_ID, 0, reinterpret_cast< const char* >(data->cbytes()), SIZE);

    auto [ec, result] = m_handler->async_read(CHUNK_ID, 0, SIZE).get();
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->size(), SIZE);
    ASSERT_EQ(result->cbytes()[0], 0xAA);
}

TEST_F(TieredReadTest, S3FallbackWhenNotOnNvme) {
    constexpr chunk_id_t CHUNK_ID = 20;
    constexpr uint64_t CHUNK_SIZE = 4096;

    // Chunk on S3 only (not on NVMe)
    setup_chunk_on_s3(CHUNK_ID, CHUNK_SIZE, 0xBB);

    auto [ec, result] = m_handler->async_read(CHUNK_ID, 0, 64).get();
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->size(), 64u);
    ASSERT_EQ(result->cbytes()[0], 0xBB);
}

TEST_F(TieredReadTest, HydrationWritesToNvme) {
    constexpr chunk_id_t CHUNK_ID = 30;
    constexpr uint64_t CHUNK_SIZE = 4096;

    setup_chunk_on_s3(CHUNK_ID, CHUNK_SIZE, 0xCC);

    // Read from S3 — should hydrate to NVMe
    auto [ec, result] = m_handler->async_read(CHUNK_ID, 0, 128).get();
    ASSERT_FALSE(ec);

    // Verify NVMe was written to (hydrated)
    ASSERT_TRUE(m_nvme_io->was_written(CHUNK_ID));
}

TEST_F(TieredReadTest, NoHydrationWhenDisabled) {
    constexpr chunk_id_t CHUNK_ID = 40;
    constexpr uint64_t CHUNK_SIZE = 4096;

    m_handler->set_config({.s3_read_fallback_enabled = true, .s3_hydrate_on_read = false});

    setup_chunk_on_s3(CHUNK_ID, CHUNK_SIZE, 0xDD);

    auto [ec, result] = m_handler->async_read(CHUNK_ID, 0, 64).get();
    ASSERT_FALSE(ec);
    ASSERT_NE(result, nullptr);

    // NVMe should NOT have been written to
    ASSERT_FALSE(m_nvme_io->was_written(CHUNK_ID));
}

TEST_F(TieredReadTest, FallbackDisabledReturnsError) {
    constexpr chunk_id_t CHUNK_ID = 50;

    m_handler->set_config({.s3_read_fallback_enabled = false, .s3_hydrate_on_read = false});

    // Chunk not on NVMe, fallback disabled
    auto [ec, result] = m_handler->async_read(CHUNK_ID, 0, 64).get();
    ASSERT_TRUE(ec);
}

TEST_F(TieredReadTest, SyncReadNvmeFastPath) {
    constexpr chunk_id_t CHUNK_ID = 60;
    constexpr uint64_t SIZE = 128;

    auto data = make_test_data(SIZE, 0xEE);
    m_nvme_io->store(CHUNK_ID, 0, reinterpret_cast< const char* >(data->cbytes()), SIZE);

    char buf[128];
    auto ec = m_handler->sync_read(CHUNK_ID, 0, buf, SIZE);
    ASSERT_FALSE(ec);
    ASSERT_EQ(static_cast< uint8_t >(buf[0]), 0xEE);
}

TEST_F(TieredReadTest, SyncReadS3Fallback) {
    constexpr chunk_id_t CHUNK_ID = 70;
    constexpr uint64_t CHUNK_SIZE = 4096;

    setup_chunk_on_s3(CHUNK_ID, CHUNK_SIZE, 0xFF);

    char buf[64];
    auto ec = m_handler->sync_read(CHUNK_ID, 0, buf, 64);
    ASSERT_FALSE(ec);
    ASSERT_EQ(static_cast< uint8_t >(buf[0]), 0xFF);
}

TEST_F(TieredReadTest, ForceHydrate) {
    constexpr chunk_id_t CHUNK_ID = 80;
    constexpr uint64_t CHUNK_SIZE = 4096;

    setup_chunk_on_s3(CHUNK_ID, CHUNK_SIZE, 0x11);

    auto ec = m_handler->hydrate(CHUNK_ID, 0, 256);
    ASSERT_FALSE(ec);

    ASSERT_TRUE(m_nvme_io->was_written(CHUNK_ID));
    ASSERT_TRUE(m_nvme_io->is_chunk_on_nvme(CHUNK_ID));
}

TEST_F(TieredReadTest, S3ReadAfterHydrationServesFromNvme) {
    constexpr chunk_id_t CHUNK_ID = 90;
    constexpr uint64_t CHUNK_SIZE = 4096;

    setup_chunk_on_s3(CHUNK_ID, CHUNK_SIZE, 0x22);

    // First read: S3 fallback + hydrate
    auto [ec1, data1] = m_handler->async_read(CHUNK_ID, 0, 64).get();
    ASSERT_FALSE(ec1);
    ASSERT_EQ(data1->cbytes()[0], 0x22);

    // Second read: should hit NVMe (hydrated)
    ASSERT_TRUE(m_nvme_io->is_chunk_on_nvme(CHUNK_ID));
    auto [ec2, data2] = m_handler->async_read(CHUNK_ID, 0, 64).get();
    ASSERT_FALSE(ec2);
    ASSERT_EQ(data2->cbytes()[0], 0x22);
}

TEST_F(TieredReadTest, ConfigChangeAtRuntime) {
    auto cfg = m_handler->config();
    ASSERT_TRUE(cfg.s3_read_fallback_enabled);
    ASSERT_TRUE(cfg.s3_hydrate_on_read);

    m_handler->set_config({.s3_read_fallback_enabled = false, .s3_hydrate_on_read = false});
    cfg = m_handler->config();
    ASSERT_FALSE(cfg.s3_read_fallback_enabled);
    ASSERT_FALSE(cfg.s3_hydrate_on_read);
}

TEST_F(TieredReadTest, NvmeReadFailureFallsToS3) {
    constexpr chunk_id_t CHUNK_ID = 100;
    constexpr uint64_t CHUNK_SIZE = 4096;

    // Mark chunk as on NVMe but don't store actual data (read will fail)
    m_nvme_io->set_on_nvme(CHUNK_ID, true);

    // Also put data on S3
    setup_chunk_on_s3(CHUNK_ID, CHUNK_SIZE, 0x33);

    // NVMe read will fail (no data stored), should fallback to S3
    auto [ec, result] = m_handler->async_read(CHUNK_ID, 0, 64).get();
    ASSERT_FALSE(ec);
    ASSERT_EQ(result->cbytes()[0], 0x33);
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_tiered_read");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
