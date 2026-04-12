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
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/nvme_superblock_fallback.h>
#include <homestore/s3/s3_object_store.h>

SISL_LOGGING_INIT(test_nvme_sb_fallback, s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

// ─── In-memory NVMe device mock ─────────────────────────────────────────────

class MockNvmeDeviceIO : public NvmeDeviceIO {
public:
    explicit MockNvmeDeviceIO(uint64_t device_size = 1 * 1024 * 1024)
        : m_storage(device_size, 0) {}

    void sync_write(const uint8_t* buf, uint32_t size, uint64_t offset) override {
        ASSERT_LE(offset + size, m_storage.size()) << "Write out of bounds";
        std::memcpy(m_storage.data() + offset, buf, size);
    }

    std::error_code sync_read(uint8_t* buf, uint32_t size, uint64_t offset) override {
        if (offset + size > m_storage.size()) {
            return std::make_error_code(std::errc::io_error);
        }
        std::memcpy(buf, m_storage.data() + offset, size);
        return {};
    }

    /// Corrupt bytes at a given range (for testing).
    void corrupt(uint64_t offset, uint32_t size) {
        for (uint32_t i = 0; i < size && (offset + i) < m_storage.size(); ++i) {
            m_storage[offset + i] ^= 0xFF;
        }
    }

    /// Zero out a range.
    void zero_out(uint64_t offset, uint32_t size) {
        std::memset(m_storage.data() + offset, 0, std::min(static_cast<uint64_t>(size), m_storage.size() - offset));
    }

    /// Raw access for inspection.
    const std::vector<uint8_t>& storage() const { return m_storage; }

private:
    std::vector<uint8_t> m_storage;
};

// ─── In-memory S3 mock (minimal — just get_object for fallback) ──────────────

class MockS3ForFallback : public S3ObjectStore {
public:
    void put(const std::string& key, const uint8_t* data, uint64_t size) {
        m_objects[key] = std::vector<uint8_t>(data, data + size);
    }

    void put_blob(const std::string& key, const sisl::byte_array& blob) {
        m_objects[key] = std::vector<uint8_t>(blob->cbytes(), blob->cbytes() + blob->size());
    }

    // S3ObjectStore interface
    folly::Future<S3Result> put_object(const std::string& key, sisl::io_blob_safe data) override {
        m_objects[key] = std::vector<uint8_t>(data.cbytes(), data.cbytes() + data.size());
        S3Result r;
        r.status_code = 200;
        return folly::makeFuture(r);
    }

    folly::Future<std::pair<S3Result, sisl::io_blob_safe>> get_object(const std::string& key) override {
        auto it = m_objects.find(key);
        if (it == m_objects.end()) {
            S3Result r;
            r.status_code = 404;
            r.error_message = "Not found";
            return folly::makeFuture(std::make_pair(r, sisl::io_blob_safe{}));
        }

        sisl::io_blob_safe blob(it->second.size(), 0);
        std::memcpy(blob.bytes(), it->second.data(), it->second.size());

        S3Result r;
        r.status_code = 200;
        r.content_length = it->second.size();
        return folly::makeFuture(std::make_pair(r, std::move(blob)));
    }

    folly::Future<std::pair<S3Result, sisl::io_blob_safe>>
    get_object_range(const std::string&, uint64_t, uint64_t) override {
        return folly::makeFuture(std::make_pair(S3Result{.status_code = 501}, sisl::io_blob_safe{}));
    }

    folly::Future<S3Result> delete_object(const std::string&) override {
        return folly::makeFuture(S3Result{.status_code = 200});
    }

    folly::Future<S3Result> delete_objects(const std::vector<std::string>&) override {
        return folly::makeFuture(S3Result{.status_code = 200});
    }

    folly::Future<S3Result> head_object(const std::string&) override {
        return folly::makeFuture(S3Result{.status_code = 200});
    }

    folly::Future<S3ListResult> list_objects(const std::string&, const std::string&, uint32_t) override {
        return folly::makeFuture(S3ListResult{});
    }

    const std::string& bucket_name() const override {
        static std::string name{"test-bucket"};
        return name;
    }

    folly::Future<S3Result> copy_object(const std::string&, const std::string&) override {
        return folly::makeFuture(S3Result{.status_code = 200});
    }

private:
    std::map<std::string, std::vector<uint8_t>> m_objects;
};

// ─── Test helpers ────────────────────────────────────────────────────────────

static constexpr uint64_t SLOT_A_OFFSET = 0;
static constexpr uint64_t SLOT_B_OFFSET = 8192;
static constexpr uint32_t SLOT_SIZE = 4096;

static sisl::byte_array make_payload(const std::string& content) {
    auto buf = sisl::make_byte_array(content.size());
    std::memcpy(buf->bytes(), content.data(), content.size());
    return buf;
}

static std::string payload_to_string(const sisl::byte_array& p) {
    if (!p) return "";
    return std::string(reinterpret_cast<const char*>(p->cbytes()), p->size());
}

// ─── Tests ───────────────────────────────────────────────────────────────────

class NvmeSuperblockFallbackTest : public ::testing::Test {
protected:
    void SetUp() override {
        m_nvme = std::make_shared<MockNvmeDeviceIO>(64 * 1024);
        m_s3 = std::make_shared<MockS3ForFallback>();
    }

    std::shared_ptr<MockNvmeDeviceIO> m_nvme;
    std::shared_ptr<MockS3ForFallback> m_s3;
};

// 1. First write bootstraps both slots
TEST_F(NvmeSuperblockFallbackTest, BootstrapWritesBothSlots) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    auto payload = make_payload("hello-bootstrap");
    mgr.write(payload);

    EXPECT_EQ(mgr.current_sequence(), 1u);
    EXPECT_TRUE(mgr.active_slot().has_value());
    EXPECT_EQ(*mgr.active_slot(), 0u);

    // Both slots should be readable and valid
    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    EXPECT_NE(loaded, nullptr);
    EXPECT_EQ(result.slot_a_valid, true);
    EXPECT_EQ(result.slot_b_valid, true);
    EXPECT_EQ(result.source, SuperblockSource::NVME_SLOT_A);  // tie-break: A wins with same seq
    EXPECT_EQ(payload_to_string(loaded), "hello-bootstrap");
}

// 2. Ping-pong writes alternate slots
TEST_F(NvmeSuperblockFallbackTest, PingPongAlternates) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    mgr.write(make_payload("write-1"));  // Bootstrap: both slots, seq=1
    mgr.write(make_payload("write-2"));  // Ping-pong: slot B, seq=2
    mgr.write(make_payload("write-3"));  // Ping-pong: slot A, seq=3

    EXPECT_EQ(mgr.current_sequence(), 3u);

    // Load should pick write-3 (seq=3, slot A)
    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    EXPECT_EQ(payload_to_string(loaded), "write-3");
    EXPECT_EQ(result.sequence_number, 3u);
    EXPECT_TRUE(result.slot_a_valid);
    EXPECT_TRUE(result.slot_b_valid);
}

// 3. Slot A corrupted, Slot B survives
TEST_F(NvmeSuperblockFallbackTest, SlotACorruptedUsesSlotB) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    mgr.write(make_payload("initial"));     // Bootstrap: both, seq=1
    mgr.write(make_payload("newer-data"));  // Slot B, seq=2

    // Corrupt slot A
    m_nvme->corrupt(SLOT_A_OFFSET, 64);

    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    EXPECT_NE(loaded, nullptr);
    EXPECT_FALSE(result.slot_a_valid);
    EXPECT_TRUE(result.slot_b_valid);
    EXPECT_EQ(result.source, SuperblockSource::NVME_SLOT_B);
    EXPECT_EQ(payload_to_string(loaded), "newer-data");
}

// 4. Slot B corrupted, Slot A survives
TEST_F(NvmeSuperblockFallbackTest, SlotBCorruptedUsesSlotA) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    mgr.write(make_payload("data-v1"));  // Bootstrap: both, seq=1
    mgr.write(make_payload("data-v2"));  // Slot B, seq=2
    mgr.write(make_payload("data-v3"));  // Slot A, seq=3

    // Corrupt slot B
    m_nvme->corrupt(SLOT_B_OFFSET, 128);

    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    EXPECT_NE(loaded, nullptr);
    EXPECT_TRUE(result.slot_a_valid);
    EXPECT_FALSE(result.slot_b_valid);
    EXPECT_EQ(result.source, SuperblockSource::NVME_SLOT_A);
    EXPECT_EQ(payload_to_string(loaded), "data-v3");
}

// 5. Both slots corrupted — S3 fallback succeeds
TEST_F(NvmeSuperblockFallbackTest, BothCorruptedS3Fallback) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    mgr.write(make_payload("some-data"));

    // Corrupt both slots
    m_nvme->corrupt(SLOT_A_OFFSET, 128);
    m_nvme->corrupt(SLOT_B_OFFSET, 128);

    // Put recovery data in S3
    auto s3_payload = make_payload("recovered-from-s3");
    m_s3->put_blob("test-vol/pdev_superblock.bin", s3_payload);

    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load(m_s3.get(), "test-vol");

    EXPECT_NE(loaded, nullptr);
    EXPECT_FALSE(result.slot_a_valid);
    EXPECT_FALSE(result.slot_b_valid);
    EXPECT_EQ(result.source, SuperblockSource::S3_FALLBACK);
    EXPECT_EQ(payload_to_string(loaded), "recovered-from-s3");
    EXPECT_EQ(result.sequence_number, 0u);  // S3 doesn't carry NVMe sequence
}

// 6. Both slots corrupted, no S3 — total failure
TEST_F(NvmeSuperblockFallbackTest, BothCorruptedNoS3) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    mgr.write(make_payload("data"));

    m_nvme->corrupt(SLOT_A_OFFSET, 128);
    m_nvme->corrupt(SLOT_B_OFFSET, 128);

    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();  // No S3 store provided

    EXPECT_EQ(loaded, nullptr);
    EXPECT_EQ(result.source, SuperblockSource::NONE);
    EXPECT_FALSE(result.error_message.empty());
}

// 7. Both slots corrupted, S3 also fails — total failure
TEST_F(NvmeSuperblockFallbackTest, BothCorruptedS3AlsoFails) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    mgr.write(make_payload("data"));

    m_nvme->corrupt(SLOT_A_OFFSET, 128);
    m_nvme->corrupt(SLOT_B_OFFSET, 128);

    // S3 has no data for this volume
    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load(m_s3.get(), "nonexistent-vol");

    EXPECT_EQ(loaded, nullptr);
    EXPECT_EQ(result.source, SuperblockSource::NONE);
    EXPECT_FALSE(result.error_message.empty());
}

// 8. Empty device (no writes) — load returns NONE
TEST_F(NvmeSuperblockFallbackTest, EmptyDeviceReturnsNone) {
    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    EXPECT_EQ(loaded, nullptr);
    EXPECT_EQ(result.source, SuperblockSource::NONE);
    EXPECT_FALSE(result.slot_a_valid);
    EXPECT_FALSE(result.slot_b_valid);
}

// 9. Write after S3 recovery re-bootstraps both slots
TEST_F(NvmeSuperblockFallbackTest, WriteAfterS3RecoveryRebootstraps) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    mgr.write(make_payload("original"));

    m_nvme->corrupt(SLOT_A_OFFSET, 128);
    m_nvme->corrupt(SLOT_B_OFFSET, 128);

    auto s3_payload = make_payload("s3-state");
    m_s3->put_blob("vol/pdev_superblock.bin", s3_payload);

    // Load from S3
    auto [loaded, result] = mgr.load(m_s3.get(), "vol");
    EXPECT_EQ(result.source, SuperblockSource::S3_FALLBACK);

    // Write new data — should bootstrap both slots again
    mgr.write(make_payload("post-recovery"));

    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [reloaded, result2] = reader.load();

    EXPECT_TRUE(result2.slot_a_valid);
    EXPECT_TRUE(result2.slot_b_valid);
    EXPECT_EQ(payload_to_string(reloaded), "post-recovery");
}

// 10. Crash during write to inactive slot — active slot survives
TEST_F(NvmeSuperblockFallbackTest, CrashDuringWriteActiveSurvives) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    mgr.write(make_payload("v1-good"));   // Bootstrap: both, seq=1
    mgr.write(make_payload("v2-good"));   // Slot B, seq=2

    // Simulate crash during third write to slot A: corrupt slot A
    // (In reality the write would be partial; we simulate with corruption)
    mgr.write(make_payload("v3-crashed"));  // This wrote to slot A, seq=3
    m_nvme->corrupt(SLOT_A_OFFSET, 256);    // Simulate crash-corrupted write

    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    // Slot B should still have v2-good
    EXPECT_NE(loaded, nullptr);
    EXPECT_FALSE(result.slot_a_valid);
    EXPECT_TRUE(result.slot_b_valid);
    EXPECT_EQ(result.source, SuperblockSource::NVME_SLOT_B);
    EXPECT_EQ(payload_to_string(loaded), "v2-good");
}

// 11. Many writes — sequence numbers are monotonic
TEST_F(NvmeSuperblockFallbackTest, SequenceNumbersMonotonic) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    for (int i = 1; i <= 20; ++i) {
        mgr.write(make_payload("data-" + std::to_string(i)));
        EXPECT_EQ(mgr.current_sequence(), static_cast<uint64_t>(i));
    }

    // Load should return the last write
    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    EXPECT_EQ(payload_to_string(loaded), "data-20");
    EXPECT_EQ(result.sequence_number, 20u);
}

// 12. Large payload close to slot size
TEST_F(NvmeSuperblockFallbackTest, LargePayload) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    // Max payload = SLOT_SIZE - header
    uint32_t max_payload = SLOT_SIZE - sizeof(nvme_sb_slot_header);
    std::string large_data(max_payload, 'X');
    mgr.write(make_payload(large_data));

    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    EXPECT_EQ(payload_to_string(loaded), large_data);
}

// 13. Slot with bad magic is detected
TEST_F(NvmeSuperblockFallbackTest, BadMagicDetected) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    mgr.write(make_payload("test"));

    // Overwrite magic bytes in slot A (first 4 bytes)
    uint32_t bad_magic = 0xDEADDEAD;
    m_nvme->sync_write(reinterpret_cast<const uint8_t*>(&bad_magic), 4, SLOT_A_OFFSET);

    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    EXPECT_FALSE(result.slot_a_valid);
    EXPECT_TRUE(result.slot_b_valid);
    EXPECT_EQ(result.source, SuperblockSource::NVME_SLOT_B);
}

// 14. Verify CRC catches single-bit payload corruption
TEST_F(NvmeSuperblockFallbackTest, CRCCatchesBitFlip) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    mgr.write(make_payload("integrity-check"));

    // Flip one bit in the payload area of slot A (after header)
    uint64_t payload_offset = SLOT_A_OFFSET + sizeof(nvme_sb_slot_header) + 5;
    uint8_t byte;
    m_nvme->sync_read(&byte, 1, payload_offset);
    byte ^= 0x01;  // flip one bit
    m_nvme->sync_write(&byte, 1, payload_offset);

    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    EXPECT_FALSE(result.slot_a_valid);
    EXPECT_TRUE(result.slot_b_valid);
    EXPECT_EQ(payload_to_string(loaded), "integrity-check");
}

// 15. Older slot is ignored when both are valid but have different data
TEST_F(NvmeSuperblockFallbackTest, NewerSlotWins) {
    NvmeSuperblockManager mgr(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);

    mgr.write(make_payload("v1"));  // Bootstrap both, seq=1
    mgr.write(make_payload("v2"));  // Slot B, seq=2

    // Both valid, slot B is newer
    NvmeSuperblockManager reader(m_nvme, SLOT_A_OFFSET, SLOT_B_OFFSET, SLOT_SIZE);
    auto [loaded, result] = reader.load();

    EXPECT_TRUE(result.slot_a_valid);
    EXPECT_TRUE(result.slot_b_valid);
    EXPECT_EQ(result.source, SuperblockSource::NVME_SLOT_B);
    EXPECT_EQ(payload_to_string(loaded), "v2");
    EXPECT_EQ(result.sequence_number, 2u);
}

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_nvme_sb_fallback");
    spdlog::set_pattern("[%D %T.%f] [%^%L%$] [%n] [%t] %v");

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
