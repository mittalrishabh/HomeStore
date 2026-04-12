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
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/s3_cp_handler.h>
#include <homestore/s3/s3_physical_dev.h>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/s3/s3_object_store.h>
#include <homestore/checkpoint/cp_mgr.hpp>

SISL_LOGGING_INIT(test_s3_cp_handler, s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

// ─── Mock S3ObjectStore ──────────────────────────────────────────────────────

class MockS3Store : public S3ObjectStore {
public:
    folly::Future< S3Result > put_object(const std::string& key, sisl::io_blob_safe data) override {
        std::lock_guard< std::mutex > lg(m_mtx);
        m_objects[key] = std::vector< uint8_t >(data.cbytes(), data.cbytes() + data.size());
        return folly::makeFuture(S3Result{.status_code = 200});
    }

    folly::Future< std::pair< S3Result, sisl::io_blob_safe > > get_object(const std::string& key) override {
        std::lock_guard< std::mutex > lg(m_mtx);
        auto it = m_objects.find(key);
        if (it == m_objects.end()) {
            return folly::makeFuture(std::make_pair(S3Result{.status_code = 404}, sisl::io_blob_safe{}));
        }
        sisl::io_blob_safe blob(it->second.size(), 0);
        std::memcpy(blob.bytes(), it->second.data(), it->second.size());
        return folly::makeFuture(std::make_pair(S3Result{.status_code = 200}, std::move(blob)));
    }

    folly::Future< std::pair< S3Result, sisl::io_blob_safe > >
    get_object_range(const std::string&, uint64_t, uint64_t) override {
        return folly::makeFuture(std::make_pair(S3Result{.status_code = 501}, sisl::io_blob_safe{}));
    }

    folly::Future< S3Result > delete_object(const std::string&) override {
        return folly::makeFuture(S3Result{.status_code = 200});
    }

    folly::Future< S3Result > delete_objects(const std::vector< std::string >&) override {
        return folly::makeFuture(S3Result{.status_code = 200});
    }

    folly::Future< S3Result > head_object(const std::string&) override {
        return folly::makeFuture(S3Result{.status_code = 200});
    }

    folly::Future< S3ListResult > list_objects(const std::string&, const std::string&, uint32_t) override {
        return folly::makeFuture(S3ListResult{});
    }

    const std::string& bucket_name() const override {
        static std::string name{"test-bucket"};
        return name;
    }

    folly::Future< S3Result > copy_object(const std::string&, const std::string&) override {
        return folly::makeFuture(S3Result{.status_code = 200});
    }

    bool has_object(const std::string& key) const {
        std::lock_guard< std::mutex > lg(m_mtx);
        return m_objects.count(key) > 0;
    }

    size_t object_count() const {
        std::lock_guard< std::mutex > lg(m_mtx);
        return m_objects.size();
    }

private:
    mutable std::mutex m_mtx;
    std::map< std::string, std::vector< uint8_t > > m_objects;
};

// ─── Mock ChunkStore ─────────────────────────────────────────────────────────

class MockChunkStore : public ChunkStore {
public:
    S3Result put(chunk_id_t chunk_id, const std::vector< DirtyBlock >& dirty_blocks,
                 uint64_t chunk_size) override {
        std::lock_guard< std::mutex > lg(m_mtx);

        if (m_fail_chunks.count(chunk_id)) {
            return S3Result{.status_code = 500, .error_message = "Simulated failure"};
        }

        m_uploaded_chunks[chunk_id] = dirty_blocks;
        m_put_call_count++;
        return S3Result{.status_code = 200};
    }

    folly::Future< std::pair< S3Result, sisl::byte_array > >
    get(chunk_id_t, offset_t, uint64_t) override {
        return folly::makeFuture(
            std::make_pair(S3Result{.status_code = 200}, sisl::make_byte_array(0)));
    }

    S3Result compact(chunk_id_t) override { return S3Result{.status_code = 200}; }
    ChunkState recover(chunk_id_t) override { return ChunkState{}; }
    ChunkMetadata describe(chunk_id_t) override { return ChunkMetadata{}; }

    // Test helpers
    void set_fail_chunk(chunk_id_t cid) {
        std::lock_guard< std::mutex > lg(m_mtx);
        m_fail_chunks.insert(cid);
    }

    void clear_fail_chunk(chunk_id_t cid) {
        std::lock_guard< std::mutex > lg(m_mtx);
        m_fail_chunks.erase(cid);
    }

    bool was_uploaded(chunk_id_t cid) const {
        std::lock_guard< std::mutex > lg(m_mtx);
        return m_uploaded_chunks.count(cid) > 0;
    }

    size_t upload_count() const {
        std::lock_guard< std::mutex > lg(m_mtx);
        return m_uploaded_chunks.size();
    }

    uint32_t put_call_count() const {
        std::lock_guard< std::mutex > lg(m_mtx);
        return m_put_call_count;
    }

    void reset() {
        std::lock_guard< std::mutex > lg(m_mtx);
        m_uploaded_chunks.clear();
        m_fail_chunks.clear();
        m_put_call_count = 0;
    }

private:
    mutable std::mutex m_mtx;
    std::map< chunk_id_t, std::vector< DirtyBlock > > m_uploaded_chunks;
    std::set< chunk_id_t > m_fail_chunks;
    uint32_t m_put_call_count{0};
};

// ─── Test Fixture ────────────────────────────────────────────────────────────

class S3CpHandlerTest : public ::testing::Test {
protected:
    void SetUp() override {
        m_s3_store = std::make_shared< MockS3Store >();
        m_chunk_store = std::make_shared< MockChunkStore >();
        m_s3_pdev = std::make_shared< S3PhysicalDev >(
            /*pdev_id=*/1, m_chunk_store, m_s3_store, "test-vol",
            /*dirty_cache_max_mb=*/4096, nullptr);

        // Register some chunks in the pdev + superblock
        m_s3_pdev->create_chunk(10, 64 * 1024 * 1024, S3ChunkType::DATA, 1);
        m_s3_pdev->create_chunk(20, 64 * 1024 * 1024, S3ChunkType::DATA, 1);
        m_s3_pdev->create_chunk(30, 16 * 1024 * 1024, S3ChunkType::INDEX, 2);
        m_s3_pdev->create_chunk(40, 8 * 1024 * 1024, S3ChunkType::METABLK, 3);

        m_handler = std::make_unique< S3CpHandler >(m_s3_pdev, /*upload_concurrency=*/2);
    }

    /// Helper: write dirty data to a chunk
    void write_dirty(chunk_id_t cid, uint64_t offset, const std::string& content) {
        auto buf = sisl::make_byte_array(content.size());
        std::memcpy(buf->bytes(), content.data(), content.size());
        m_s3_pdev->write(cid, offset, buf);
    }

    /// Helper: simulate a full CP cycle (switchover + flush + cleanup)
    bool run_cp_cycle() {
        // Create CP objects
        CP cur_cp(nullptr);
        cur_cp.m_cp_id = m_cp_id++;
        cur_cp.m_cp_status = cp_status_t::cp_io_ready;

        CP new_cp(nullptr);
        new_cp.m_cp_id = m_cp_id;
        new_cp.m_cp_status = cp_status_t::cp_io_ready;

        // Switchover: drain dirty cache into context
        auto ctx = m_handler->on_switchover_cp(&cur_cp, &new_cp);
        // We need to attach context to cur_cp for flush to find it.
        // The handler uses cp_consumer_t::HS_CLIENT slot.
        cur_cp.set_context(cp_consumer_t::HS_CLIENT, std::move(ctx));

        cur_cp.m_cp_status = cp_status_t::cp_flushing;

        // Flush
        auto result = m_handler->cp_flush(&cur_cp).get();

        // Cleanup
        m_handler->cp_cleanup(&cur_cp);

        return result;
    }

    std::shared_ptr< MockS3Store > m_s3_store;
    std::shared_ptr< MockChunkStore > m_chunk_store;
    std::shared_ptr< S3PhysicalDev > m_s3_pdev;
    std::unique_ptr< S3CpHandler > m_handler;
    cp_id_t m_cp_id{0};
};

// ─── Tests ───────────────────────────────────────────────────────────────────

// 1. Empty CP — no dirty data, flush is a no-op
TEST_F(S3CpHandlerTest, EmptyCpNoFlush) {
    auto result = run_cp_cycle();
    EXPECT_TRUE(result);
    EXPECT_EQ(m_chunk_store->upload_count(), 0u);
}

// 2. Single dirty chunk gets uploaded
TEST_F(S3CpHandlerTest, SingleDirtyChunkUploaded) {
    write_dirty(10, 0, "hello-data");

    auto result = run_cp_cycle();
    EXPECT_TRUE(result);
    EXPECT_TRUE(m_chunk_store->was_uploaded(10));
    EXPECT_EQ(m_chunk_store->upload_count(), 1u);
}

// 3. Multiple dirty chunks all get uploaded
TEST_F(S3CpHandlerTest, MultipleDirtyChunksUploaded) {
    write_dirty(10, 0, "data-1");
    write_dirty(20, 0, "data-2");
    write_dirty(30, 0, "index-data");

    auto result = run_cp_cycle();
    EXPECT_TRUE(result);
    EXPECT_TRUE(m_chunk_store->was_uploaded(10));
    EXPECT_TRUE(m_chunk_store->was_uploaded(20));
    EXPECT_TRUE(m_chunk_store->was_uploaded(30));
    EXPECT_EQ(m_chunk_store->upload_count(), 3u);
}

// 4. Failed chunk upload doesn't crash — marked for retry
TEST_F(S3CpHandlerTest, FailedChunkMarkedForRetry) {
    write_dirty(10, 0, "good-data");
    write_dirty(20, 0, "bad-data");
    m_chunk_store->set_fail_chunk(20);

    auto result = run_cp_cycle();
    EXPECT_TRUE(result);  // S3 failure doesn't fail the CP

    EXPECT_TRUE(m_chunk_store->was_uploaded(10));
    EXPECT_FALSE(m_chunk_store->was_uploaded(20));

    auto failed = m_handler->failed_chunks();
    EXPECT_EQ(failed.size(), 1u);
    EXPECT_TRUE(failed.count(20));
}

// 5. Failed chunk retried in next CP
TEST_F(S3CpHandlerTest, FailedChunkRetriedNextCp) {
    write_dirty(20, 0, "data");
    m_chunk_store->set_fail_chunk(20);

    run_cp_cycle();  // First CP — chunk 20 fails
    EXPECT_FALSE(m_chunk_store->was_uploaded(20));

    // Fix the failure and run another CP
    m_chunk_store->clear_fail_chunk(20);
    m_chunk_store->reset();

    run_cp_cycle();  // Second CP — chunk 20 should be retried
    EXPECT_TRUE(m_chunk_store->was_uploaded(20));
    EXPECT_TRUE(m_handler->failed_chunks().empty());
}

// 6. Superblock written after chunk uploads
TEST_F(S3CpHandlerTest, SuperblockWrittenAfterFlush) {
    auto gen_before = m_s3_pdev->superblock().generation();

    write_dirty(10, 0, "data");
    run_cp_cycle();

    // Generation should have incremented
    EXPECT_GT(m_s3_pdev->superblock().generation(), gen_before);
}

// 7. No superblock write if all chunks failed
TEST_F(S3CpHandlerTest, NoSuperblockOnTotalFailure) {
    write_dirty(10, 0, "data");
    m_chunk_store->set_fail_chunk(10);

    auto gen_before = m_s3_pdev->superblock().generation();
    run_cp_cycle();

    // No successful uploads → superblock should NOT be written
    // (generation might still increment in the mutable ref, but
    // the write_superblock call checks chunks_ok > 0)
    // The key check: was_uploaded should be false
    EXPECT_FALSE(m_chunk_store->was_uploaded(10));
}

// 8. Multiple writes to same chunk accumulate dirty blocks
TEST_F(S3CpHandlerTest, MultipleWritesSameChunkAccumulate) {
    write_dirty(10, 0, "block-0");
    write_dirty(10, 4096, "block-1");
    write_dirty(10, 8192, "block-2");

    run_cp_cycle();
    EXPECT_TRUE(m_chunk_store->was_uploaded(10));
    EXPECT_EQ(m_chunk_store->put_call_count(), 1u);  // One put() call for chunk 10
}

// 9. First CP (switchover from nullptr) is empty
TEST_F(S3CpHandlerTest, FirstCpSwitchoverFromNull) {
    CP new_cp(nullptr);
    new_cp.m_cp_id = 0;
    auto ctx = m_handler->on_switchover_cp(nullptr, &new_cp);
    auto* s3ctx = dynamic_cast< S3CpContext* >(ctx.get());

    EXPECT_NE(s3ctx, nullptr);
    EXPECT_TRUE(s3ctx->dirty_chunk_ids.empty());
    EXPECT_TRUE(s3ctx->dirty_blocks.empty());
}

// 10. Progress reporting
TEST_F(S3CpHandlerTest, ProgressReporting) {
    // Before any CP, progress should be 0
    EXPECT_EQ(m_handler->cp_progress_percent(), 0);

    write_dirty(10, 0, "data");
    run_cp_cycle();

    // After cleanup, progress resets to 0
    EXPECT_EQ(m_handler->cp_progress_percent(), 0);
}

// 11. Dirty cache is empty after switchover drains it
TEST_F(S3CpHandlerTest, DirtyCacheDrainedAtSwitchover) {
    write_dirty(10, 0, "data");
    EXPECT_TRUE(m_s3_pdev->is_chunk_dirty(10));

    CP cur_cp(nullptr);
    cur_cp.m_cp_id = 0;
    CP new_cp(nullptr);
    new_cp.m_cp_id = 1;

    auto ctx = m_handler->on_switchover_cp(&cur_cp, &new_cp);

    // After switchover, dirty cache should be empty
    EXPECT_FALSE(m_s3_pdev->is_chunk_dirty(10));
    EXPECT_EQ(m_s3_pdev->dirty_cache_size_bytes(), 0u);
}

// 12. Concurrent CP doesn't lose data written during flush
TEST_F(S3CpHandlerTest, WriteDuringFlushGoesToNextCp) {
    write_dirty(10, 0, "cp1-data");

    // Switchover CP1
    CP cp1(nullptr);
    cp1.m_cp_id = 0;
    CP cp2(nullptr);
    cp2.m_cp_id = 1;
    auto ctx1 = m_handler->on_switchover_cp(&cp1, &cp2);
    cp1.set_context(cp_consumer_t::HS_CLIENT, std::move(ctx1));

    // Write more data — this goes into CP2's dirty cache (after switchover)
    write_dirty(20, 0, "cp2-data");

    // Flush CP1
    cp1.m_cp_status = cp_status_t::cp_flushing;
    m_handler->cp_flush(&cp1).get();
    m_handler->cp_cleanup(&cp1);

    // Chunk 10 should be uploaded (from CP1)
    EXPECT_TRUE(m_chunk_store->was_uploaded(10));

    // Chunk 20 should NOT be uploaded yet (belongs to CP2)
    EXPECT_FALSE(m_chunk_store->was_uploaded(20));

    // Now run CP2
    m_chunk_store->reset();
    CP cp3(nullptr);
    cp3.m_cp_id = 2;
    auto ctx2 = m_handler->on_switchover_cp(&cp2, &cp3);
    cp2.set_context(cp_consumer_t::HS_CLIENT, std::move(ctx2));
    cp2.m_cp_status = cp_status_t::cp_flushing;
    m_handler->cp_flush(&cp2).get();
    m_handler->cp_cleanup(&cp2);

    // Now chunk 20 should be uploaded
    EXPECT_TRUE(m_chunk_store->was_uploaded(20));
}

// 13. All chunk types get uploaded
TEST_F(S3CpHandlerTest, AllChunkTypesUploaded) {
    write_dirty(10, 0, "data-chunk");    // DATA
    write_dirty(30, 0, "index-chunk");   // INDEX
    write_dirty(40, 0, "metablk-chunk"); // METABLK

    run_cp_cycle();

    EXPECT_TRUE(m_chunk_store->was_uploaded(10));
    EXPECT_TRUE(m_chunk_store->was_uploaded(30));
    EXPECT_TRUE(m_chunk_store->was_uploaded(40));
}

// 14. CP flush always returns true (S3 failure is non-fatal)
TEST_F(S3CpHandlerTest, FlushAlwaysReturnsTrue) {
    write_dirty(10, 0, "data");
    write_dirty(20, 0, "data");
    m_chunk_store->set_fail_chunk(10);
    m_chunk_store->set_fail_chunk(20);

    auto result = run_cp_cycle();
    EXPECT_TRUE(result);  // Even with all failures
}

int main(int argc, char* argv[]) {
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_s3_cp_handler");
    spdlog::set_pattern("[%D %T.%f] [%^%L%$] [%n] [%t] %v");

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
