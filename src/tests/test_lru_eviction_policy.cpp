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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <set>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sisl/logging/logging.h>
#include <sisl/options/options.h>

#include <homestore/s3/lru_eviction_policy.h>

SISL_LOGGING_INIT(s3)
SISL_OPTIONS_ENABLE(logging)

using namespace homestore;

///////////////////////////////////////////////////////////////////////////////
// Basic LRU Behavior
///////////////////////////////////////////////////////////////////////////////

TEST(LruEvictionPolicyTest, EmptyPolicyReturnsNoCandidate) {
    LruEvictionPolicy policy;
    chunk_id_t id;
    uint64_t size;
    ASSERT_FALSE(policy.select_eviction_candidate(id, size));
}

TEST(LruEvictionPolicyTest, SingleChunkSelectedAndRemoved) {
    LruEvictionPolicy policy;
    policy.add_chunk(1, 4096);

    ASSERT_EQ(policy.tracked_count(), 1u);
    ASSERT_TRUE(policy.is_tracked(1));

    chunk_id_t id;
    uint64_t size;
    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 1u);
    ASSERT_EQ(size, 4096u);

    ASSERT_EQ(policy.tracked_count(), 0u);
    ASSERT_FALSE(policy.is_tracked(1));
    ASSERT_FALSE(policy.select_eviction_candidate(id, size));
}

TEST(LruEvictionPolicyTest, LruOrderWithoutAccess) {
    LruEvictionPolicy policy;
    policy.add_chunk(10, 1000);
    policy.add_chunk(20, 2000);
    policy.add_chunk(30, 3000);

    chunk_id_t id;
    uint64_t size;

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 10u);

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 20u);

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 30u);

    ASSERT_FALSE(policy.select_eviction_candidate(id, size));
}

TEST(LruEvictionPolicyTest, AccessMovesToBack) {
    LruEvictionPolicy policy;
    policy.add_chunk(1, 100);
    policy.add_chunk(2, 200);
    policy.add_chunk(3, 300);

    // Access chunk 1 — moves to back
    policy.record_access(1);

    chunk_id_t id;
    uint64_t size;

    // LRU order should now be: 2, 3, 1
    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 2u);

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 3u);

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 1u);
}

TEST(LruEvictionPolicyTest, MultipleAccessesReorder) {
    LruEvictionPolicy policy;
    policy.add_chunk(1, 100);
    policy.add_chunk(2, 200);
    policy.add_chunk(3, 300);
    policy.add_chunk(4, 400);

    // Access pattern: 1, 3, 2
    policy.record_access(1);
    policy.record_access(3);
    policy.record_access(2);

    // Expected LRU order: 4, 1, 3, 2
    chunk_id_t id;
    uint64_t size;

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 4u);

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 1u);

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 3u);

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 2u);
}

///////////////////////////////////////////////////////////////////////////////
// Add / Remove
///////////////////////////////////////////////////////////////////////////////

TEST(LruEvictionPolicyTest, DuplicateAddIsNoOp) {
    LruEvictionPolicy policy;
    policy.add_chunk(5, 500);
    policy.add_chunk(5, 999);

    ASSERT_EQ(policy.tracked_count(), 1u);

    chunk_id_t id;
    uint64_t size;
    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 5u);
    ASSERT_EQ(size, 500u); // original size preserved
}

TEST(LruEvictionPolicyTest, RemoveChunk) {
    LruEvictionPolicy policy;
    policy.add_chunk(1, 100);
    policy.add_chunk(2, 200);
    policy.add_chunk(3, 300);

    policy.remove_chunk(2);
    ASSERT_EQ(policy.tracked_count(), 2u);
    ASSERT_FALSE(policy.is_tracked(2));

    chunk_id_t id;
    uint64_t size;

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 1u);

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 3u);

    ASSERT_FALSE(policy.select_eviction_candidate(id, size));
}

TEST(LruEvictionPolicyTest, RemoveNonExistentChunkIsNoOp) {
    LruEvictionPolicy policy;
    policy.add_chunk(1, 100);
    policy.remove_chunk(999);
    ASSERT_EQ(policy.tracked_count(), 1u);
}

TEST(LruEvictionPolicyTest, AccessNonExistentChunkIsNoOp) {
    LruEvictionPolicy policy;
    policy.add_chunk(1, 100);
    policy.record_access(999);
    ASSERT_EQ(policy.tracked_count(), 1u);
}

///////////////////////////////////////////////////////////////////////////////
// Re-add after eviction
///////////////////////////////////////////////////////////////////////////////

TEST(LruEvictionPolicyTest, ReaddAfterEviction) {
    LruEvictionPolicy policy;
    policy.add_chunk(1, 100);
    policy.add_chunk(2, 200);

    chunk_id_t id;
    uint64_t size;

    // Evict chunk 1
    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 1u);

    // Re-add chunk 1 (e.g., after re-hydration)
    policy.add_chunk(1, 100);
    ASSERT_EQ(policy.tracked_count(), 2u);

    // Chunk 2 is older, should be evicted first
    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 2u);

    ASSERT_TRUE(policy.select_eviction_candidate(id, size));
    ASSERT_EQ(id, 1u);
}

///////////////////////////////////////////////////////////////////////////////
// Integration with EvictionCandidateSelector interface
///////////////////////////////////////////////////////////////////////////////

TEST(LruEvictionPolicyTest, WorksAsEvictionCandidateSelector) {
    auto policy = std::make_shared< LruEvictionPolicy >();
    policy->add_chunk(10, 4096);
    policy->add_chunk(20, 8192);

    // Use through the abstract interface
    EvictionCandidateSelector* selector = policy.get();

    chunk_id_t id;
    uint64_t size;

    ASSERT_TRUE(selector->select_eviction_candidate(id, size));
    ASSERT_EQ(id, 10u);
    ASSERT_EQ(size, 4096u);

    ASSERT_TRUE(selector->select_eviction_candidate(id, size));
    ASSERT_EQ(id, 20u);
    ASSERT_EQ(size, 8192u);
}

///////////////////////////////////////////////////////////////////////////////
// Concurrent Access
///////////////////////////////////////////////////////////////////////////////

TEST(LruEvictionPolicyTest, ConcurrentAccessesAreThreadSafe) {
    LruEvictionPolicy policy;
    constexpr uint32_t NUM_CHUNKS = 100;

    for (uint32_t i = 0; i < NUM_CHUNKS; ++i) {
        policy.add_chunk(i, 4096);
    }

    std::vector< std::thread > threads;

    // Concurrent record_access from multiple threads
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([&policy, t]() {
            for (int i = 0; i < 1000; ++i) {
                policy.record_access(static_cast< chunk_id_t >((t * 1000 + i) % NUM_CHUNKS));
            }
        });
    }

    for (auto& th : threads) { th.join(); }

    ASSERT_EQ(policy.tracked_count(), NUM_CHUNKS);
}

TEST(LruEvictionPolicyTest, ConcurrentAddRemove) {
    LruEvictionPolicy policy;
    std::atomic< bool > stop{false};

    std::thread adder([&]() {
        for (uint32_t i = 0; i < 200; ++i) {
            policy.add_chunk(i, 4096);
        }
    });

    std::thread remover([&]() {
        for (uint32_t i = 0; i < 100; ++i) {
            policy.remove_chunk(i);
        }
    });

    adder.join();
    remover.join();

    // Some chunks may or may not be present depending on timing
    ASSERT_LE(policy.tracked_count(), 200u);
}

TEST(LruEvictionPolicyTest, ConcurrentSelectAndAccess) {
    LruEvictionPolicy policy;
    constexpr uint32_t NUM_CHUNKS = 50;

    for (uint32_t i = 0; i < NUM_CHUNKS; ++i) {
        policy.add_chunk(i, 4096);
    }

    std::set< chunk_id_t > evicted;
    std::mutex evicted_mutex;

    std::thread evictor([&]() {
        for (uint32_t i = 0; i < 20; ++i) {
            chunk_id_t id;
            uint64_t size;
            if (policy.select_eviction_candidate(id, size)) {
                std::lock_guard lock{evicted_mutex};
                evicted.insert(id);
            }
        }
    });

    std::thread accessor([&]() {
        for (uint32_t i = 0; i < 1000; ++i) {
            policy.record_access(static_cast< chunk_id_t >(i % NUM_CHUNKS));
        }
    });

    evictor.join();
    accessor.join();

    // 20 chunks should have been evicted
    ASSERT_EQ(evicted.size(), 20u);
    ASSERT_EQ(policy.tracked_count(), NUM_CHUNKS - 20);
}

///////////////////////////////////////////////////////////////////////////////
// Large-Scale LRU Correctness
///////////////////////////////////////////////////////////////////////////////

TEST(LruEvictionPolicyTest, LargeScaleLruOrder) {
    LruEvictionPolicy policy;
    constexpr uint32_t N = 1000;

    for (uint32_t i = 0; i < N; ++i) {
        policy.add_chunk(i, 4096);
    }

    // Access all chunks in reverse order — chunk 999 is LRU, chunk 0 is MRU
    for (uint32_t i = 0; i < N; ++i) {
        policy.record_access(N - 1 - i);
    }

    // Eviction should follow reverse order: 999, 998, 997, ...
    for (uint32_t i = 0; i < 10; ++i) {
        chunk_id_t id;
        uint64_t size;
        ASSERT_TRUE(policy.select_eviction_candidate(id, size));
        ASSERT_EQ(id, N - 1 - i);
    }
}

int main(int argc, char* argv[]) {
    ::testing::InitGoogleTest(&argc, argv);
    SISL_OPTIONS_LOAD(argc, argv, logging);
    sisl::logging::SetLogger("test_lru_eviction_policy");
    spdlog::set_pattern("[%D %T%z] [%^%l%$] [%n] [%t] %v");
    return RUN_ALL_TESTS();
}
