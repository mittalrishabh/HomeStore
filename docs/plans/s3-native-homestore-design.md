# S3-Native HomeStore Design Document

**Author:** Architect Agent  
**Date:** 2026-04-04  
**Status:** Draft  
**Reviewers:** Rishabh Mittal

---

## 1. Executive Summary

This document describes the design for making HomeStore S3-native. The goal is to use S3 as the durable filesystem for data, NVMe as a hot read/write cache, and EBS exclusively for binlog (WAL) durability. On instance failure, recovery restores data from S3 and replays binlogs from EBS, targeting RTO in the order of minutes.

### Key Design Principles
- **S3 is the filesystem, not a backup target.** Immutable data segments live permanently in S3.
- **NVMe is ephemeral.** It is a write buffer and read cache. Losing it is expected.
- **EBS carries only binlogs.** Sequential append fsync to EBS for commit durability.
- **Immutable segments get automatic S3 durability.** Once uploaded, no further writes.
- **Zero RPO.** Binlog fsync to EBS before commit ack ensures no committed data loss.
- **Minutes RTO.** Parallel S3 download + short binlog replay.

---

## 2. Current Architecture (Baseline)

```
┌─────────────────────────────────────────────────────┐
│                    HomeBlocks                        │
│  Volume (LBA → BlkId B+Tree index + solo repl_dev) │
├─────────────────────────────────────────────────────┤
│                    HomeStore                         │
│  ┌──────────┐ ┌──────────┐ ┌────────┐ ┌──────────┐ │
│  │ IndexSvc │ │ DataSvc  │ │ LogSvc │ │ MetaSvc  │ │
│  │ (B+Tree) │ │ (blocks) │ │ (WAL)  │ │ (K/V sb) │ │
│  └────┬─────┘ └────┬─────┘ └───┬────┘ └────┬─────┘ │
│       │             │           │            │       │
│  ┌────┴─────────────┴───────────┴────────────┴────┐ │
│  │              VirtualDev / ChunkMgr              │ │
│  │          BlkAllocator (varsize/append)           │ │
│  └─────────────────────┬───────────────────────────┘ │
│                        │                             │
│  ┌─────────────────────┴───────────────────────────┐ │
│  │         PhysicalDev (io_uring / iomgr)          │ │
│  └─────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
                         │
                    Local NVMe
```

**All data lives on local NVMe.** Recovery reads everything from local disk. No S3 involvement.

### Current Write Path (Volume::write)
1. `rd()->alloc_blks()` — allocate block IDs from chunk's BlkAllocator
2. `rd()->async_write()` — write data to NVMe via VirtualDev → PhysicalDev
3. `indx_table()->write_to_index()` — update B+Tree mapping (LBA → BlkId)
4. Journal the mapping for crash recovery (via repl_dev journal)
5. CP flush periodically persists dirty B+Tree nodes and allocator bitmaps

### Current Read Path (Volume::read)
1. `indx_table()->read_from_index()` — lookup B+Tree for BlkId
2. `rd()->async_read()` — read data from NVMe via VirtualDev → PhysicalDev

---

## 3. Target Architecture

```
┌──────────────────────────────────────────────────────────┐
│                      HomeBlocks                           │
│   Volume (LBA → BlkId B+Tree + solo repl_dev)            │
├──────────────────────────────────────────────────────────┤
│                      HomeStore                            │
│  ┌──────────┐ ┌─────────────┐ ┌────────┐ ┌──────────┐   │
│  │ IndexSvc │ │ DataSvc     │ │ LogSvc │ │ MetaSvc  │   │
│  │ (B+Tree) │ │ (tiered)    │ │ (WAL)  │ │ (K/V sb) │   │
│  └────┬─────┘ └──────┬──────┘ └───┬────┘ └────┬─────┘   │
│       │              │             │            │         │
│  ┌────┴──────────────┴─────────────┴────────────┴──────┐ │
│  │                VirtualDev / ChunkMgr                 │ │
│  │            BlkAllocator + S3TierTracker              │ │
│  └────────┬────────────────────────────┬───────────────┘ │
│           │                            │                 │
│  ┌────────┴─────────┐    ┌─────────────┴──────────────┐  │
│  │    PhysicalDev   │    │    S3DataStore (NEW)        │  │
│  │   (NVMe cache)   │    │  (durable data filesystem) │  │
│  └──────────────────┘    └────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
         │                              │
    Local NVMe                     Amazon S3
    (ephemeral)               (11 nines durable)

         ┌──────────────────┐
         │   EBS Volume     │
         │  (binlog only)   │
         └──────────────────┘
```

---

## 4. Detailed Design

### 4.1 Tiering Strategy: Data-Only S3 Tiering

**Decision: Tier data blocks to S3. Keep index, metadata, and logs on NVMe (and EBS for binlogs).**

Rationale:
- Data blocks are written once and read many times — naturally immutable after allocation
- Index B+Tree nodes are modified in-place with complex dependency chains (wb_cache) — poor fit for S3's object model
- Index is small relative to data (a 32-byte KV entry per data page). A 1TB volume with 4KB pages = ~8GB of index, vs ~1TB of data
- On recovery, index can be reconstructed by replaying the binlog against the data

This means:
- `BlkDataService` gets an S3 backend
- `IndexSvc` stays NVMe-only (reconstructed on recovery)
- `LogSvc` stays on the EBS-backed journal device
- `MetaSvc` superblocks are persisted to S3 as small metadata objects

### 4.2 The Chunk as the S3 Object Unit

**Key insight: the Chunk is the natural unit for S3 objects.**

Currently, HomeStore divides physical devices into fixed-size Chunks (configurable, typically 64MB-1GB). Each Chunk has its own BlkAllocator. This maps cleanly to S3:

- Each Chunk = one S3 object
- Chunk ID → S3 key: `s3://<bucket>/<volume-id>/data/chunk-<chunk_id>`
- When a Chunk is fully written and sealed (no more allocations), it becomes immutable → upload to S3
- Chunks that are still being written to stay on NVMe only

**Chunk Lifecycle:**

```
  ACTIVE              SEALED              UPLOADED           EVICTABLE
  ┌──────┐           ┌──────┐            ┌──────┐           ┌──────┐
  │NVMe  │  full →   │NVMe  │  upload →  │NVMe  │  evict →  │S3    │
  │only  │           │only  │  to S3     │+ S3  │  from     │only  │
  │      │           │      │            │      │  NVMe     │      │
  └──────┘           └──────┘            └──────┘           └──────┘
  
  BlkAllocator        No new allocs       S3 confirmed       NVMe freed
  active              Chunk sealed        durable            read from S3
```

### 4.3 S3DataStore — New Component

A new component `S3DataStore` encapsulates all S3 interactions:

```cpp
class S3DataStore {
public:
    // Lifecycle
    S3DataStore(const S3Config& config);
    
    // Upload a sealed chunk to S3
    folly::Future<std::error_code> upload_chunk(chunk_num_t chunk_id, 
                                                 const char* data, 
                                                 uint64_t size);
    
    // Download a chunk from S3 (for recovery or cache miss)
    folly::Future<std::error_code> download_chunk(chunk_num_t chunk_id,
                                                   char* buf,
                                                   uint64_t size);
    
    // Read a specific block range from an S3-resident chunk
    folly::Future<std::error_code> read_block_range(chunk_num_t chunk_id,
                                                     uint64_t offset,
                                                     uint64_t size,
                                                     char* buf);
    
    // Check if a chunk exists in S3
    folly::Future<bool> chunk_exists(chunk_num_t chunk_id);
    
    // Upload metadata (superblocks, allocator state)
    folly::Future<std::error_code> upload_metadata(const std::string& key,
                                                    const sisl::byte_view& data);
    
    // List all chunks for a volume (recovery)
    folly::Future<std::vector<chunk_num_t>> list_chunks(const std::string& volume_prefix);
    
    // Delete a chunk from S3 (when volume is destroyed)
    folly::Future<std::error_code> delete_chunk(chunk_num_t chunk_id);
    
private:
    // S3 client (AWS SDK)
    std::shared_ptr<Aws::S3::S3Client> m_s3_client;
    std::string m_bucket;
    std::string m_prefix;
    
    // Upload thread pool (don't block reactor threads)
    folly::IOThreadPoolExecutor m_upload_pool;
    
    // Multipart upload for large chunks
    folly::Future<std::error_code> multipart_upload(const std::string& key,
                                                     const char* data,
                                                     uint64_t size);
};
```

**S3 Object Layout:**
```
s3://<bucket>/<instance-id>/
  ├── data/
  │   ├── chunk-0000000001          # Sealed data chunk
  │   ├── chunk-0000000002
  │   ├── chunk-0000000005
  │   └── ...
  ├── meta/
  │   ├── superblock.bin            # HomeStore super block
  │   ├── device_manifest.json      # Chunk → volume mapping
  │   └── allocator_state.bin       # Block allocator bitmaps
  └── index/
      └── <volume-id>/
          └── snapshot-<cp_id>.bin  # Optional: periodic index snapshot
```

### 4.4 Chunk Sealing and Upload Pipeline

#### 4.4.1 When is a Chunk Sealed?

A data chunk is sealed when it is full (no more blocks can be allocated from it). This is detected in the `BlkAllocator`:

```cpp
// In VarsizeBlkAllocator or AppendBlkAllocator
BlkAllocStatus alloc(...) {
    auto status = do_alloc(...);
    if (available_blks() == 0 || available_blks() < min_useful_blks) {
        // Chunk is full — notify the tier manager
        m_chunk_sealed_cb(m_chunk_id);
    }
    return status;
}
```

For `AppendBlkAllocator` (append-only workloads), sealing is natural — once the offset reaches the end.

For `VarsizeBlkAllocator` (random alloc/free), sealing is trickier because blocks can be freed and re-allocated. Two options:
- **Option A:** Seal when allocated-count reaches a high watermark (e.g., 95%) and no frees are pending for this CP
- **Option B:** Never seal varsize chunks — only append-allocated chunks go to S3. Varsize stays NVMe-only.

**Recommendation: Use AppendBlkAllocator for data chunks.** This is already the natural fit for a write-once-read-many workload. Data is append-allocated, and old blocks are freed but never reused in the same chunk. The freed space is recovered at the S3 tier through chunk compaction (Section 4.8).

#### 4.4.2 CP-Triggered Upload

The upload is triggered as part of the CP (checkpoint) flow. A new CP consumer is registered:

```cpp
class S3TierCPCallbacks : public CPCallbacks {
public:
    std::unique_ptr<CPContext> on_switchover_cp(CP* cur_cp, CP* new_cp) override {
        // Collect the list of chunks sealed during this CP
        return std::make_unique<S3TierCPContext>(m_sealed_chunks.exchange({}));
    }
    
    folly::Future<bool> cp_flush(CP* cp) override {
        auto ctx = static_cast<S3TierCPContext*>(cp->context(cp_consumer_t::S3_TIER));
        
        std::vector<folly::Future<bool>> futs;
        for (auto chunk_id : ctx->sealed_chunks()) {
            auto chunk = m_dmgr.get_chunk(chunk_id);
            // Read entire chunk from NVMe
            auto buf = hs_utils::iobuf_alloc(chunk->size(), sisl::buftag::data, chunk->physical_dev()->align_size());
            auto read_fut = chunk->physical_dev_mutable()->async_read(buf, chunk->size(), chunk->start_offset());
            
            futs.emplace_back(
                std::move(read_fut).thenValue([this, chunk_id, buf, size = chunk->size()](auto&& ec) {
                    if (ec) return folly::makeFuture<bool>(false);
                    // Upload to S3
                    return m_s3_store->upload_chunk(chunk_id, buf, size)
                        .thenValue([this, chunk_id, buf](auto&& ec) {
                            hs_utils::iobuf_free(buf, sisl::buftag::data);
                            if (!ec) {
                                // Mark chunk as S3-durable
                                m_tier_tracker->mark_s3_durable(chunk_id);
                            }
                            return !ec;
                        });
                })
            );
        }
        
        return folly::collectAllUnsafe(futs).thenValue([](auto&& results) {
            return std::all_of(results.begin(), results.end(), 
                              [](auto& r) { return r.value(); });
        });
    }
};
```

**Critical ordering:** S3 upload happens *after* DataSvc and IndexSvc have flushed to NVMe for this CP, but *before* the CP is finalized. This ensures the data is consistent on NVMe before we snapshot it to S3.

The CP consumer ordering:
1. `BLK_DATA_SVC` — flush dirty data to NVMe
2. `INDEX_SVC` — flush dirty B+Tree nodes to NVMe  
3. `S3_TIER` (new) — upload sealed chunks to S3
4. `SEALER` (replication) — update committed LSN

#### 4.4.3 Chunk State Tracker (`S3TierTracker`)

A new metadata structure tracks the S3 durability state of each chunk:

```cpp
enum class chunk_tier_state_t : uint8_t {
    NVME_ONLY,      // Active, writable, NVMe-resident only
    SEALED,         // Full, no more allocations, pending S3 upload
    S3_UPLOADING,   // Upload in progress
    S3_DURABLE,     // Confirmed in S3, still cached on NVMe
    S3_ONLY,        // Evicted from NVMe, reads go to S3
};

class S3TierTracker {
    // Per-chunk state, persisted via MetaSvc
    struct chunk_tier_info {
        chunk_num_t chunk_id;
        chunk_tier_state_t state;
        uint64_t s3_upload_cp_id;    // CP at which chunk was uploaded
        uint64_t s3_object_size;     // Size in S3
        std::string s3_etag;         // S3 ETag for verification
    };
    
    std::unordered_map<chunk_num_t, chunk_tier_info> m_chunk_states;
    
public:
    void mark_sealed(chunk_num_t id);
    void mark_s3_durable(chunk_num_t id);
    void mark_evicted(chunk_num_t id);
    
    chunk_tier_state_t get_state(chunk_num_t id) const;
    std::vector<chunk_num_t> get_sealed_chunks() const;
    std::vector<chunk_num_t> get_evictable_chunks() const;
    
    // Persisted via MetaSvc for crash recovery
    void persist();
    void recover(const sisl::byte_view& buf);
};
```

### 4.5 Tiered Read Path

The read path needs to handle blocks that may be on NVMe, S3, or both:

```cpp
// Modified VirtualDev::async_read
folly::Future<std::error_code> VirtualDev::async_read(char* buf, uint64_t size, 
                                                       BlkId const& bid, bool part_of_batch) {
    Chunk* pchunk;
    uint64_t const dev_offset = to_dev_offset(bid, &pchunk);
    
    auto tier_state = m_tier_tracker->get_state(bid.chunk_num());
    
    switch (tier_state) {
    case chunk_tier_state_t::NVME_ONLY:
    case chunk_tier_state_t::SEALED:
    case chunk_tier_state_t::S3_UPLOADING:
    case chunk_tier_state_t::S3_DURABLE:
        // Data is on NVMe (possibly also S3) — read from NVMe
        return pchunk->physical_dev_mutable()->async_read(buf, size, dev_offset, part_of_batch);
        
    case chunk_tier_state_t::S3_ONLY:
        // Data has been evicted from NVMe — read from S3
        // Calculate offset within the chunk
        uint64_t offset_in_chunk = dev_offset - pchunk->start_offset();
        return m_s3_store->read_block_range(bid.chunk_num(), offset_in_chunk, size, buf);
    }
}
```

#### 4.5.1 Read-Through Cache (NVMe as Cache)

When reading from S3, we can optionally re-cache the chunk on NVMe for subsequent reads:

```cpp
// Cache policy: re-cache hot chunks from S3 to NVMe
folly::Future<std::error_code> VirtualDev::async_read_with_cache(char* buf, uint64_t size,
                                                                  BlkId const& bid) {
    auto tier_state = m_tier_tracker->get_state(bid.chunk_num());
    
    if (tier_state == chunk_tier_state_t::S3_ONLY) {
        // Check if this chunk is hot enough to re-cache
        m_read_counter[bid.chunk_num()]++;
        
        if (m_read_counter[bid.chunk_num()] > RECACHE_THRESHOLD) {
            // Background: pull entire chunk back to NVMe
            schedule_chunk_recache(bid.chunk_num());
        }
        
        // For this read: fetch from S3 (byte-range GET)
        uint64_t offset_in_chunk = /* calculated from bid */;
        return m_s3_store->read_block_range(bid.chunk_num(), offset_in_chunk, size, buf);
    }
    
    // NVMe-resident: direct read
    return direct_nvme_read(buf, size, bid);
}
```

### 4.6 NVMe Cache Eviction

Once a chunk is S3_DURABLE, its NVMe space can be reclaimed for new writes. Eviction is triggered by NVMe capacity pressure:

```cpp
class NVMeCacheManager {
    // Evict chunks from NVMe when space is needed
    void evict_if_needed() {
        auto free_pct = m_dmgr.free_capacity_pct();
        if (free_pct > LOW_WATERMARK_PCT) return;
        
        // Evict oldest S3_DURABLE chunks first (LRU)
        auto evictable = m_tier_tracker->get_evictable_chunks();
        std::sort(evictable.begin(), evictable.end(), 
                  [](auto& a, auto& b) { return a.s3_upload_cp_id < b.s3_upload_cp_id; });
        
        for (auto& chunk_id : evictable) {
            if (free_pct > HIGH_WATERMARK_PCT) break;
            
            // Release chunk's NVMe blocks
            auto chunk = m_dmgr.get_chunk(chunk_id);
            chunk->blk_allocator_mutable()->free_all();
            m_tier_tracker->mark_evicted(chunk_id);
            
            free_pct = m_dmgr.free_capacity_pct();
        }
    }
    
    static constexpr float LOW_WATERMARK_PCT = 20.0f;   // Start evicting
    static constexpr float HIGH_WATERMARK_PCT = 40.0f;  // Stop evicting
};
```

### 4.7 Write Path Changes

The write path changes are minimal. The key difference is that data chunks use `AppendBlkAllocator` and are sealed when full:

```
Volume::write (unchanged logic)
  │
  ├── 1. alloc_blks() ← AppendBlkAllocator (chunk may get sealed here)
  ├── 2. async_write() → NVMe (unchanged)
  ├── 3. write_to_index() → B+Tree on NVMe (unchanged)  
  ├── 4. journal write → binlog fsync to EBS (unchanged — this is the durability point)
  │
  └── [CP boundary]
        ├── DataSvc cp_flush → NVMe (unchanged)
        ├── IndexSvc cp_flush → NVMe (unchanged)  
        └── S3TierSvc cp_flush → sealed chunks uploaded to S3 (NEW)
```

**Commit ack flow:**
1. Data written to NVMe ✓
2. Binlog fsynced to EBS ✓ — *this is where commit is acked to client*
3. Index updated in memory ✓
4. (Async, at CP) data uploaded to S3 ← NOT in commit path

The commit latency is unchanged. S3 upload is background work at CP time.

### 4.8 Chunk Compaction (Garbage Collection)

Over time, blocks within S3-resident chunks get freed (overwrites, deletes). This creates dead space in S3 objects. Compaction reclaims this space:

```
Chunk in S3:  [live][dead][live][live][dead][dead][live]
                              │
                     Compact (background)
                              │
                              ▼
New Chunk:    [live][live][live][live]
```

**Compaction triggers:**
- When dead-block ratio exceeds a threshold (e.g., 40% dead)
- Background process, does not block foreground I/O

**Compaction flow:**
1. Identify chunks with high dead-block ratio (from allocator bitmaps)
2. Allocate a new chunk on NVMe
3. Copy live blocks from S3 chunk to new NVMe chunk
4. Update B+Tree index to point to new BlkIds
5. Upload new chunk to S3
6. Delete old S3 object
7. Free old chunk metadata

This is similar to LSM compaction and can run as a background task scheduled by `CPManager` or a dedicated compaction thread.

### 4.9 Recovery Flow

This is the most significant change. Current recovery reads everything from local NVMe. S3-native recovery pulls data from S3 and replays binlogs.

#### 4.9.1 Full Recovery Flow (Instance Failure)

```
Instance dies (NVMe lost)
  │
  ├── 1. Launch new instance with fresh NVMe + existing EBS (binlog)
  │
  ├── 2. Read MetaSvc from S3: s3://.../meta/superblock.bin
  │      → Recovers: device layout, chunk mapping, tier state, CP id
  │
  ├── 3. Download S3-durable data chunks (parallel)
  │      → For each chunk in tier_state == S3_DURABLE or S3_ONLY:
  │         s3://.../data/chunk-NNNN → local NVMe
  │      → Only download chunks needed for active volumes
  │      → Can be lazy: download on first access (see 4.9.2)
  │
  ├── 4. Recover LogSvc from EBS (existing binlog replay path)
  │      → Replay binlog entries from last S3-consistent CP
  │      → This rebuilds any data written after last S3 upload
  │
  ├── 5. Rebuild IndexSvc (B+Tree)
  │      → Option A: Replay all index operations from binlog
  │      → Option B: Download index snapshot from S3 + replay delta
  │
  └── 6. Instance is online
```

**RTO Analysis:**
- Step 2: ~1 second (small metadata)
- Step 3: Proportional to data size, but parallelizable. 1TB at 5GB/s S3 throughput = ~200 seconds. With lazy loading, can be deferred.
- Step 4: Replay only entries since last CP. At 1-minute CP intervals, ~1 minute of binlog.
- Step 5: B+Tree rebuild from binlog is the bottleneck. For large datasets, consider periodic index snapshots to S3.
- **Total: ~3-5 minutes for a 1TB volume with 1-minute CPs**

#### 4.9.2 Lazy Recovery (Optimization)

Instead of downloading all chunks upfront, use lazy loading:

1. Download only MetaSvc + tier state from S3
2. Replay binlog from EBS (rebuilds in-memory state + recent data)
3. Mark all S3-only chunks as `S3_ONLY` tier state
4. Start serving requests immediately
5. Reads to S3_ONLY chunks fetch from S3 on demand (with NVMe caching)

This reduces RTO to just the binlog replay time (seconds to a minute), at the cost of higher read latency until the NVMe cache warms up.

#### 4.9.3 MetaSvc S3 Persistence

HomeStore's MetaSvc stores superblocks. These must also be recoverable from S3:

```cpp
// After each CP, persist critical metadata to S3
void persist_meta_to_s3(CP* cp) {
    // 1. Device manifest (chunk → volume mapping)
    auto manifest = m_dmgr.serialize_manifest();
    m_s3_store->upload_metadata("meta/device_manifest.bin", manifest);
    
    // 2. Tier tracker state
    auto tier_state = m_tier_tracker->serialize();
    m_s3_store->upload_metadata("meta/tier_state.bin", tier_state);
    
    // 3. CP superblock (last flushed CP id)
    auto cp_sb = cp_mgr().serialize_sb();
    m_s3_store->upload_metadata("meta/cp_superblock.bin", cp_sb);
    
    // 4. Allocator bitmaps (which blocks are live in each chunk)
    for (auto& [chunk_id, chunk] : m_all_chunks) {
        auto bitmap = chunk->blk_allocator()->serialize();
        m_s3_store->upload_metadata(
            fmt::format("meta/allocator/chunk-{}.bin", chunk_id), bitmap);
    }
}
```

### 4.10 Binlog Integration

The binlog on EBS serves two purposes:
1. **Commit durability** — fsync before ack ensures zero RPO
2. **Recovery bridge** — covers the gap between last S3 upload and crash

**Binlog trimming:** Once a CP completes and all sealed chunks are uploaded to S3, binlog entries before that CP can be trimmed. This keeps EBS usage bounded.

```cpp
// After S3 upload confirms, trim binlog
void on_s3_upload_complete(uint64_t cp_id) {
    // All data up to cp_id is now in S3
    // Trim binlog entries before cp_id
    m_log_service->truncate(cp_id);
}
```

**Binlog size bound:** With 1-minute CPs and 100MB/s write throughput, maximum binlog size = ~6GB. EBS gp3 baseline is fine for this.

---

## 5. S3 Object Layout and Naming

```
s3://<bucket>/<cluster-id>/<instance-id>/
  ├── data/
  │   ├── chunk-00000001.dat           # Data chunk (raw block data)
  │   ├── chunk-00000002.dat
  │   └── ...
  ├── meta/
  │   ├── superblock-<cp_id>.bin       # HomeStore superblock at CP
  │   ├── tier_state-<cp_id>.bin       # Chunk tier states at CP
  │   ├── device_manifest-<cp_id>.bin  # Device/chunk/volume mapping
  │   └── allocator/
  │       ├── chunk-00000001.bin       # Allocator bitmap
  │       └── ...
  └── index/                           # Optional index snapshots
      └── <volume-id>/
          └── snapshot-<cp_id>.bin
```

**Versioning:** Metadata objects are suffixed with `cp_id` to support point-in-time recovery and avoid overwrite races. A `latest` pointer file tracks the most recent consistent CP.

---

## 6. Configuration

```cpp
struct S3TierConfig {
    // S3 connection
    std::string bucket;
    std::string region;
    std::string prefix;              // e.g., "cluster-1/instance-1"
    
    // Upload tuning
    uint32_t upload_threads{4};      // Concurrent S3 uploads
    uint64_t multipart_threshold{64 * Mi};  // Use multipart above this
    uint64_t multipart_part_size{16 * Mi};  // Part size for multipart
    
    // Eviction policy
    float evict_low_watermark_pct{20.0f};   // Start evicting NVMe
    float evict_high_watermark_pct{40.0f};  // Stop evicting
    
    // Compaction
    float compaction_dead_ratio{0.4f};      // Compact when >40% dead
    uint32_t compaction_threads{2};
    
    // Recovery
    bool lazy_recovery{true};               // Lazy chunk loading
    uint32_t recovery_download_threads{8};  // Parallel S3 downloads
    
    // Read cache
    uint32_t recache_threshold{10};         // Reads before re-caching
};
```

---

## 7. Implementation Phases

### Phase 1: Foundation (Weeks 1-3)
- [ ] Implement `S3DataStore` with upload/download/read_block_range
- [ ] Implement `S3TierTracker` with MetaSvc persistence
- [ ] Add `chunk_tier_state_t` to Chunk metadata
- [ ] Switch data VDev to use `AppendBlkAllocator`
- [ ] Add chunk sealing detection

### Phase 2: Upload Pipeline (Weeks 4-5)
- [ ] Implement `S3TierCPCallbacks` as new CP consumer
- [ ] Register in CP consumer ordering (after INDEX_SVC, before SEALER)
- [ ] Implement CP-triggered upload for sealed chunks
- [ ] Add tier state to MetaSvc persistence

### Phase 3: Tiered Read Path (Weeks 6-7)
- [ ] Modify `VirtualDev::async_read` for tiered reads
- [ ] Implement S3 byte-range GET for block-level reads
- [ ] Implement NVMe cache eviction (`NVMeCacheManager`)
- [ ] Add read-through caching for S3-only chunks

### Phase 4: Recovery (Weeks 8-10)
- [ ] Implement S3 metadata recovery (superblock, tier state, allocator bitmaps)
- [ ] Implement chunk download from S3
- [ ] Implement lazy recovery mode
- [ ] Implement index rebuild from binlog replay
- [ ] Implement binlog trimming after S3 upload
- [ ] End-to-end recovery testing

### Phase 5: Compaction & Production Hardening (Weeks 11-13)
- [ ] Implement chunk compaction (GC)
- [ ] Add S3 upload retry logic with exponential backoff
- [ ] Add metrics and monitoring (upload latency, cache hit rate, S3 costs)
- [ ] Failure injection testing (S3 unavailable, partial uploads, NVMe failure mid-CP)
- [ ] Performance benchmarking (write throughput impact, read latency with S3 fallback)

---

## 8. Risk Analysis and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| S3 upload fails mid-CP | Chunk stuck in SEALED state, NVMe fills up | Retry with backoff. Chunk stays on NVMe until upload succeeds. Alert on repeated failures. |
| S3 read latency on cache miss | P99 read latency spikes | Lazy pre-fetch of nearby chunks. Read-through caching. Optimize for sequential access patterns. |
| NVMe fills before S3 upload completes | Write stalls | Backpressure: slow down writes when NVMe utilization > 80%. Prioritize S3 uploads. |
| Index rebuild takes too long on recovery | RTO exceeds target | Periodic index snapshots to S3 (optional). Parallel replay. |
| S3 eventual consistency | Stale reads after upload | Use S3 strong consistency (available since Dec 2020 for all operations). |
| Chunk compaction amplifies writes | Increased S3 PUT costs | Tune dead ratio threshold. Only compact during low-traffic periods. |
| Binlog on EBS fills up | Cannot accept new writes | Aggressive binlog trimming after S3 upload. Monitor EBS utilization. |

---

## 9. Metrics and Observability

```
# S3 Tier Metrics
s3_tier_chunks_sealed_total          # Chunks sealed (cumulative)
s3_tier_chunks_uploaded_total        # Chunks uploaded to S3
s3_tier_chunks_evicted_total         # Chunks evicted from NVMe
s3_tier_upload_latency_seconds       # Upload latency histogram
s3_tier_upload_bytes_total           # Total bytes uploaded
s3_tier_read_cache_hit_ratio         # NVMe cache hit rate
s3_tier_s3_read_latency_seconds      # S3 read latency histogram
s3_tier_s3_read_bytes_total          # Bytes read from S3
s3_tier_nvme_utilization_pct         # NVMe capacity utilization
s3_tier_compaction_bytes_total       # Bytes compacted
s3_tier_binlog_lag_bytes             # Binlog size since last S3 upload
```

---

## 10. Open Questions

1. **Chunk size for S3:** Current chunk sizes (64MB-1GB) are reasonable for S3 objects. Should we standardize on a specific size for data chunks? Larger chunks = fewer S3 objects but slower individual uploads. Recommend: 256MB as default.

2. **Multi-region DR:** Should S3 cross-region replication be the DR strategy? This gives automatic geo-redundancy but adds cost. Alternative: upload to S3 in multiple regions from the upload pipeline.

3. **Index snapshots:** Are periodic B+Tree snapshots to S3 worth the complexity? They reduce recovery time but require serializing the in-place-modified B+Tree. The binlog-only rebuild may be sufficient if binlog replay is fast enough.

4. **Encryption:** Should data be encrypted at rest in S3? S3 SSE-S3 or SSE-KMS adds minimal overhead. Recommend SSE-S3 as default.

5. **AppendBlkAllocator vs VarsizeBlkAllocator:** Switching all data chunks to AppendBlkAllocator simplifies sealing but changes allocation behavior. Need to verify that append-only allocation doesn't degrade write patterns for database workloads.

---

## 11. Appendix: Code Change Summary

### HomeStore Changes
| File | Change |
|------|--------|
| `src/lib/device/virtual_dev.cpp` | Tiered read path, cache eviction hooks |
| `src/lib/device/chunk.h` | Add `chunk_tier_state_t` field |
| `src/lib/blkdata_svc/blkdata_service.cpp` | Wire S3DataStore to read path |
| `src/lib/checkpoint/cp_mgr.cpp` | Register S3_TIER CP consumer, ordering |
| `src/lib/homestore.cpp` | Initialize S3DataStore and S3TierTracker |
| NEW: `src/lib/s3_tier/s3_data_store.cpp` | S3 client wrapper |
| NEW: `src/lib/s3_tier/s3_tier_tracker.cpp` | Chunk tier state machine |
| NEW: `src/lib/s3_tier/s3_tier_cp.cpp` | CP callbacks for S3 upload |
| NEW: `src/lib/s3_tier/nvme_cache_mgr.cpp` | NVMe cache eviction |
| NEW: `src/lib/s3_tier/chunk_compactor.cpp` | GC / compaction |

### HomeBlocks Changes
| File | Change |
|------|--------|
| `src/lib/homeblks_impl.cpp` | Pass S3 config, init S3 tier during startup |
| `src/lib/volume/volume.cpp` | Recovery path changes (S3 + binlog replay) |
