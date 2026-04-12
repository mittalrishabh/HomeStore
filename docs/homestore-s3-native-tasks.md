# HomeStore S3-Native: Implementation Tasks

**Reference:** `docs/homestore-s3-native-design.md` (v5)
**Status:** Task breakdown — NOT started. Awaiting instructions before coding.

**Architecture:** S3 as native pdev with mirrored chunks. Dynamic chunk placement. Single shared bitmap.
**Implementation strategy:** v1 (full chunk upload) first, but using v3-forward `ChunkStore` interface so the SST/LSM swap is zero caller changes.

---

## Phase 1: S3 pdev + Full Chunk Upload (v1 MVP)

The goal of Phase 1 is to implement S3 as a native pdev in HomeStore's device hierarchy, wire it into vdev's mirrored chunk mechanism, and get dirty chunks uploaded to S3 at each checkpoint.

---

### Task 1.1: S3 Object Store Abstraction

**Goal:** Create a low-level abstraction for S3 operations. Nothing HomeStore-specific — just a clean interface over S3.

**What to build:**
- An `S3ObjectStore` class/module with methods:
  - `put_object(key, data)` → upload bytes to S3 key
  - `get_object(key)` → download bytes from S3 key
  - `get_object_range(key, offset, length)` → partial read (for v3 SST lookups)
  - `delete_object(key)` → delete an S3 object
  - `delete_objects(keys)` → batch delete (for compaction cleanup)
  - `head_object(key)` → check if object exists, get size
  - `list_objects(prefix)` → list objects under a prefix
  - `copy_object(src_key, dst_key)` → server-side copy (for clones)
- Configuration: bucket name, region, credentials, endpoint URL (for S3-compatible stores like MinIO)
- Error handling: retries with exponential backoff for transient failures (5xx, timeouts)
- Logging: log all S3 operations with latency for debugging

**Key decisions:**
- Use the AWS SDK (C++ or Rust depending on HomeStore's build system)
- All operations are async (return futures/callbacks)
- PUT operations must verify upload via checksum (Content-MD5 or SHA-256)
- Include `get_object_range`, `delete_objects`, `copy_object` now — v1 won't use all of them but v3/clones will

**Acceptance criteria:**
- Can upload, download (full + range), delete (single + batch), copy, and list S3 objects
- Handles transient S3 errors with retry
- Unit tests with a mock S3 backend (or LocalStack/MinIO)

**Dependencies:** None. This is standalone.

---

### Task 1.2: pdev_s3 — S3 Physical Device

**Goal:** Implement S3 as a new pdev type in HomeStore's DeviceManager. This is the core integration that makes S3 a native device.

**What to build:**
- `S3PhysicalDev` class implementing the pdev interface:
  - `write(chunk_id, offset, data)` → stashes dirty block in memory cache, marks chunk dirty (no immediate S3 upload)
  - `read(chunk_id, offset, size)` → reads from S3 via ChunkStore::get()
  - `create_chunk(chunk_id)` → registers chunk for S3 backing
  - `remove_chunk(chunk_id)` → removes chunk's S3 data
  - `drain_dirty_cache(chunk_id)` → returns and clears dirty blocks for CP upload
- *Dirty cache:* `map<chunk_id, vector<DirtyBlock>>` — in-memory cache of all writes since last CP
  - Each write stashes a `sisl::byte_array` (shared_ptr bump, not a data copy)
  - At CP time: cache is drained into `chunk_store->put(chunk_id, dirty_blocks)`
  - Avoids re-reading from NVMe at CP time
  - Essential for v3 (SST) — provides exact dirty blocks for SST file
- *Memory bound:* configurable `s3.dirty_cache_max_mb` (default: 4096). If exceeded, force early CP flush.
- Registration in DeviceManager as a new pdev type

**How it integrates:**
- DeviceManager creates pdev_s3 alongside pdev_nvme at startup (if S3 is enabled)
- VirtualDev's mirrored chunk mechanism pairs each chunk across both pdevs
- On write: NVMe pdev does sync write, pdev_s3 caches dirty block in memory
- Mirror write waits for BOTH pdev completions before replying to upper layer
  (pdev_s3.write is ~instant since it's a cache insert, so no latency impact)
- On CP: pdev_s3 drains dirty cache → ChunkStore::put()

**Key decisions:**
- pdev_s3 writes cache dirty blocks in memory (not deferred to NVMe re-read)
- Data is `sisl::byte_array` — shared_ptr, so caching is a pointer bump not a copy
- pdev_s3 reads go through ChunkStore::get() (which handles v1 full-chunk and v3 SST transparently)
- pdev_s3 must handle the case where a chunk only exists on S3 (no NVMe mirror — evicted or recovery scenario)
- Early CP flush triggered if dirty cache exceeds memory threshold

**Acceptance criteria:**
- pdev_s3 registered in DeviceManager alongside pdev_nvme
- VirtualDev can create mirrored chunks across both pdevs
- Writes accepted without blocking on S3
- Reads from S3 return correct data
- Unit tests with mock S3

**Dependencies:** Task 1.1 (S3ObjectStore), Task 1.3 (ChunkStore)

---

### Task 1.3: ChunkStore Interface + FullChunkStore (v1)

**Goal:** Implement the v3-forward `ChunkStore` abstraction with the v1 `FullChunkStore` backend. This is the key interface that ALL S3 data access goes through.

**What to build:**

*Abstract interface:*
```cpp
struct DirtyBlock {
    offset_t offset;              // offset within chunk
    sisl::byte_array data;        // shared_ptr<io_blob_safe> — refcounted, aligned, RAII
};

class ChunkStore {
public:
    virtual S3Result put(chunk_id_t chunk_id,
                         const std::vector<DirtyBlock>& dirty_blocks) = 0;

    virtual folly::Future<Data> get(chunk_id_t chunk_id,
                                     offset_t offset) = 0;

    virtual S3Result compact(chunk_id_t chunk_id) = 0;

    virtual ChunkState recover(chunk_id_t chunk_id) = 0;

    virtual ChunkMetadata describe(chunk_id_t chunk_id) = 0;
};
```

*v1 implementation (`FullChunkStore`):*
- `put()`: ignores `dirty_blocks` granularity, reads full chunk from NVMe, uploads as single S3 object
- `get()`: downloads full S3 object, seeks to offset, returns block (with local caching to avoid repeated GETs)
- `compact()`: no-op (already a single object)
- `recover()`: downloads single S3 object, returns data for NVMe restore
- `describe()`: returns single S3 key for the chunk

*Chunk-to-S3 mapping (embedded):*
- Bucket per cluster: `homestore-<cluster_id>`, volume prefix: `<volume_id>/`
- S3 key format: `<volume_id>/chunks/<chunk_id>/data.dat` (no snapshots) or `<volume_id>/chunks/<chunk_id>/data_gen<N>.dat` (with snapshots)

**Key decisions:**
- `put()` receives `dirty_blocks` even in v1 — the interface captures dirty block info that v3's `SSTChunkStore` will use
- `get()` in v1 downloads the full chunk and caches it locally to avoid repeated S3 GETs
- The interface is the contract. v1 is just the first implementation.

**Acceptance criteria:**
- `FullChunkStore` correctly uploads/downloads full chunks via S3ObjectStore
- All callers use only the `ChunkStore` interface
- Unit tests verify round-trip: put → get returns correct data at correct offset
- `compact()` is a no-op but doesn't error

**Dependencies:** Task 1.1 (S3ObjectStore)

---

### Task 1.4: pdev_s3 Superblock

**Goal:** Define and implement the pdev_s3 superblock — the single S3 metadata object that mirrors pdev_nvme's superblock structure plus snapshot state. This is the entry point for all S3 recovery.

**What to build:**

*pdev_s3 superblock:*
```
pdev_s3_superblock {
  pdev_id: u64
  generation: u64
  chunks: [
    { chunk_id: u64,
      chunk_size: u64,
      chunk_type: enum,       // data, index, WAL, metablk
      s3_key: string,         // current S3 object for this chunk
      vdev_id: u64 }
  ]
  snapshots: []               // empty for Phase 1
  checksum: u64
}
```
S3 key: `<volume_id>/pdev_superblock.bin`

*No per-chunk superblocks or volume index needed.* All chunk info is in pdev_s3 superblock. All HomeStore metadata (bitmap, CP state) is in MetaBlk chunks (uploaded as regular chunks).

- Serialization/deserialization (protobuf, flatbuffers, or plain binary — match HomeStore conventions)
- `write_pdev_superblock(S3ObjectStore, sb)` → serialize + PUT
- `read_pdev_superblock(S3ObjectStore)` → GET + deserialize + verify checksum

**Key decisions:**
- Mirrors pdev_nvme superblock structure — same chunk layout info
- Written last during CP (S3 commit point)
- Single entry point for recovery: download this → know all chunks and their S3 keys
- MetaBlk chunks identified by `chunk_type: metablk` — downloaded first during recovery
- No NVMe info (device offsets, etc.) in S3 metadata

**Acceptance criteria:**
- Can write and read pdev_s3 superblock to/from S3
- Checksum validation catches corruption
- Round-trip test: write → read → compare
- Correctly identifies MetaBlk chunks by type for recovery

**Dependencies:** Task 1.1 (S3ObjectStore)

---

### Task 1.5: CP Hook — Flush Dirty Chunks to S3 via ChunkStore

**Goal:** Hook into CPManager's checkpoint flow. After the normal NVMe flush completes, upload all dirty chunks to S3 via the `ChunkStore` interface.

**What to build:**
- A CP listener/callback that fires after NVMe checkpoint flush is complete
- For each dirty chunk (tracked by pdev_s3's dirty cache):
  - Drain dirty cache: `dirty_blocks = pdev_s3->drain_dirty_cache(chunk_id)`
  - Call `chunk_store->put(chunk_id, dirty_blocks)`
  - Upload bitmap to S3 for this chunk
  - Update chunk superblock on S3
  - On success: clear dirty flag
  - On failure: log error, retry next CP (chunk stays dirty)
- After all uploads: update volume index on S3

**S3 flush ordering (must match NVMe CP order):**
```
Normal NVMe CP flush (existing, unchanged)
    │
    ▼
S3 flush (matching NVMe order):
  a. Data chunks:
     For each dirty data chunk:
       chunk_store->put(chunk_id, pdev_s3->drain_dirty_cache(chunk_id))
  b. WAL chunks: flush to S3
  c. B+tree chunks: flush to S3
  d. MetaBlk chunks: flush to S3 (includes CP superblock with bitmap)
  e. pdev_s3 superblock: update with current chunk S3 keys + generation (commit point)
```

**pdev_s3 superblock is the S3 commit point.** Written last, after all chunk data and MetaBlk are uploaded. If crash mid-CP, pdev_s3 superblock for that generation was never written, so recovery falls back to previous generation.

**Key decisions:**
- S3 upload is *after* NVMe flush — NVMe is already durable before we touch S3
- If S3 upload fails, the system continues normally on NVMe. S3 is best-effort until it catches up.
- Volume index update is the commit point — only written after ALL dirty chunks are uploaded
- Parallelism: upload multiple chunks concurrently (configurable concurrency limit)
- *Callers pass `dirty_blocks` to `put()` even though v1 ignores them* — ensures calling code is v3-ready

**Acceptance criteria:**
- After a CP, all dirty chunks are on S3 via `chunk_store->put()`
- Chunk superblocks and volume index updated
- Bitmap uploaded for each dirty chunk
- Failure of S3 upload does not affect NVMe operation
- Chunks that failed upload are retried at next CP

**Dependencies:** Task 1.2 (pdev_s3), Task 1.3 (ChunkStore), Task 1.4 (superblocks), CPManager hook point

---

### Task 1.6: Configuration

**Goal:** Add configuration options for the S3 layer.

**What to build:**
- Config entries:
  - `s3.enabled: bool` (default: false) — master switch
  - `s3.bucket_prefix: string` — bucket name prefix (default: `homestore`). Full bucket = `<prefix>-<cluster_id>`
  - `s3.cluster_id: string` — cluster identifier for bucket naming
  - `s3.region: string` — AWS region
  - `s3.endpoint: string` (optional) — for S3-compatible stores (MinIO, etc.)
  - `s3.upload_concurrency: int` (default: 4) — max parallel chunk uploads per CP
  - `s3.retry_count: int` (default: 3) — retries per S3 operation
  - `s3.retry_backoff_ms: int` (default: 1000) — initial backoff
  - `s3.chunk_store_backend: string` (default: `full`) — `full` (v1) or `sst` (v3)
  - `s3.dirty_cache_max_mb: int` (default: 4096) — max dirty cache size before forcing early CP
- Loaded at startup, immutable during operation

**Acceptance criteria:**
- S3 layer is completely disabled when `s3.enabled = false` (zero overhead)
- All config values validated at startup
- Works with real AWS S3 and S3-compatible stores (MinIO for testing)

**Dependencies:** None.

---

## Phase 2: Tiered Reads + Recovery

The goal of Phase 2 is to enable tiered reads (NVMe → S3 fallback) and recovery from S3 when NVMe is lost.

---

### Task 2.1: Tiered Read Path

**Goal:** Implement the read path that checks NVMe first and transparently falls back to S3 if the chunk is not present on NVMe.

**What to build:**
- Modify VirtualDev read path:
  ```
  read(blkid):
    chunk = resolve_chunk(blkid)
    if chunk.on_nvme:
      return nvme_pdev.read(chunk, offset)    // fast path
    elif chunk.on_s3:
      return s3_pdev.read(chunk, offset)      // slow path via ChunkStore::get()
    else:
      error: chunk not available on any device
  ```
- S3 reads go through pdev_s3 → ChunkStore::get()
- Latency tracking: log when reads fall through to S3 (performance monitoring)

**Key decisions:**
- NVMe is always preferred when available (hot path unchanged)
- S3 fallback is transparent to callers (DataSvc, IndexSvc don't know about tiers)
- No automatic hydration in this task — that's Phase 3

**Acceptance criteria:**
- Reads from chunks on NVMe: same latency as before
- Reads from chunks only on S3: return correct data (higher latency is expected)
- Callers (DataSvc, IndexSvc) don't need any changes
- Metrics: track NVMe-hit vs S3-fallback ratio

**Dependencies:** Task 1.2 (pdev_s3), Task 1.3 (ChunkStore)

---

### Task 2.2: Recovery — Bulk Download from S3

**Goal:** On startup, if NVMe is empty/wiped, download all chunks from S3 to restore local state (Option B — simpler, for v1).

**What to build:**
- Detection: on startup, check if NVMe has valid HomeStore data
  - If yes: normal startup (Phase 1 behavior)
  - If no: trigger S3 recovery
- S3 recovery flow:
  1. Read volume index from S3 → discover all chunk_ids
  2. Download essential chunks (identified by chunk_type in pdev_s3 superblock):
     - MetaBlk chunks (CP superblock with bitmap, service state)
     - WAL chunks (log replay)
     - B+tree / index chunks (index nodes)
     These are all small — fast to download.
  3. For each data chunk:
     - Download data via `chunk_store->recover(chunk_id)` using S3 key from pdev_s3 superblock
     - Write data to NVMe
     - Set chunk: on_nvme=true, on_s3=true
  4. HomeStore initializes from MetaBlk + WAL + B+tree (same as normal NVMe boot)
  5. Replay binlog for post-CP mutations

**Key decisions:**
- Download chunks in parallel (configurable concurrency)
- Verify checksums on downloaded data
- Progress logging (this could take minutes for large datasets)
- If S3 download fails for a chunk: retry with backoff, then fail startup if persistent

**Acceptance criteria:**
- A node with wiped NVMe can fully recover from S3
- Recovered state matches the last checkpoint that was uploaded to S3
- Bitmap correctly restored → BlkAllocator state is valid
- Recovery time < 10 minutes for 100GB of data

**Dependencies:** Phase 1 complete (S3 has valid data)

---

### Task 2.3: NVMe Superblock Fallback + Ping-Pong

**Goal:** If the NVMe superblock is corrupted, fall back to S3 state. Also implement ping-pong superblock on NVMe.

**What to build:**
- NVMe ping-pong superblock:
  - Two fixed NVMe locations (slot A, slot B)
  - On each CP: write to the inactive slot, then mark it as active
  - On startup: read both slots, use the one with higher generation
  - If one slot is corrupted: use the other
- S3 fallback:
  - If both NVMe slots corrupted: read volume index + chunk superblocks from S3
  - Reconstruct NVMe superblock from S3 state
  - Continue with normal startup (chunks are still on NVMe)

**Acceptance criteria:**
- Crash during superblock write → other slot is still valid
- Both slots corrupted → recovered from S3 automatically
- Logged as a warning/alert

**Dependencies:** Task 1.4 (S3 superblocks)

---

## Phase 3: Dynamic Chunk Lifecycle

The goal of Phase 3 is to enable dynamic chunk placement — eviction from NVMe and on-demand hydration from S3.

---

### Task 3.1: Chunk Eviction

**Goal:** Remove cold chunks from NVMe to free space, while keeping them on S3.

**What to build:**
- `evict_chunk(chunk_id)` method:
  1. Verify `chunk.on_s3 == true` (must be durable on S3)
  2. Ensure any pending dirty data is flushed to S3
  3. Remove chunk from NVMe pdev: free NVMe space
  4. Update chunk state: `on_nvme = false`, `nvme_pdev = null`
  5. Bitmap: unchanged (stays in memory / MetaSvc, backed by S3 copy)
- Block new NVMe writes to the chunk during eviction
- After eviction, new writes to this chunk's blocks trigger hydration first

**Key decisions:**
- Eviction requires S3 durability as precondition (never evict a chunk that isn't on S3)
- Eviction is a background operation (not on the hot write path)
- Bitmap stays valid — only the data location changes
- Write to an evicted chunk: must hydrate first (or allocate on a different chunk)

**Acceptance criteria:**
- Evicted chunks free NVMe space
- Reads to evicted chunks go through S3 (tiered read path)
- Bitmap unchanged after eviction
- Cannot evict a chunk that's not on S3

**Dependencies:** Task 2.1 (tiered reads must work before eviction makes sense)

---

### Task 3.2: Chunk Hydration (Cache-on-Read)

**Goal:** When a read hits a chunk that's only on S3, optionally bring it back to NVMe.

**What to build:**
- Hydration trigger: on S3 read fallback, optionally hydrate the whole chunk
- `hydrate_chunk(chunk_id)`:
  1. Allocate NVMe space for the chunk
  2. Download chunk data from S3 via `chunk_store->recover(chunk_id)`
  3. Write to NVMe
  4. Update chunk state: `on_nvme = true`
  5. Future reads go through NVMe (fast path)
- Configuration: `s3.hydrate_on_read: bool` (default: true)
- Hydration is async — the initial read returns from S3, hydration happens in background

**Key decisions:**
- Hydration is optional (configurable)
- The triggering read returns immediately from S3 (don't block on hydration)
- Hydration is at chunk granularity (not individual blocks)
- If NVMe is full, hydration may trigger eviction of another cold chunk (LRU)

**Acceptance criteria:**
- After hydration, reads to the chunk go through NVMe
- Hydration doesn't block the triggering read
- Correctly handles concurrent reads during hydration
- NVMe space management handles full-NVMe scenarios

**Dependencies:** Task 3.1 (eviction), Task 2.1 (tiered reads)

---

### Task 3.3: On-Demand Recovery (Option A)

**Goal:** Fast startup when NVMe is lost — start serving reads immediately, hydrate on demand.

**What to build:**
- Modified recovery flow (replacing bulk download for production):
  1. Download pdev_s3 superblock → chunk list (types, S3 keys)
  2. Download essential chunks: MetaBlk + WAL + B+tree / index
     → CP superblock with bitmap, service state, logs, index nodes
  3. HomeStore initializes from MetaBlk + WAL + B+tree
  4. Mark data chunks only: `on_nvme = false`, `on_s3 = true`
  5. Start serving reads → first access triggers hydration
  6. Background: proactively hydrate frequently-accessed chunks
- Proactive hydration policy: background thread hydrates chunks based on access patterns

**Key decisions:**
- Only bitmaps are downloaded eagerly (small, needed for allocator state)
- Data is hydrated on demand (first read triggers it)
- Background hydration runs at low priority to not compete with user reads

**Acceptance criteria:**
- Node starts serving reads within seconds of recovery starting
- First read to any chunk works (from S3) even if not yet hydrated
- Background hydration progressively warms NVMe
- All chunks eventually hydrated (given enough NVMe capacity)

**Dependencies:** Task 3.1, 3.2

---

### Task 3.4: Eviction Policy

**Goal:** Decide which chunks to evict when NVMe is full.

**What to build:**
- Eviction policy engine:
  - Track per-chunk access recency / frequency
  - When NVMe capacity threshold is hit, select coldest chunks for eviction
  - Configurable: `s3.nvme_capacity_threshold: float` (default: 0.9 = evict when 90% full)
  - Configurable: `s3.eviction_policy: string` (default: `lru`)
- Integration with chunk hydration: hydrating a chunk may trigger eviction of another

**Acceptance criteria:**
- NVMe capacity stays within configured threshold
- Cold chunks evicted before hot chunks
- Eviction + hydration churn is bounded (no thrashing)

**Dependencies:** Task 3.1, 3.2

---

## Phase 4: Snapshots

The goal of Phase 4 is to enable zero-cost snapshots backed by pinned S3 objects.

---

### Task 4.1: Snapshot Creation

**Goal:** Create a snapshot by pinning current S3 objects and recording state.

**What to build:**
- `create_snapshot(snap_id)`:
  1. Trigger a checkpoint (ensure S3 is fully up to date)
  2. Record snapshot in pdev_s3 superblock:
     `{ snap_id, generation, btree_root_blkid, chunk_snapshot_keys: [{chunk_id, s3_key}, ...] }`
  3. Switch S3 naming mode: future CPs write to new keys (`<chunk_id>_gen<N>.dat`)

**Acceptance criteria:**
- Snapshot created in < 1 second (excluding CP flush time)
- After snapshot, new CPs write to new S3 keys
- Snapshot correctly recorded in pdev_s3 superblock

**Dependencies:** Phase 1 complete

---

### Task 4.2: Snapshot Read

**Goal:** Read data from a snapshot by traversing the snapshot's B+tree on S3.

**What to build:**
- `snapshot_read(snap_id, LBA)`:
  1. Look up snapshot in pdev_s3 superblock → get btree_root_blkid + chunk_snapshot_keys
  2. Traverse B+tree from S3 (read nodes using snapshot's pinned S3 keys)
  3. Resolve data BlkId → chunk_id + offset
  4. Read data from S3 using snapshot's chunk_snapshot_keys
- Multiple S3 GETs per read (one per B+tree level + data)

**Acceptance criteria:**
- Can read any LBA from any snapshot
- Returns correct data (matches what was written before the snapshot)
- Handles missing S3 objects gracefully (error, not crash)

**Dependencies:** Task 4.1

---

### Task 4.3: S3 GC — Intermediate CP Objects

**Goal:** Clean up S3 objects from intermediate CPs that aren't pinned by any snapshot.

**What to build:**
- After each CP commit (when snapshots exist):
  - The previous CP's S3 objects are now superseded
  - If no snapshot pins them: delete from S3
  - If a snapshot pins them: keep
- GC runs asynchronously after CP commit (not blocking)

**Acceptance criteria:**
- Intermediate S3 objects cleaned up within one CP cycle
- Snapshot-pinned objects never deleted
- S3 storage stays close to (live data + snapshot data)

**Dependencies:** Task 4.1

---

### Task 4.4: Snapshot Deletion

**Goal:** Delete a snapshot and clean up its S3 objects.

**What to build:**
- `delete_snapshot(snap_id)`:
  1. Get snapshot's chunk_snapshot_keys from pdev_s3 superblock
  2. For each S3 key: if no other snapshot references it, delete from S3
  3. Remove snapshot from pdev_s3 superblock
  4. If no snapshots remain: switch back to overwrite mode

**Acceptance criteria:**
- Snapshot deleted from all metadata
- Unreferenced S3 objects cleaned up
- System correctly switches back to overwrite mode when no snapshots remain

**Dependencies:** Task 4.1, 4.3

---

## Phase 5: SST/LSM Per Chunk (v3 Target)

The goal of Phase 5 is to swap the chunk storage format from full-chunk blobs to per-chunk LSM with SST files. The `ChunkStore` interface is unchanged — only the implementation is swapped.

---

### Task 5.1: SST File Format

**Goal:** Define and implement the SST file format for chunk data on S3.

**What to build:**
- SST file binary format:
  ```
  SST File {
    header: { magic, version, chunk_id, cp_generation, num_entries, checksum }
    data_blocks: [
      { offset: u64, data: [u8; block_size] },
      ...  // sorted by offset
    ]
    index: [ { offset: u64, file_position: u64 } ]  // for binary search
    footer: { index_offset, num_entries, checksum }
  }
  ```
- Writer: takes `vector<DirtyBlock>` (sorted by offset), produces SST file
- Reader: given SST file + target offset, binary-searches index, returns data block
- Supports S3 range reads (read just the footer/index without downloading the whole file)

**Acceptance criteria:**
- Round-trip: write SST with N blocks → read any block by offset → correct data
- Binary search finds blocks in O(log N)
- Works with S3 range reads

**Dependencies:** None (standalone format library). Can start in parallel with Phase 1.

---

### Task 5.2: Chunk Manifest

**Goal:** Track the set of SST files that make up a chunk's state on S3.

**What to build:**
- Per-chunk manifest at `chunks/<chunk_id>/manifest.json`
- Read/write manifest to S3
- Manifest update is the commit point for each chunk's CP upload

**Dependencies:** Task 5.1

---

### Task 5.3: SSTChunkStore Implementation

**Goal:** Implement the `ChunkStore` interface using SST files.

**What to build:**
- `SSTChunkStore : public ChunkStore`
- `put()`: sort dirty_blocks → write new SST → upload → update manifest
- `get()`: read manifest → search SSTs newest-first → return data
- `compact()`: merge all SSTs → write compacted SST → delete old SSTs
- `recover()`: download manifest + SSTs → merge-read → return data for NVMe restore
- `describe()`: return manifest with SST list

**Acceptance criteria:**
- All `ChunkStore` interface tests pass with `SSTChunkStore` (same tests as `FullChunkStore`)
- Multiple `put()` calls → `get()` returns latest data
- `compact()` reduces SST count to 1 per chunk
- `recover()` correctly merges all SSTs

**Dependencies:** Task 5.1, 5.2, Task 1.1 (S3ObjectStore)

---

### Task 5.4: Compaction Scheduler

**Goal:** Run per-chunk compaction in background to keep SST file counts bounded.

**What to build:**
- Background thread: periodically checks SST counts per chunk
- Trigger compaction when SST count > threshold (configurable, default 10)
- Rate-limited to avoid S3 bandwidth spikes
- Compaction doesn't block CPs or reads

**Dependencies:** Task 5.3

---

### Task 5.5: v1 → v3 Swap

**Goal:** Replace `FullChunkStore` with `SSTChunkStore`.

**What to build:**
- Configuration switch: `s3.chunk_store_backend: full | sst`
- Migration: `SSTChunkStore` can read legacy full-chunk objects as fallback (backward compatible)
- All callers unchanged — they use `ChunkStore` interface

**Acceptance criteria:**
- Switching backend config from `full` → `sst` works without data loss
- All Phase 1-4 functionality works identically with either backend

**Dependencies:** Task 5.3, Phase 1-4 complete

---

## Phase 6: Writable Clones

The goal of Phase 6 is to enable fast replica provisioning by cloning from S3.

---

### Task 6.1: Clone Creation

**Goal:** Create a new volume that starts from S3 state with its own prefix.

**What to build:**
- `clone_volume(source_volume_id, at_snap_id)`:
  1. Source must have a snapshot
  2. Copy S3 objects from source prefix to clone prefix (same bucket, S3 CopyObject)
  3. Clone gets its own volume index and chunk superblocks
  4. NVMe: hydrate on demand or bulk download
  5. Clone is writable immediately, future CPs write under clone's prefix

**Acceptance criteria:**
- Clone contains all data from the snapshot point
- Clone is writable after creation
- Clone's writes don't affect source, and vice versa

**Dependencies:** Phase 4 complete

---

### Task 6.2: Volume Lifecycle Management

**Goal:** Handle volume-level S3 lifecycle (creation, deletion).

**What to build:**
- Volume deletion = delete all S3 objects under `<volume_id>/` prefix
- Snapshot refcounting is volume-local (within volume's chunk superblocks)
- Bucket created at cluster provisioning, not per volume

**Acceptance criteria:**
- Volume deletion cleanly removes all S3 data
- No orphaned S3 objects

**Dependencies:** Task 6.1

---

## Task Dependency Graph

```
Phase 1 (S3 pdev + v1 Full Chunk):
  1.6 Config ──────────────────────────────────────┐
  1.1 S3ObjectStore ───────────────────────────────┤
                                                    ├─► 1.2 pdev_s3 ──────────────┐
  1.3 ChunkStore + FullChunkStore ◄── 1.1 ────────┤                               │
  1.4 Chunk Superblock + Vol Index ◄── 1.1 ────────┤                               │
                                                    │                               ▼
                                                    │                         1.5 CP Hook
                                                    │                             │
Phase 2 (Tiered Reads + Recovery):                  │                             ▼
  2.1 Tiered Read Path ◄── 1.2, 1.3                │                     Phase 1 complete
  2.2 Bulk Recovery ◄── Phase 1                     │                             │
  2.3 Superblock Fallback ◄── 1.4                  │                             │
                                                    │                             ▼
Phase 3 (Dynamic Chunk Lifecycle):                  │                     Phase 2 complete
  3.1 Chunk Eviction ◄── 2.1                        │                             │
  3.2 Chunk Hydration ◄── 3.1                       │                             │
  3.3 On-Demand Recovery ◄── 3.1, 3.2              │                             │
  3.4 Eviction Policy ◄── 3.1, 3.2                 │                             │
                                                    │                             ▼
Phase 4 (Snapshots):                                │                     Phase 3 complete
  4.1 Snapshot Creation ◄── Phase 1                 │                             │
  4.2 Snapshot Read ◄── 4.1                         │                             │
  4.3 S3 GC ◄── 4.1                                │                             │
  4.4 Snapshot Deletion ◄── 4.1, 4.3               │                             │
                                                    │                             ▼
Phase 5 (SST/LSM — v3):                            │                     Phase 4 complete
  5.1 SST File Format (standalone — can start anytime!)                           │
  5.2 Chunk Manifest ◄── 5.1                        │                             │
  5.3 SSTChunkStore ◄── 5.1, 5.2, 1.1              │                             │
  5.4 Compaction Scheduler ◄── 5.3                  │                             │
  5.5 v1→v3 Swap ◄── 5.3, Phase 1-4                │                     Phase 5 complete
                                                                                   │
Phase 6 (Clones):                                                                  ▼
  6.1 Clone Creation ◄── Phase 4                                          Phase 6 complete
  6.2 Volume Lifecycle ◄── 6.1
```

## Parallelization Opportunities

Tasks that can be worked on simultaneously:
- **Task 1.1 + 1.6** — all independent
- **Task 1.3 + 1.4** — both depend on 1.1 only
- **Task 5.1 (SST format)** — standalone, can start in parallel with ANY phase
- **Task 2.1 + 2.3** — both depend on Phase 1 but not on each other
- **Task 3.1 + 3.4** — eviction policy can be designed while eviction is implemented
- **Task 4.2 + 4.3** — both depend on 4.1 but not on each other

---

## Estimated Complexity

| Task | Complexity | Notes |
|------|-----------|-------|
| 1.1 S3ObjectStore | Medium | Standard SDK wrapper, async + retry + range reads |
| 1.2 pdev_s3 | High | Core integration — new pdev type in DeviceManager |
| 1.3 ChunkStore + FullChunkStore | Medium | Abstract interface + v1 impl |
| 1.4 Chunk Superblock + Vol Index | Low-Medium | Data structures + serialize |
| 1.5 CP Hook | High | Core integration point. Must not break existing CP. |
| 1.6 Config | Low | Standard config entries |
| 2.1 Tiered Read Path | Medium-High | VirtualDev read path modification |
| 2.2 Bulk Recovery | Medium-High | Full state restoration via ChunkStore::recover |
| 2.3 Superblock Fallback + Ping-Pong | Medium | Atomic write guarantees + fallback logic |
| 3.1 Chunk Eviction | Medium | Dynamic placement change, must be safe |
| 3.2 Chunk Hydration | Medium-High | Async hydration, concurrency handling |
| 3.3 On-Demand Recovery | Medium | Modified startup flow, bitmap-only eager download |
| 3.4 Eviction Policy | Medium | Access tracking, LRU implementation |
| 4.1 Snapshot Creation | Medium | Pin S3 objects, update metadata |
| 4.2 Snapshot Read | Medium-High | B+tree traversal over S3 |
| 4.3 S3 GC | Medium | Reference tracking across snapshots |
| 4.4 Snapshot Deletion | Medium | GC + mode switch back |
| 5.1 SST File Format | Medium | Binary format, writer, reader, index |
| 5.2 Chunk Manifest | Low-Medium | JSON manifest for SST tracking |
| 5.3 SSTChunkStore | High | Full LSM implementation behind ChunkStore |
| 5.4 Compaction Scheduler | Medium | Background task, threshold-based |
| 5.5 v1→v3 Swap | Medium | Config switch + backward compat |
| 6.1 Clone Creation | High | Cross-volume S3 copy, NVMe hydration |
| 6.2 Volume Lifecycle | Medium | Prefix deletion, refcounting |
