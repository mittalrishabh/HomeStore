# Design Document: Making HomeStore S3-Native

**Author:** Architect Agent + Rishabh Mittal
**Date:** 2026-04-05
**Status:** Draft v5
**Reviewers:** Rishabh Mittal

---

## 1. Overview

### 1.1 Problem Statement

HomeStore currently stores all data on local NVMe devices. Every block — data, index nodes, WAL entries — lives on directly-attached storage. This creates a hard coupling between storage capacity and physical hardware.

To achieve cloud-native elasticity and cost efficiency, we need HomeStore to use S3 as the durable store, while keeping local NVMe as the mutable hot tier.

### 1.2 Core Design Principles

1. **S3 is a native device, not a bolt-on.** S3 is modeled as a pdev in HomeStore's device hierarchy, using vdev's existing mirrored chunk mechanism.
2. **NVMe is the mutable fast tier.** NVMe stays fully mutable, in-place — identical to current HomeStore.
3. **Dynamic chunk placement.** Chunks can live on NVMe, S3, or both. Placement changes at runtime (eviction, hydration, recovery).
4. **Single bitmap per chunk.** One BlkAllocator bitmap shared across NVMe and S3 — allocation state is device-independent.
5. **CoW on S3 only.** When snapshots exist, S3 writes fork to new objects. NVMe doesn't care.

### 1.3 Goals

- **S3 as native pdev** — S3 participates in vdev's mirrored chunk model, not as a separate sync layer.
- **Tiered reads** — Reads go to NVMe if chunk is present, transparently fall back to S3 if not.
- **Dynamic chunk lifecycle** — Chunks can be created on, evicted from, or hydrated to NVMe at any time.
- **Crash recovery from S3** — A node can recover from S3 if NVMe is lost (on-demand hydration).
- **Zero-cost snapshots** — Snapshot = pin current S3 objects. No data copy, no NVMe impact.
- **Single shared bitmap** — BlkAllocator bitmap is per-chunk and device-independent.

### 1.4 Non-Goals

- CoW on NVMe (NVMe is always the single mutable copy).
- Replacing binlog-based replication (orthogonal to S3 tiering).
- Changing the HomeBlocks volume API surface.
- Symmetric mirroring (S3 writes are async/batched at CP, not synchronous like NVMe).

---

## 2. Architecture

### 2.1 Key Concept: S3 as a Native pdev with Mirrored Chunks

Instead of bolting S3 on as a separate sync layer, S3 is modeled as a *physical device (pdev)* in HomeStore's device hierarchy. VirtualDev's existing mirrored chunk mechanism handles the write fanout.

```
┌─────────────────────────────────────────┐
│              HomeBlocks                   │
│  (Volume abstraction: LBA → BlkId)       │
├──────────────┬──────────────────────────┤
│  IndexSvc    │   Replication (Binlog)    │
│  (B+Tree)    │   (binlog-based)          │
├──────────────┼──────────────────────────┤
│  MetaSvc     │   DataSvc / LogSvc        │
│  (K/V)       │   (Block alloc/IO)        │
├──────────────┴──────────────────────────┤
│           CPManager                       │
│     (Checkpoint + crash recovery)         │
├──────────────────────────────────────────┤
│          DeviceManager                    │
│   VirtualDev (mirrored chunks)            │
│     ┌────────────┐  ┌───────────────┐    │
│     │ pdev_nvme  │  │  pdev_s3 (NEW)│    │
│     │ (local,    │  │  (remote,     │    │
│     │  fast,     │  │   durable,    │    │
│     │  mutable)  │  │   append-only)│    │
│     └────────────┘  └───────────────┘    │
└──────────────────────────────────────────┘
```

### 2.2 Chunk Placement Model

Chunk-to-pdev mappings are *dynamic*, not static. A chunk can live on NVMe, S3, or both.

```
Chunk {
  chunk_id: u64
  bitmap: BlkAllocator          // SINGLE bitmap — shared across NVMe and S3

  // Dynamic placement (can change at runtime):
  on_nvme: bool                 // currently materialized on NVMe pdev
  on_s3: bool                   // currently durable on S3 pdev

  nvme_pdev: pdev*              // nullable — chunk may not be on NVMe
  s3_pdev: pdev*                // nullable — chunk may not be on S3 yet
}
```

*Single bitmap per chunk:* The BlkAllocator bitmap tracks which blocks are allocated/free regardless of which device(s) the data lives on. The bitmap is a logical allocation map, not tied to a specific pdev.

### 2.3 Chunk Lifecycle

```
New chunk:
  Create on NVMe + allocate bitmap → write data → CP flushes to S3
  State: on_nvme=true, on_s3=true

Eviction (NVMe full, chunk cold):
  Verify chunk is on S3 → remove from NVMe pdev → free NVMe space
  State: on_nvme=false, on_s3=true
  Bitmap: unchanged

Hydration (read miss on evicted chunk):
  Fetch from S3 → create on NVMe pdev → cache locally
  State: on_nvme=true, on_s3=true
  Bitmap: unchanged

Recovery (NVMe lost):
  All chunks: on_nvme=false, on_s3=true
  Hydrate on demand (or bulk download)
```

### 2.4 Asymmetric Mirroring

This is NOT a standard symmetric mirror. The two pdevs have different characteristics:

| Property | pdev_nvme | pdev_s3 |
|----------|-----------|---------|
| Writes | Synchronous, in-place | Async, batched at CP |
| Reads | Primary (fast) | Fallback (on cache miss) |
| Mutability | Fully mutable | Append-only (v3: SST) |
| Presence | Optional (chunk may be evicted) | Always (durable store) |
| Capacity | Limited (physical NVMe) | Unlimited (elastic S3) |

---

## 3. How It Works

### 3.1 Write Path

Writes go to both NVMe and pdev_s3 as a mirrored write. pdev_s3 caches dirty blocks in memory. The write is not acknowledged to the upper layer until both pdevs confirm.

```
Volume.write(LBA, data)
    │
    ▼
DataSvc.alloc_blocks()              ← BlkAllocator, single shared bitmap
    │
    ▼
VirtualDev.write(BlkId, data)       ← mirrored write to both pdevs:
    │
    ├─► nvme_pdev.write(chunk, offset, data)   ← sync, in-place, fast
    │
    └─► s3_pdev.write(chunk, offset, data)     ← cache in dirty_cache
    │       dirty_cache[chunk_id].append(           (shared_ptr bump, ~instant)
    │         DirtyBlock{offset, data})
    │
    ├─► await BOTH completions before replying to upper layer
    │
    ▼
IndexSvc.update(LBA → BlkId)       ← B+tree in-place update
    │
    ▼
Replication.replicate()             ← binlog shipped to peers
```

**Mirror write semantics:** The mirrored write waits for both pdevs to confirm before acknowledging to the upper layer. Since pdev_s3.write() is just a dirty cache insert (sisl::byte_array shared_ptr bump), it's effectively instant — no added latency. But the contract is correct: both mirrors have accepted the write.

**Dirty cache:** pdev_s3 maintains an in-memory cache of dirty blocks per chunk (`map<chunk_id, vector<DirtyBlock>>`). Each write stashes a `sisl::byte_array` reference (shared_ptr bump, not a data copy). At CP time, the dirty cache is drained into `chunk_store->put()`. This avoids re-reading from NVMe at CP time, and gives v3 (SST) the exact dirty blocks to write.

**Memory bound:** If the dirty cache exceeds a configurable threshold (e.g., `s3.dirty_cache_max_mb: 4096`), force an early CP flush to drain it. This prevents unbounded memory growth between CPs.

### 3.2 Read Path (Tiered)

Reads check NVMe first, fall back to S3 if chunk is not present on NVMe.

```
Volume.read(LBA)
    │
    ▼
IndexSvc.lookup(LBA) → BlkId
    │
    ▼
chunk = resolve_chunk(BlkId)
    │
    ├── chunk.on_nvme? ── YES ──► nvme_pdev.read(offset) ──► return data  (fast)
    │
    └── chunk.on_nvme? ── NO  ──► s3_pdev.read(offset)   ──► return data  (slow)
                                     │
                                     └──► optionally: hydrate chunk to NVMe (cache-on-read)
```

### 3.3 Checkpoint (S3 Sync)

At CP time, dirty chunks are flushed to the S3 pdev. The flush order on S3 must match the NVMe CP ordering. The CP superblock is flushed directly (not cached) as it is the commit point.

```
CPManager triggers checkpoint
    │
    ▼
1. Normal NVMe checkpoint (unchanged):
   - Data blocks flushed
   - WAL flushed
   - B+tree nodes flushed (wb_cache)
   - BlkAllocator bitmap persisted (via MetaSvc)
   - CP superblock written last (NVMe commit point)
    │
    ▼
2. S3 pdev sync (matching NVMe flush order):
   a. Data chunks:
      For each dirty data chunk:
        chunk_store->put(chunk_id, pdev_s3.drain_dirty_cache(chunk_id))
   b. WAL chunks:
      Flush WAL data to S3
   c. B+tree chunks:
      Flush dirty B+tree nodes to S3
   d. MetaBlk chunks:
      Flush MetaBlk to S3 (includes CP superblock with bitmap)
   e. pdev_s3 superblock:
      Update with current chunk S3 keys + generation
      This is the S3 commit point (written last).
    │
    ▼
3. CP complete — S3 state is consistent at this generation
```

**pdev_s3 superblock is the S3 commit point.** Written last, after all chunk data and MetaBlk are uploaded. On S3 recovery, if pdev_s3 superblock shows generation N, all chunk data for generation N is guaranteed durable on S3.

**Flush ordering matters for recovery correctness:** If we crash mid-CP, the pdev_s3 superblock for that generation was never written, so recovery falls back to the previous generation where everything is consistent.

### 3.4 Snapshot Creation

**Cost:** Flush current state to S3 + pin S3 objects. No NVMe changes.

```
create_snapshot(snap_id):
    │
    ▼
1. Trigger a checkpoint (ensure S3 is fully up to date)
    │
    ▼
2. Pin current S3 objects:
   For each chunk:
     Record: "snapshot S1 references s3://chunks/7_gen42.dat"
    │
    ▼
3. Record snapshot in pdev_s3 superblock:
   snapshot S1 = {
     generation: 42,
     btree_root_blkid: current root,
     chunk_snapshot_keys: [{chunk_id, s3_key}, ...]
   }
    │
    ▼
4. Future CPs write to NEW S3 objects:
   chunk_7 → s3://chunks/7_gen43.dat (new key)
   Old S3 object preserved for snapshot
    │
    ▼
5. NVMe: nothing changes. Still mutable. Still in-place.
```

### 3.5 Operation With Snapshots Active

**NVMe:** Completely unchanged. In-place writes, in-place B+tree updates. NVMe doesn't know snapshots exist.

**S3:** CoW at the chunk level. Each CP writes *new* S3 objects (new keys) instead of overwriting the old ones. The old S3 objects are pinned by the snapshot.

```
Example timeline:

Gen 42: Snapshot S1 taken
  NVMe chunk 7: [A][B][C][D]           ← mutable, live
  S3 snapshot:  s3://chunk_7_gen42.dat  ← [A][B][C][D] (pinned, immutable)

Gen 43: Overwrite B → B', C → C' (normal in-place on NVMe)
  NVMe chunk 7: [A][B'][C'][D]         ← updated in-place
  S3 at CP 43:  s3://chunk_7_gen43.dat  ← [A][B'][C'][D] (new object)
  S3 snapshot:  s3://chunk_7_gen42.dat  ← [A][B][C][D] (still there)

Gen 44: Overwrite A → A'
  NVMe chunk 7: [A'][B'][C'][D]        ← updated in-place
  S3 at CP 44:  s3://chunk_7_gen44.dat  ← [A'][B'][C'][D] (new object)
  S3 gen 43:    s3://chunk_7_gen43.dat  ← can be deleted (no snapshot pins it)
  S3 snapshot:  s3://chunk_7_gen42.dat  ← still pinned by S1
```

### 3.6 Snapshot Read

Snapshot reads go to S3 using the snapshot's pinned S3 keys from pdev_s3 superblock.

```
snapshot_read(snap_id, LBA):
  snap = pdev_s3_superblock.snapshots[snap_id]
  blkid = btree_lookup_from_s3(snap.btree_root_blkid, LBA)
  chunk_id, offset = decode(blkid)
  s3_key = snap.chunk_snapshot_keys[chunk_id].s3_key
  return read_from_s3(s3_key, offset)
```

### 3.7 Snapshot Deletion

```
delete_snapshot(snap_id):
  snap = pdev_s3_superblock.snapshots[snap_id]
  For each s3_key in snap.chunk_snapshot_keys:
    If no other snapshot references that S3 key:
      delete from S3

  Remove snapshot from pdev_s3 superblock

  If no snapshots remain:
    CPs go back to overwriting same S3 objects (no more CoW)
```

### 3.8 Chunk Eviction

When NVMe is full, cold chunks can be evicted to free space.

```
evict_chunk(chunk_id):
  Precondition: chunk.on_s3 == true (must be durable on S3 first)
    │
    ▼
1. Mark chunk as evicting (block new writes to this chunk)
    │
    ▼
2. Ensure any pending dirty data is flushed to S3 (trigger mini-CP if needed)
    │
    ▼
3. Remove chunk from NVMe pdev:
   - Free NVMe space
   - chunk.on_nvme = false
   - chunk.nvme_pdev = null
    │
    ▼
4. Bitmap: unchanged — allocation state preserved
   Future reads to this chunk go through S3 path
```

### 3.9 Chunk Hydration

When a read hits a chunk that's only on S3, optionally hydrate it back to NVMe.

```
hydrate_chunk(chunk_id):
    │
    ▼
1. Allocate space on NVMe pdev for this chunk
    │
    ▼
2. Download chunk data from S3 via chunk_store->recover(chunk_id)
    │
    ▼
3. Write to NVMe pdev
    │
    ▼
4. chunk.on_nvme = true
   chunk.nvme_pdev = <new allocation>
   Bitmap: unchanged — same logical state
    │
    ▼
5. Future reads go through NVMe (fast path)
```

---

## 4. Superblock Architecture

S3 metadata is consolidated into two objects: the pdev_s3 superblock (chunk layout + snapshots) and the MetaBlk chunks (which include the CP superblock with bitmap). No per-chunk superblocks or volume index needed.

### 4.1 pdev_s3 Superblock

Mirrors the pdev_nvme superblock structure, plus S3-specific state and snapshots. This is the *single entry point* for S3 recovery.

```
pdev_s3 Superblock {
  pdev_id: u64
  generation: u64                     // monotonic, incremented per CP

  // Chunk layout (mirrors pdev_nvme superblock):
  chunks: [
    { chunk_id: u64,
      chunk_size: u64,
      chunk_type: enum,               // data, index, WAL, metablk
      s3_key: string,                 // current S3 object for this chunk
      vdev_id: u64 }
  ]

  // Snapshot state:
  snapshots: [
    { snap_id: u64,
      generation: u64,
      chunk_snapshot_keys: [
        { chunk_id: u64, s3_key: string }
      ],
      btree_root_blkid: BlkId }
  ]

  checksum: u64
}
```

**S3 Location:**
```
s3://homestore-<cluster_id>/<volume_id>/pdev_superblock.bin
```

Updated at the end of each CP (after all chunk data is uploaded). This is the S3 commit point.

### 4.2 MetaBlk Chunks (on S3)

MetaBlk chunks contain HomeStore's internal metadata, including:
- CP superblock (with BlkAllocator bitmap)
- Service superblocks (IndexSvc, DataSvc, LogSvc, etc.)
- All MetaSvc entries

These are uploaded to S3 as regular chunks (identified by `chunk_type: metablk` in the pdev_s3 superblock). On recovery, they are the *first* chunks downloaded — HomeStore needs them to initialize.

### 4.3 NVMe Superblock (Unchanged)

Existing HomeStore state on NVMe:
- pdev_nvme superblock: chunk → NVMe device/offset mapping
- CP superblock (in MetaBlk): BlkAllocator bitmap, CP state
- B+tree state, WAL state
- Ping-pong (double-buffer) for crash safety

### 4.4 Separation of Concerns

```
S3 side (pdev_s3 superblock):        NVMe side (pdev_nvme superblock):
────────────────────────────────  ────────────────────────────────────
- chunk_id → S3 key                  - chunk_id → NVMe device + offset
- chunk types (data/index/WAL/meta)  - BlkAllocator bitmap (in MetaBlk)
- Snapshot state + pinned keys       - B+tree nodes (wb_cache)
- No NVMe info whatsoever            - WAL / LogSvc state
                                     - No S3 info whatsoever
```

### 4.5 Why No Per-Chunk Superblocks or Volume Index

All chunk information is in the pdev_s3 superblock. All HomeStore metadata (bitmap, CP state, service configs) is in the MetaBlk chunks. This gives us:
- *Single entry point for recovery:* download pdev_s3 superblock → know everything
- *No bucket scanning:* pdev_s3 superblock lists all chunks and their S3 keys
- *Minimal metadata objects:* just pdev_s3 superblock + chunk data objects
- *Snapshot state centralized:* one place to manage pins and GC

---

## 5. S3 Object Layout

### 5.1 Bucket Strategy: One Bucket Per Cluster

Each cluster gets its own S3 bucket. Volumes are separated by key prefix.

```
Bucket: homestore-<cluster_id>
```

**Rationale:**
- One bucket per cluster keeps bucket count manageable (clusters are coarse-grained)
- Volume isolation via key prefix (`<volume_id>/...`)
- IAM policies can scope access per volume using key prefix conditions
- Volume deletion = delete all objects under `<volume_id>/` prefix

**Bucket creation:** Part of cluster provisioning. Created once per cluster.

**Bucket deletion:** Only on cluster teardown. Volume deletion deletes objects under the volume's prefix, not the bucket.

### 5.2 Key Schema

```
s3://homestore-<cluster_id>/<volume_id>/pdev_superblock.bin                       (pdev_s3 superblock)
s3://homestore-<cluster_id>/<volume_id>/chunks/<chunk_id>.dat                     (v1: chunk data)
s3://homestore-<cluster_id>/<volume_id>/chunks/<chunk_id>_gen<N>.dat              (snapshots)
```

In v3 (SST/LSM per chunk):
```
s3://homestore-<cluster_id>/<volume_id>/pdev_superblock.bin
s3://homestore-<cluster_id>/<volume_id>/chunks/<chunk_id>/manifest.json
s3://homestore-<cluster_id>/<volume_id>/chunks/<chunk_id>/sst_<seq>.dat
```

### 5.3 Naming Modes

**No snapshots:** `chunks/<chunk_id>/data.dat` — overwritten each CP.

**Snapshots exist:** `chunks/<chunk_id>/data_gen<N>.dat` — new object per CP. Old objects pinned by snapshots.

### 5.4 Object Sizes

Chunk size determines S3 object size. Same as HomeStore's existing chunk size (configurable). Target: 64MB–256MB per object for efficient S3 operations.

---

## 5A. S3 Upload Granularity

### 5A.1 Decision: v1 = Full Chunk Upload

Each CP uploads **entire dirty chunks** as single S3 objects. One `PutObject` per dirty chunk.

This is the simplest correct approach:
- One S3 object = one chunk (e.g., 32MB–256MB)
- Recovery is trivial: download chunk → write to NVMe
- Snapshot CoW is clean: pin one S3 object per chunk
- Chunk superblock mapping is straightforward: `chunk_id → s3_key`

**Trade-off acknowledged:** If only 4KB is dirty in a 32MB chunk, we still upload 32MB. This is up to 8000x write amplification. Acceptable for v1 because:
- Simplicity matters more than bandwidth optimization at this stage
- Need real workload data to know actual dirty-page distribution
- S3 bandwidth is cheap relative to engineering complexity

### 5A.2 v2 Evolution: Baseline + Delta Uploads

If v1 bandwidth consumption is too high (measure first!), evolve to a hybrid scheme with two S3 object types per chunk:

**Type 1 — Baseline (full chunk):**
```
Key:  s3://chunks/{chunk_id}/base_gen{N}.dat
Size: Full chunk (e.g., 32MB)
When: First upload + every K checkpoints (e.g., K=10 or K=20)
```

**Type 2 — Delta (dirty pages only):**
```
Key:  s3://chunks/{chunk_id}/delta_cp{X}.dat
Size: Only the pages that changed in that CP
Format: { page_bitmap: [12, 305, 7001], page_data: [4KB, 4KB, 4KB] }
When: Every CP between baselines
```

**Why not start with v2:**
- Adds complexity to recovery (must replay deltas in order)
- Snapshot CoW must track both baselines and deltas
- Need real workload data to tune K (baseline interval) and page granularity
- v1 is a strict subset of v2 (v1 = v2 where K=1, i.e., every CP is a baseline)

**Migration path:** v1 → v2 requires no schema break. Add delta support alongside existing full-chunk uploads.

### 5A.3 v3 Target: Chunk-as-LSM (SST Files Per Chunk)

The ultimate target architecture. Each chunk on S3 is structured like an LSM tree with its own SST (Sorted String Table) files and per-chunk compaction.

**Key insight:** NVMe stays mutable and in-place (no change). The SST/LSM structure exists *only* on S3. Upper layers (B+tree, CP, BlkId) are completely unaware — they still deal in BlkIds. The LSM is below the BlkId abstraction.

**How it works:**

```
BlkId → (chunk_id, offset_within_chunk) → SST lookup within chunk's LSM
```

- Each chunk on S3 maintains its own set of SST files
- SST files are sorted by offset (block offset within the chunk)
- Top-level SST files contain the actual data blocks
- Lower-level SST files use offset as key and data as value
- Writes to S3 are always *append-only* — new data becomes a new SST file
- Compaction merges SST files within a single chunk, GCs old versions
- No cross-chunk references — each chunk is fully self-contained

**Write flow (S3 side):**
```
CP triggers:
  For each dirty chunk:
    Collect dirty blocks (offset → data pairs)
    Write as a new SST file: s3://chunks/{chunk_id}/sst_{seq}.dat
    Update chunk manifest with new SST
```

**Read flow (S3 side):**
```
read_from_s3(chunk_id, offset):
  Read chunk manifest → list of SST files (newest first)
  Search SSTs for offset (newest wins)
  Return data block
```

**Compaction (per-chunk):**
```
compact(chunk_id):
  Merge all SST files for chunk into one sorted SST
  Upload merged SST
  Delete old SSTs
  Update manifest
```

**SST file format:**
```
SST File {
  header: { chunk_id, cp_generation, num_entries, checksum }
  data_blocks: [
    { offset: u64, data: [u8; block_size] },
    { offset: u64, data: [u8; block_size] },
    ...
  ]  // sorted by offset
  index: [ { offset: u64, file_position: u64 } ]  // for binary search
  footer: { index_offset, checksum }
}
```

**Why this is better than v1/v2:**
- *Minimal S3 bandwidth* — each CP only uploads the dirty blocks as a small SST
- *No delta replay complexity* — reads search SSTs directly (like LSM point lookups)
- *Compaction amortizes cost* — merging happens in background, doesn't block CPs
- *Self-contained* — each chunk is independently readable, compactable, and recoverable
- *Natural fit for snapshots* — snapshot pins SST files; new CPs append new SSTs; old SSTs are immutable

**Why not start with v3:**
- SST format, compaction logic, manifest management = significant new code
- Want to validate end-to-end S3 plumbing with v1 first
- v1 → v3 migration is clean: just change how a chunk is represented on S3

### 5A.4 v3-Forward Interface: ChunkStore Abstraction

**Critical design decision:** v1 implementation must use the v3-ready interface. Callers (CP hook, read path, recovery) program against this abstraction, never against the storage format directly.

```cpp
// Abstract interface — v1 and v3 implement differently, callers don't change
class ChunkStore {
public:
    // Write dirty blocks to S3 for a chunk
    // v1: serialize full chunk → single S3 PUT
    // v3: write dirty blocks as new SST file → S3 PUT
    virtual S3Result put(chunk_id_t chunk_id,
                         const std::vector<DirtyBlock>& dirty_blocks) = 0;

    // Read a block from S3 by chunk + offset
    // v1: S3 GET full chunk → seek to offset → return block
    // v3: search SSTs for offset → return block
    virtual folly::Future<Data> get(chunk_id_t chunk_id,
                                     offset_t offset) = 0;

    // Compact storage for a chunk (reduce S3 footprint)
    // v1: no-op (already a single object)
    // v3: merge SST files → write compacted SST → delete old SSTs
    virtual S3Result compact(chunk_id_t chunk_id) = 0;

    // Recover chunk state from S3 (for NVMe restore)
    // v1: download single S3 object → write to NVMe
    // v3: download manifest + SSTs → merge → write to NVMe
    virtual ChunkState recover(chunk_id_t chunk_id) = 0;

    // List/describe current S3 state for a chunk
    // v1: return single S3 key
    // v3: return manifest with SST list
    virtual ChunkMetadata describe(chunk_id_t chunk_id) = 0;
};

// v1 implementation
class FullChunkStore : public ChunkStore { ... };

// v3 implementation (future)
class SSTChunkStore : public ChunkStore { ... };
```

**DirtyBlock struct:**
```cpp
struct DirtyBlock {
    offset_t offset;              // offset within chunk
    sisl::byte_array data;        // shared_ptr<io_blob_safe> — refcounted, aligned, RAII
};
```

**Why this matters:**
- CP hook calls `chunk_store->put(chunk_id, dirty_blocks)` — doesn't know if it's uploading a full chunk or appending an SST
- Recovery calls `chunk_store->recover(chunk_id)` — doesn't know if it's downloading one blob or merging SSTs
- Swapping v1 → v3 is a single implementation swap, zero caller changes
- v1 `put()` ignores the `dirty_blocks` granularity and uploads the full chunk anyway, but the interface captures the dirty block info for v3 to use

### 5A.5 Bandwidth Comparison

Assumptions: 100 chunks, 10 dirty per CP, chunk size = 32MB, avg 50 dirty pages (4KB each) per dirty chunk.

| Approach | S3 bandwidth per CP | S3 PUTs per CP | Recovery cost (per chunk) |
|----------|--------------------:|---------------:|-------------------------:|
| **v1: Full chunk** | 320 MB | 10 | 1 GET (32MB) |
| **v2: Delta (K=10)** | ~2 MB (9 of 10 CPs) | 10 | 1 GET + up to 9 GETs |
| **v2: Amortized** | ~34 MB avg | 10 | ~1.9 GETs avg |
| **v3: SST per CP** | ~2 MB | 10 | N GETs (manifest + SSTs) |
| **v3: Post-compaction** | ~2 MB (CPs) + periodic compaction | 10 + compaction PUTs | 1 GET (compacted SST) |

v3 achieves v2-level bandwidth *without* the ordered-delta-replay complexity. Compaction keeps recovery cost bounded.

---

## 6. Recovery

### 6.1 Normal Recovery (NVMe Intact)

Unchanged from today:
1. Read superblock from NVMe (higher-gen ping-pong slot)
2. Replay binlog from last CP
3. Resume

### 6.2 Full Recovery (NVMe Lost)

With the tiered/dynamic chunk model, recovery can be on-demand:

**Option A: On-demand hydration (fast startup)**
1. Download pdev_s3 superblock → chunk list (types, S3 keys, snapshots)
2. Download essential chunks (small, needed for HomeStore init):
   - MetaBlk chunks (CP superblock with bitmap, service state)
   - WAL chunks (log replay)
   - B+tree / index chunks (index nodes)
3. HomeStore initializes from MetaBlk + WAL + B+tree (same as normal NVMe boot)
4. Mark data chunks only: on_nvme=false, on_s3=true
5. Start serving reads — first access to a data chunk triggers hydration from S3
6. Background: proactively hydrate hot data chunks

**Option B: Bulk download (simpler, slower startup)**
1. Download pdev_s3 superblock → chunk list
2. Download MetaBlk + WAL + B+tree chunks → HomeStore can initialize
3. Download all data chunks from S3 → write to NVMe
4. Normal startup once all chunks are local
5. Replay binlog for post-CP mutations

**Recommended:** Option A for production (fast recovery), Option B for simplicity in v1.

### 6.3 Superblock Recovery

If NVMe pdev superblock is corrupted → fall back to pdev_s3 superblock on S3.
If both lost → restore from latest snapshot in S3 (if snapshots exist).

---

## 7. S3 Garbage Collection

### 7.1 Without Snapshots

Each CP overwrites the same S3 object per chunk. No GC needed — old version is gone.

### 7.2 With Snapshots

Old S3 objects accumulate (pinned by snapshots). GC rule:

An S3 object can be deleted when:
- It is not the *current* version (not in the pdev_s3 superblock's chunk list)
- AND no snapshot in pdev_s3 superblock references it

On snapshot deletion: check pdev_s3 superblock's remaining snapshots. For each S3 key from the deleted snapshot, if no other snapshot references it, delete from S3.

### 7.3 Intermediate CP Objects

Between two snapshots, each CP creates new S3 objects. Only the latest and the snapshot-pinned versions matter. Intermediate versions can be GC'd:

```
Snapshot S1 at gen 42: pins gen 42 objects
CP 43: creates gen 43 objects
CP 44: creates gen 44 objects ← this is current

gen 43 objects: not current, not pinned by any snapshot → DELETE
gen 42 objects: pinned by S1 → KEEP
gen 44 objects: current → KEEP
```

This GC runs after each CP commit.

---

## 8. Writable Clones & Fast Replica Spin-Up

### 8.1 Concept

A writable clone starts from S3 state and gets its own NVMe + S3 prefix.

### 8.2 Clone Creation

```
clone(source_volume, at_generation):
  1. Create snapshot S on source (pins S3 objects)
  2. New volume V' gets its own key prefix: <clone_volume_id>/
  3. Copy S3 objects from source prefix to clone prefix
     (same bucket, S3 CopyObject — server-side, fast)
  4. V' allocates fresh NVMe — hydrate on demand or bulk download
  5. V' is now writable — in-place on its own NVMe
     Future CPs write under V's own prefix
```

### 8.3 Use Cases

- **Fast replica provisioning** — Clone from S3 instead of streaming via binlog.
- **Dev/test forks** — Writable copy with full isolation.
- **Point-in-time recovery** — Clone from a snapshot at any pinned generation.

### 8.4 S3 Object Lifecycle

Each volume owns its objects under its key prefix (`<volume_id>/...`):
- Volume deletion = delete all objects under `<volume_id>/` prefix
- Clones copy objects within the same bucket (fast, server-side S3 CopyObject)
- Snapshot GC is volume-local (only need to track references within the volume's own snapshots)
- No cross-volume reference counting needed

---

## 9. Consistency Model

### 9.1 Durability Levels

| Level | Guarantee | Latency |
|-------|-----------|---------|
| *Replicated (binlog)* | Replicas have it on NVMe. Survives minority failures. | Microseconds–low ms |
| *Checkpoint-flushed* | On NVMe + S3. Survives process crash + NVMe loss. | Periodic (CP interval) |

### 9.2 Durability Gap

Between CPs, data is on NVMe only (protected by binlog replication). S3 is updated at each CP. Gap is bounded by the CP interval.

### 9.3 CP Upload Ordering

CPs must commit to S3 in order. If CP N fails to upload, CP N+1 is blocked. Data accumulates on NVMe until S3 backlog clears.

### 9.4 Who Uploads

Primary-only. On failover, new primary reads S3 state to determine what was already uploaded, then resumes.

---

## 10. What Changes vs What Doesn't

### Changes

| Component | Change |
|-----------|--------|
| DeviceManager | New pdev type: pdev_s3 (implements pdev interface for S3) |
| VirtualDev | Mirrored chunks span pdev_nvme + pdev_s3 (uses existing mirror mechanism) |
| Chunk | Dynamic placement (on_nvme, on_s3 flags). Single shared bitmap. |
| Read path | Tiered: NVMe first, S3 fallback if chunk not on NVMe |
| CPManager | CP hook flushes dirty chunks to S3 pdev via ChunkStore |
| Recovery | Can recover from S3 (on-demand hydration or bulk download) |

### Doesn't Change

| Component | Status |
|-----------|--------|
| Write path (NVMe side) | Unchanged — in-place writes to NVMe pdev |
| B+tree / IndexSvc | Unchanged — in-place, wb_cache as-is |
| BlkAllocator | Unchanged logic — same allocation/free, bitmap now shared across devices |
| WAL / LogSvc | Unchanged |
| DataSvc | Unchanged |
| Binlog replication | Unchanged |
| Volume API | Unchanged |

---

## 11. Migration Strategy

### Phase 1: S3 pdev + Full Chunk Upload (v1 MVP)
- Implement `pdev_s3` as a new pdev type in DeviceManager
- Wire into VirtualDev's mirrored chunk mechanism
- Implement `ChunkStore` interface (Section 5A.4) with `FullChunkStore` backend
- CP hook flushes dirty chunks to S3 via `ChunkStore::put()`
- Chunk superblock + volume index on S3 (Section 4)
- Bitmap uploaded to S3 per chunk for recovery
- *Key: all callers use ChunkStore abstraction, never talk to S3 directly*

### Phase 2: Tiered Reads + Recovery
- Implement tiered read path: NVMe → S3 fallback
- Recovery from S3: bulk download (Option B) initially
- Test: wipe NVMe, recover from S3 + binlog replay
- Validate end-to-end: write → CP → S3 upload → NVMe wipe → S3 recovery → reads succeed

### Phase 3: Dynamic Chunk Lifecycle
- Chunk eviction: remove cold chunks from NVMe (Section 3.8)
- Chunk hydration: cache-on-read from S3 (Section 3.9)
- On-demand recovery (Option A)
- NVMe capacity management / eviction policies

### Phase 4: Snapshots
- Implement snapshot creation (pin S3 objects, record in chunk superblocks)
- S3 CoW: when snapshots exist, CPs write new S3 objects instead of overwriting
- Snapshot reads from S3 via `ChunkStore::get()`
- Snapshot deletion + S3 GC

### Phase 5: SST/LSM Per Chunk (v3)
- Implement `SSTChunkStore` backend behind same `ChunkStore` interface
- SST file format: sorted offset→data entries with index block
- Per-chunk manifest tracking SST files
- `put()` writes dirty blocks as new SST file (append-only)
- `get()` searches SSTs newest-first for offset
- `compact()` merges SSTs within a chunk, GCs old files
- `recover()` downloads manifest + SSTs, merge-reads to reconstruct NVMe
- Swap `FullChunkStore` → `SSTChunkStore` — zero caller changes

### Phase 6: Writable Clones
- Clone = new volume with copied S3 objects under new prefix
- Fast replica provisioning via S3 CopyObject

---

## 12. Open Questions

1. **Chunk upload granularity** — *Decided: v1 = full chunk upload, v3 target = SST/LSM per chunk (Section 5A).* Implement v1 first using the v3-forward ChunkStore interface (Section 5A.4).

2. **Binlog on S3** — Should the binlog also be uploaded to S3 for replay during recovery? Or is it ephemeral (only needed between CPs)?

3. **Snapshot read performance** — B+tree traversal from S3 could be slow (multiple round trips per node). Cache hot B+tree nodes locally? Pre-fetch index chunks?

4. **CP upload latency** — S3 upload adds time to the CP path. Should it be synchronous (simpler, S3 always up to date) or async (faster CP, but S3 may lag)?

5. **Multi-region S3** — Replicate S3 objects across regions for DR?

6. **Snapshot limit** — Maximum snapshot slots per chunk superblock?

7. **Cost modeling** — S3 PUT/GET costs at scale. Need to model request rates per CP.

8. **Eviction policy** — LRU? Access-frequency-based? Configurable?

9. **Hydration strategy** — Cache-on-read only, or proactive background hydration of hot chunks?

10. **pdev_s3 interface boundaries** — How much of the existing pdev interface does pdev_s3 need to implement? Reads + writes + metadata, or a subset?

---

## 13. Success Metrics

- NVMe write/read latency unchanged from baseline (zero overhead on hot path).
- Recovery from NVMe loss: serving reads within seconds (on-demand hydration).
- Full NVMe restoration < 10 minutes for 100GB.
- Snapshot creation < 1 second (just S3 pin + superblock update).
- Clone creation < 30 seconds regardless of volume size.
- S3 storage overhead < 1.1x of live data size (without snapshots).
- Cold chunk reads from S3 < 100ms (single block read).

---

## 14. Summary

The design is built on integrating S3 as a native pdev in HomeStore's device hierarchy:

- **Architecture:** S3 is a pdev, chunks are mirrored across NVMe + S3 via vdev's existing mechanism.
- **Placement is dynamic:** Chunks can be on NVMe, S3, or both. Eviction and hydration change placement at runtime.
- **Single bitmap:** One BlkAllocator bitmap per chunk, shared across devices. Allocation state is device-independent.
- **Writes:** NVMe (fast, sync). S3 updated at CP (async, batched).
- **Reads:** NVMe first, S3 fallback. Cache-on-read for hydration.
- **Snapshots:** Pin S3 objects (immutable). Future CPs write new S3 objects. NVMe doesn't care.
- **Recovery:** On-demand from S3. No need to restore all chunks before serving.
- **Clones:** Copy S3 objects to new prefix, hydrate NVMe on demand.

---

## 15. References

- HomeStore source (eBay): storage engine internals
- HomeBlocks: volume abstraction layer
- AWS S3 strong consistency model (Dec 2020)
- AWS S3 PutObject atomicity guarantees
- VirtualDev mirrored chunk implementation (HomeStore)
