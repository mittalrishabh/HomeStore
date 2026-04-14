/*********************************************************************************
 * Copyright 2024 eBay Inc.
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
#include <cstring>

#include <homestore/s3/pdev_s3_superblock.h>
#include <homestore/crc.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

bool PdevS3Superblock::remove_chunk(uint64_t chunk_id) {
    auto it = std::remove_if(m_chunks.begin(), m_chunks.end(),
                             [chunk_id](const s3_chunk_entry& e) { return e.chunk_id == chunk_id; });
    if (it == m_chunks.end()) return false;
    m_chunks.erase(it, m_chunks.end());
    return true;
}

const s3_chunk_entry* PdevS3Superblock::find_chunk(uint64_t chunk_id) const {
    for (const auto& entry : m_chunks) {
        if (entry.chunk_id == chunk_id) return &entry;
    }
    return nullptr;
}

s3_chunk_entry* PdevS3Superblock::find_chunk_mutable(uint64_t chunk_id) {
    for (auto& entry : m_chunks) {
        if (entry.chunk_id == chunk_id) return &entry;
    }
    return nullptr;
}

bool PdevS3Superblock::update_chunk_key(uint64_t chunk_id, const std::string& new_key, uint64_t generation) {
    auto* entry = find_chunk_mutable(chunk_id);
    if (!entry) return false;
    entry->set_s3_key(new_key);
    if (generation != 0) { entry->generation = generation; }
    return true;
}

std::vector< const s3_chunk_entry* > PdevS3Superblock::get_chunks_by_type(S3ChunkType type) const {
    std::vector< const s3_chunk_entry* > result;
    for (const auto& entry : m_chunks) {
        if (entry.chunk_type == type) { result.push_back(&entry); }
    }
    return result;
}

bool PdevS3Superblock::remove_snapshot(uint64_t snap_id) {
    auto it = std::remove_if(m_snapshots.begin(), m_snapshots.end(),
                             [snap_id](const SnapshotRecord& s) { return s.header.snap_id == snap_id; });
    if (it == m_snapshots.end()) return false;
    m_snapshots.erase(it, m_snapshots.end());
    return true;
}

const PdevS3Superblock::SnapshotRecord* PdevS3Superblock::find_snapshot(uint64_t snap_id) const {
    for (const auto& snap : m_snapshots) {
        if (snap.header.snap_id == snap_id) return &snap;
    }
    return nullptr;
}

uint64_t PdevS3Superblock::serialized_size() const {
    uint64_t size = sizeof(pdev_s3_sb_header) + m_chunks.size() * sizeof(s3_chunk_entry);
    for (const auto& snap : m_snapshots) {
        size += sizeof(s3_snapshot_entry);
        size += snap.chunk_keys.size() * sizeof(s3_snapshot_chunk_key);
    }
    return size;
}

sisl::byte_array PdevS3Superblock::serialize() const {
    auto total_size = static_cast< uint32_t >(serialized_size());
    auto buf = sisl::make_byte_array(total_size, 0);
    auto* raw = buf->bytes();

    // Write header
    pdev_s3_sb_header hdr = m_header;
    hdr.num_chunks = static_cast< uint32_t >(m_chunks.size());
    hdr.num_snapshots = static_cast< uint32_t >(m_snapshots.size());
    hdr.checksum = 0;
    std::memcpy(raw, &hdr, sizeof(hdr));

    // Write chunk entries
    auto* ptr = raw + sizeof(pdev_s3_sb_header);
    for (const auto& entry : m_chunks) {
        std::memcpy(ptr, &entry, sizeof(s3_chunk_entry));
        ptr += sizeof(s3_chunk_entry);
    }

    // Write snapshot entries: [s3_snapshot_entry][chunk_key_0][chunk_key_1]...
    for (const auto& snap : m_snapshots) {
        s3_snapshot_entry snap_hdr = snap.header;
        snap_hdr.num_chunk_keys = static_cast< uint32_t >(snap.chunk_keys.size());
        std::memcpy(ptr, &snap_hdr, sizeof(s3_snapshot_entry));
        ptr += sizeof(s3_snapshot_entry);

        for (const auto& ck : snap.chunk_keys) {
            std::memcpy(ptr, &ck, sizeof(s3_snapshot_chunk_key));
            ptr += sizeof(s3_snapshot_chunk_key);
        }
    }

    // Compute and store checksum
    auto csum = compute_checksum(raw, total_size);
    auto* hdr_ptr = reinterpret_cast< pdev_s3_sb_header* >(raw);
    hdr_ptr->checksum = csum;

    return buf;
}

bool PdevS3Superblock::deserialize(const sisl::byte_array& data) {
    if (!data || data->size() < sizeof(pdev_s3_sb_header)) {
        LOGERRORMOD(s3, "PdevS3Superblock::deserialize: buffer too small ({} < {})",
                    data ? data->size() : 0, sizeof(pdev_s3_sb_header));
        return false;
    }

    auto* raw = data->cbytes();
    auto total_size = data->size();

    // Read header
    std::memcpy(&m_header, raw, sizeof(pdev_s3_sb_header));

    // Validate magic
    if (m_header.magic != pdev_s3_sb_header::MAGIC) {
        LOGERRORMOD(s3, "PdevS3Superblock::deserialize: invalid magic 0x{:08X} (expected 0x{:08X})",
                    m_header.magic, pdev_s3_sb_header::MAGIC);
        return false;
    }

    // Validate version
    if (m_header.version > pdev_s3_sb_header::CURRENT_VERSION) {
        LOGERRORMOD(s3, "PdevS3Superblock::deserialize: unsupported version {} (max {})",
                    m_header.version, pdev_s3_sb_header::CURRENT_VERSION);
        return false;
    }

    // Validate minimum size for chunks
    uint64_t min_size = sizeof(pdev_s3_sb_header) + m_header.num_chunks * sizeof(s3_chunk_entry);
    if (total_size < min_size) {
        LOGERRORMOD(s3, "PdevS3Superblock::deserialize: buffer too small for {} chunks ({} < {})",
                    m_header.num_chunks, total_size, min_size);
        return false;
    }

    // Validate checksum
    auto saved_checksum = m_header.checksum;
    auto temp_buf = sisl::make_byte_array(static_cast< uint32_t >(total_size), 0);
    std::memcpy(temp_buf->bytes(), raw, total_size);
    auto* temp_hdr = reinterpret_cast< pdev_s3_sb_header* >(temp_buf->bytes());
    temp_hdr->checksum = 0;

    auto computed = compute_checksum(temp_buf->cbytes(), total_size);
    if (computed != saved_checksum) {
        LOGERRORMOD(s3, "PdevS3Superblock::deserialize: checksum mismatch (stored={} computed={})",
                    saved_checksum, computed);
        return false;
    }

    // Read chunk entries
    m_chunks.clear();
    m_chunks.resize(m_header.num_chunks);
    auto* ptr = raw + sizeof(pdev_s3_sb_header);
    for (uint32_t i = 0; i < m_header.num_chunks; ++i) {
        std::memcpy(&m_chunks[i], ptr, sizeof(s3_chunk_entry));
        ptr += sizeof(s3_chunk_entry);
    }

    // Read snapshot entries (v2+)
    m_snapshots.clear();
    uint32_t num_snaps = (m_header.version >= 2) ? m_header.num_snapshots : 0;
    for (uint32_t i = 0; i < num_snaps; ++i) {
        if (static_cast< uint64_t >(ptr - raw) + sizeof(s3_snapshot_entry) > total_size) {
            LOGERRORMOD(s3, "PdevS3Superblock::deserialize: truncated snapshot entry {}", i);
            return false;
        }

        SnapshotRecord snap;
        std::memcpy(&snap.header, ptr, sizeof(s3_snapshot_entry));
        ptr += sizeof(s3_snapshot_entry);

        uint64_t keys_size = snap.header.num_chunk_keys * sizeof(s3_snapshot_chunk_key);
        if (static_cast< uint64_t >(ptr - raw) + keys_size > total_size) {
            LOGERRORMOD(s3, "PdevS3Superblock::deserialize: truncated snapshot chunk keys for snap_id={}",
                        snap.header.snap_id);
            return false;
        }

        snap.chunk_keys.resize(snap.header.num_chunk_keys);
        for (uint32_t j = 0; j < snap.header.num_chunk_keys; ++j) {
            std::memcpy(&snap.chunk_keys[j], ptr, sizeof(s3_snapshot_chunk_key));
            ptr += sizeof(s3_snapshot_chunk_key);
        }

        m_snapshots.push_back(std::move(snap));
    }

    LOGDEBUGMOD(s3, "PdevS3Superblock::deserialize: pdev_id={} gen={} num_chunks={} num_snapshots={}",
                m_header.pdev_id, m_header.generation, m_header.num_chunks, m_snapshots.size());
    return true;
}

S3Result PdevS3Superblock::write_to_s3(S3ObjectStore& s3_store, const std::string& volume_id) const {
    auto key = s3_key(volume_id);
    auto buf = serialize();

    LOGDEBUGMOD(s3, "Writing pdev_s3 superblock to S3: key={} pdev_id={} gen={} num_chunks={} num_snapshots={} size={}",
                key, m_header.pdev_id, m_header.generation, m_header.num_chunks, m_snapshots.size(), buf->size());

    sisl::io_blob_safe blob(buf->size(), 0);
    std::memcpy(blob.bytes(), buf->cbytes(), buf->size());
    auto result = s3_store.put_object(key, std::move(blob)).get();
    if (!result.ok()) {
        LOGERRORMOD(s3, "Failed to write pdev_s3 superblock to S3 key={}: {}", key, result.error_message);
    }
    return result;
}

S3Result PdevS3Superblock::read_from_s3(S3ObjectStore& s3_store, const std::string& volume_id) {
    auto key = s3_key(volume_id);

    LOGDEBUGMOD(s3, "Reading pdev_s3 superblock from S3: key={}", key);

    auto [get_result, blob] = s3_store.get_object(key).get();
    if (!get_result.ok()) {
        LOGERRORMOD(s3, "Failed to read pdev_s3 superblock from S3 key={}: {}", key, get_result.error_message);
        return get_result;
    }

    auto data = sisl::make_byte_array(static_cast< uint32_t >(blob.size()), 0);
    std::memcpy(data->bytes(), blob.cbytes(), blob.size());

    if (!deserialize(data)) {
        S3Result err;
        err.status_code = 500;
        err.error_message = "Failed to deserialize pdev_s3 superblock from " + key;
        return err;
    }

    LOGDEBUGMOD(s3, "Loaded pdev_s3 superblock: pdev_id={} gen={} num_chunks={} num_snapshots={}",
                m_header.pdev_id, m_header.generation, num_chunks(), m_snapshots.size());
    S3Result ok;
    ok.status_code = 0;
    return ok;
}

uint64_t PdevS3Superblock::compute_checksum(const uint8_t* data, uint64_t size) {
    uint32_t crc = init_crc32;
    uint64_t remaining = size;
    const uint8_t* ptr = data;
    while (remaining > 0) {
        auto chunk = static_cast< uint32_t >(std::min< uint64_t >(remaining, UINT32_MAX));
        crc = crc32_ieee(crc, ptr, chunk);
        ptr += chunk;
        remaining -= chunk;
    }
    return crc;
}

} // namespace homestore
