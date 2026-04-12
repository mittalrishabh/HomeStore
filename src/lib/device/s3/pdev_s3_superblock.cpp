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

sisl::byte_array PdevS3Superblock::serialize() const {
    auto total_size = static_cast< uint32_t >(serialized_size());
    auto buf = sisl::make_byte_array(total_size, 0);
    auto* raw = buf->bytes();

    // Write header
    pdev_s3_sb_header hdr = m_header;
    hdr.num_chunks = static_cast< uint32_t >(m_chunks.size());
    hdr.checksum = 0; // Zero checksum for computation
    std::memcpy(raw, &hdr, sizeof(hdr));

    // Write chunk entries
    auto* chunk_ptr = raw + sizeof(pdev_s3_sb_header);
    for (const auto& entry : m_chunks) {
        std::memcpy(chunk_ptr, &entry, sizeof(s3_chunk_entry));
        chunk_ptr += sizeof(s3_chunk_entry);
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

    // Validate size consistency
    uint64_t expected_size = sizeof(pdev_s3_sb_header) + m_header.num_chunks * sizeof(s3_chunk_entry);
    if (total_size < expected_size) {
        LOGERRORMOD(s3, "PdevS3Superblock::deserialize: buffer too small for {} chunks ({} < {})",
                    m_header.num_chunks, total_size, expected_size);
        return false;
    }

    // Validate checksum
    auto saved_checksum = m_header.checksum;

    // Create a temp copy with checksum zeroed to recompute
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
    auto* chunk_ptr = raw + sizeof(pdev_s3_sb_header);
    for (uint32_t i = 0; i < m_header.num_chunks; ++i) {
        std::memcpy(&m_chunks[i], chunk_ptr, sizeof(s3_chunk_entry));
        chunk_ptr += sizeof(s3_chunk_entry);
    }

    LOGDEBUGMOD(s3, "PdevS3Superblock::deserialize: pdev_id={} gen={} num_chunks={}",
                m_header.pdev_id, m_header.generation, m_header.num_chunks);
    return true;
}

S3Result PdevS3Superblock::write_to_s3(S3ObjectStore& s3_store, const std::string& volume_id) const {
    auto key = s3_key(volume_id);
    auto buf = serialize();

    LOGDEBUGMOD(s3, "Writing pdev_s3 superblock to S3: key={} pdev_id={} gen={} num_chunks={} size={}",
                key, m_header.pdev_id, m_header.generation, m_header.num_chunks, buf->size());

    // Convert byte_array to io_blob_safe for the async API, then .get() to block
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

    // Convert io_blob_safe to byte_array for deserialization
    auto data = sisl::make_byte_array(static_cast< uint32_t >(blob.size()), 0);
    std::memcpy(data->bytes(), blob.cbytes(), blob.size());

    if (!deserialize(data)) {
        S3Result err;
        err.status_code = 500;
        err.error_message = "Failed to deserialize pdev_s3 superblock from " + key;
        return err;
    }

    LOGDEBUGMOD(s3, "Loaded pdev_s3 superblock: pdev_id={} gen={} num_chunks={}",
                m_header.pdev_id, m_header.generation, num_chunks());
    S3Result ok;
    ok.status_code = 0;
    return ok;
}

uint64_t PdevS3Superblock::compute_checksum(const uint8_t* data, uint64_t size) {
    // Use HomeStore's standard CRC-32C
    return crc32_ieee(init_crc32, data, static_cast< uint32_t >(size));
}

} // namespace homestore
