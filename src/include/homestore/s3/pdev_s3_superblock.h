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
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <homestore/crc.h>
#include <homestore/homestore_decl.hpp>
#include <homestore/s3/chunk_store.h>
#include <homestore/s3/s3_object_store.h>

namespace homestore {

/// Type of chunk stored on S3 — mirrors HomeStore's internal chunk types.
/// Used to identify which chunks to download first during recovery (MetaBlk
/// chunks contain CP superblock + bitmap; WAL chunks for log replay).
enum class S3ChunkType : uint8_t {
    DATA = 0,
    INDEX = 1,
    WAL = 2,
    METABLK = 3,
};

/**
 * @brief Per-chunk entry in the pdev_s3 superblock.
 *
 * Each entry describes a chunk's S3 location and type, analogous to
 * chunk_info on the NVMe pdev superblock. No NVMe-specific info
 * (device offsets, etc.) is stored here.
 */
#pragma pack(1)
struct s3_chunk_entry {
    static constexpr size_t MAX_S3_KEY_LEN = 256;

    uint64_t chunk_id{0};
    uint64_t chunk_size{0};
    S3ChunkType chunk_type{S3ChunkType::DATA};
    uint64_t vdev_id{0};
    uint64_t generation{0};     ///< S3 object generation (for snapshot support)
    char s3_key[MAX_S3_KEY_LEN]{};   ///< Current S3 object key for this chunk

    void set_s3_key(const std::string& key) {
        std::strncpy(s3_key, key.c_str(), MAX_S3_KEY_LEN - 1);
        s3_key[MAX_S3_KEY_LEN - 1] = '\0';
    }

    std::string get_s3_key() const { return std::string{s3_key}; }

    bool is_metablk() const { return chunk_type == S3ChunkType::METABLK; }
    bool is_wal() const { return chunk_type == S3ChunkType::WAL; }
    bool is_index() const { return chunk_type == S3ChunkType::INDEX; }
    bool is_data() const { return chunk_type == S3ChunkType::DATA; }
};
#pragma pack()

/**
 * @brief The pdev_s3 superblock — single S3 metadata object that mirrors
 *        pdev_nvme's superblock structure plus snapshot state.
 *
 * This is the entry point for all S3 recovery:
 *   1. Download pdev_s3_superblock from known S3 key
 *   2. Parse chunk list → know all chunks and their S3 keys
 *   3. Download MetaBlk chunks first (identified by chunk_type == METABLK)
 *   4. Then WAL, index, data chunks
 *
 * S3 key: <volume_id>/pdev_superblock.bin
 * Written last during CP (S3 commit point). If crash mid-CP, this
 * superblock for that generation was never written, so recovery falls
 * back to previous generation.
 *
 * The in-memory representation uses a variable-length chunk array.
 * Serialized format:
 *   [header (fixed)] [chunk_entry_0] [chunk_entry_1] ... [chunk_entry_N]
 *
 * Checksum covers the entire serialized buffer (with checksum field zeroed).
 */

#pragma pack(1)
struct pdev_s3_sb_header {
    static constexpr uint32_t MAGIC = 0x5033534B;  // "P3SK"
    static constexpr uint32_t CURRENT_VERSION = 1;

    uint32_t magic{MAGIC};
    uint32_t version{CURRENT_VERSION};
    uint64_t pdev_id{0};
    uint64_t generation{0};        ///< Monotonically increasing per CP flush
    uint32_t num_chunks{0};        ///< Number of s3_chunk_entry following the header
    uint32_t reserved{0};
    uint64_t checksum{0};          ///< CRC-32C of the entire serialized buffer (this field zeroed during computation)
};
#pragma pack()

/**
 * @brief In-memory representation of the pdev_s3 superblock.
 *
 * Provides serialization/deserialization to a flat buffer for S3 storage,
 * with checksum validation.
 */
class PdevS3Superblock {
public:
    PdevS3Superblock() = default;
    ~PdevS3Superblock() = default;

    // Accessors
    uint64_t pdev_id() const { return m_header.pdev_id; }
    void set_pdev_id(uint64_t id) { m_header.pdev_id = id; }

    uint64_t generation() const { return m_header.generation; }
    void set_generation(uint64_t gen) { m_header.generation = gen; }
    void increment_generation() { ++m_header.generation; }

    uint32_t num_chunks() const { return static_cast< uint32_t >(m_chunks.size()); }

    const std::vector< s3_chunk_entry >& chunks() const { return m_chunks; }
    std::vector< s3_chunk_entry >& chunks_mutable() { return m_chunks; }

    /// Add a chunk entry
    void add_chunk(const s3_chunk_entry& entry) { m_chunks.push_back(entry); }

    /// Remove a chunk entry by chunk_id
    bool remove_chunk(uint64_t chunk_id);

    /// Find a chunk entry by chunk_id (returns nullptr if not found)
    const s3_chunk_entry* find_chunk(uint64_t chunk_id) const;
    s3_chunk_entry* find_chunk_mutable(uint64_t chunk_id);

    /// Update a chunk's S3 key (e.g. after a new generation upload)
    bool update_chunk_key(uint64_t chunk_id, const std::string& new_key, uint64_t generation = 0);

    /// Get chunks by type (for recovery ordering)
    std::vector< const s3_chunk_entry* > get_chunks_by_type(S3ChunkType type) const;

    /**
     * @brief Serialize the superblock to a flat buffer.
     *
     * Layout: [pdev_s3_sb_header] [s3_chunk_entry * N]
     * Checksum computed over the entire buffer with checksum field zeroed.
     *
     * @return Serialized buffer
     */
    sisl::byte_array serialize() const;

    /**
     * @brief Deserialize a superblock from a flat buffer.
     *
     * Validates magic, version, and checksum. Returns false on any validation
     * failure.
     *
     * @param data  Buffer containing serialized superblock
     * @return true if deserialization succeeded and checksum is valid
     */
    bool deserialize(const sisl::byte_array& data);

    /**
     * @brief Write the superblock to S3.
     *
     * @param s3_store   S3 object store
     * @param volume_id  Volume ID for key construction
     * @return S3Result indicating success or failure
     */
    S3Result write_to_s3(S3ObjectStore& s3_store, const std::string& volume_id) const;

    /**
     * @brief Read the superblock from S3.
     *
     * @param s3_store   S3 object store
     * @param volume_id  Volume ID for key construction
     * @return S3Result indicating success or failure (superblock populated on success)
     */
    S3Result read_from_s3(S3ObjectStore& s3_store, const std::string& volume_id);

    /// Get the S3 key for the pdev superblock
    static std::string s3_key(const std::string& volume_id) {
        return volume_id + "/pdev_superblock.bin";
    }

    /// Get the total serialized size
    uint64_t serialized_size() const {
        return sizeof(pdev_s3_sb_header) + m_chunks.size() * sizeof(s3_chunk_entry);
    }

private:
    pdev_s3_sb_header m_header;
    std::vector< s3_chunk_entry > m_chunks;

    /// Compute CRC-32C over serialized data (with checksum field zeroed)
    static uint64_t compute_checksum(const uint8_t* data, uint64_t size);
};

} // namespace homestore
