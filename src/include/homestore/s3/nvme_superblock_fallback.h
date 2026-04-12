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
#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <utility>

#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <homestore/crc.h>
#include <homestore/s3/s3_object_store.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

// Forward declaration — full definition in pdev_s3_superblock.h
class PdevS3Superblock;

/// Source of the superblock that was loaded at startup.
enum class SuperblockSource : uint8_t {
    NVME_SLOT_A = 0,
    NVME_SLOT_B = 1,
    S3_FALLBACK = 2,
    NONE = 3,  ///< No valid superblock found anywhere
};

/// Result of a superblock load attempt.
struct SuperblockLoadResult {
    SuperblockSource source{SuperblockSource::NONE};
    uint64_t sequence_number{0};  ///< Monotonic sequence from the loaded slot
    bool slot_a_valid{false};
    bool slot_b_valid{false};
    std::string error_message;    ///< Non-empty if source == NONE
};

/**
 * @brief On-disk layout for a single NVMe superblock slot (ping-pong scheme).
 *
 * Two of these exist at fixed NVMe offsets. On each CP, the inactive slot is
 * overwritten and then marked active. On startup the slot with the higher
 * sequence_number wins. CRC-32C covers everything except the checksum field
 * itself.
 *
 * The `payload` area stores the serialized first_block (or whatever the caller
 * puts in). This struct is a fixed-size *header*; the actual payload follows
 * it contiguously in the I/O buffer.
 */
#pragma pack(1)
struct nvme_sb_slot_header {
    static constexpr uint32_t MAGIC = 0x50504E47;  // "PPNG" — Ping-Pong NVMe SB
    static constexpr uint32_t CURRENT_VERSION = 1;

    uint32_t magic{MAGIC};
    uint32_t version{CURRENT_VERSION};
    uint64_t sequence_number{0};     ///< Monotonically increasing; higher = newer
    uint32_t payload_size{0};        ///< Size of the payload that follows this header
    uint32_t reserved{0};
    uint64_t checksum{0};            ///< CRC-32C of header (with checksum zeroed) + payload
};
#pragma pack()

/**
 * @brief Callback interface for reading/writing raw bytes to NVMe at fixed
 *        offsets. This decouples NvmeSuperblockManager from PhysicalDev so
 *        we can unit-test with in-memory backing.
 */
class NvmeDeviceIO {
public:
    virtual ~NvmeDeviceIO() = default;

    /// Write `size` bytes from `buf` at `offset` on the device.
    virtual void sync_write(const uint8_t* buf, uint32_t size, uint64_t offset) = 0;

    /// Read `size` bytes into `buf` from `offset` on the device.
    virtual std::error_code sync_read(uint8_t* buf, uint32_t size, uint64_t offset) = 0;
};

/**
 * @brief NVMe ping-pong superblock manager with S3 fallback.
 *
 * Owns two fixed slots on NVMe (slot A at offset_a, slot B at offset_b).
 * Writes alternate between them (ping-pong). On startup, the valid slot with
 * the higher sequence number is used. If both slots are corrupt, the manager
 * falls back to reading the pdev_s3 superblock from S3, reconstructs local
 * state, and logs a warning.
 *
 * Thread-safety: write() must be called from a single thread (CP flush path).
 * load() is called once at startup.
 */
class NvmeSuperblockManager {
public:
    /// @param device_io   Raw device I/O interface
    /// @param slot_a_offset  NVMe byte offset for slot A
    /// @param slot_b_offset  NVMe byte offset for slot B
    /// @param slot_size      Max bytes per slot (header + payload)
    NvmeSuperblockManager(std::shared_ptr< NvmeDeviceIO > device_io,
                          uint64_t slot_a_offset,
                          uint64_t slot_b_offset,
                          uint32_t slot_size);

    ~NvmeSuperblockManager() = default;

    // Non-copyable, non-movable
    NvmeSuperblockManager(const NvmeSuperblockManager&) = delete;
    NvmeSuperblockManager& operator=(const NvmeSuperblockManager&) = delete;

    /**
     * @brief Load the superblock from NVMe (ping-pong) with S3 fallback.
     *
     * 1. Read both NVMe slots, validate CRC-32C.
     * 2. Pick the valid slot with the higher sequence_number.
     * 3. If both are corrupt and s3_store is provided, attempt S3 recovery.
     * 4. Return the payload and metadata about which source was used.
     *
     * @param s3_store   Optional S3 store for fallback (nullptr to disable)
     * @param volume_id  Volume ID for S3 key construction
     * @return pair of (payload bytes, load result)
     */
    std::pair< sisl::byte_array, SuperblockLoadResult >
    load(S3ObjectStore* s3_store = nullptr, const std::string& volume_id = {});

    /**
     * @brief Write the superblock to the *inactive* NVMe slot (ping-pong).
     *
     * Increments the sequence number, computes CRC-32C, and writes to the
     * slot that was NOT the last one written. On the very first write both
     * slots are written (bootstrap).
     *
     * @param payload  Serialized superblock data
     */
    void write(const sisl::byte_array& payload);

    /// Current sequence number (0 before first load/write).
    uint64_t current_sequence() const { return m_current_seq; }

    /// Which slot was last written (A=0, B=1, nullopt before first write).
    std::optional< uint8_t > active_slot() const { return m_active_slot; }

    /// Slot size (max payload + header).
    uint32_t slot_size() const { return m_slot_size; }

private:
    /// Read and validate a single slot.
    /// @return (payload, header) or (nullptr, zeroed header) if invalid.
    std::pair< sisl::byte_array, nvme_sb_slot_header >
    read_and_validate_slot(uint64_t offset);

    /// Write a fully prepared buffer (header + payload) to a slot.
    void write_slot(uint64_t offset, const uint8_t* buf, uint32_t total_size);

    /// Compute CRC-32C over header (with checksum field zeroed) + payload.
    static uint64_t compute_crc(const nvme_sb_slot_header& hdr, const uint8_t* payload, uint32_t payload_size);

    /// Attempt to reconstruct a superblock payload from S3.
    sisl::byte_array recover_from_s3(S3ObjectStore& s3_store, const std::string& volume_id);

    std::shared_ptr< NvmeDeviceIO > m_device_io;
    uint64_t m_slot_a_offset;
    uint64_t m_slot_b_offset;
    uint32_t m_slot_size;

    uint64_t m_current_seq{0};
    std::optional< uint8_t > m_active_slot;  ///< 0 = A, 1 = B
};

} // namespace homestore
