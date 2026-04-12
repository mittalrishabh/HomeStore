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

#include <homestore/s3/nvme_superblock_fallback.h>
#include <homestore/crc.h>

SISL_LOGGING_INIT(s3)

namespace homestore {

// ─── Construction ────────────────────────────────────────────────────────────

NvmeSuperblockManager::NvmeSuperblockManager(std::shared_ptr< NvmeDeviceIO > device_io,
                                             uint64_t slot_a_offset,
                                             uint64_t slot_b_offset,
                                             uint32_t slot_size)
    : m_device_io{std::move(device_io)}
    , m_slot_a_offset{slot_a_offset}
    , m_slot_b_offset{slot_b_offset}
    , m_slot_size{slot_size} {
    RELEASE_ASSERT(m_device_io != nullptr, "NvmeDeviceIO must not be null");
    RELEASE_ASSERT(m_slot_size > sizeof(nvme_sb_slot_header),
                   "slot_size must be larger than header ({})", sizeof(nvme_sb_slot_header));
}

// ─── CRC ─────────────────────────────────────────────────────────────────────

uint64_t NvmeSuperblockManager::compute_crc(const nvme_sb_slot_header& hdr,
                                            const uint8_t* payload,
                                            uint32_t payload_size) {
    // Copy header and zero the checksum field for CRC computation
    nvme_sb_slot_header tmp = hdr;
    tmp.checksum = 0;

    uint32_t crc = crc32_ieee(init_crc32, reinterpret_cast< const unsigned char* >(&tmp), sizeof(tmp));
    if (payload && payload_size > 0) {
        crc = crc32_ieee(crc, reinterpret_cast< const unsigned char* >(payload), payload_size);
    }
    return static_cast< uint64_t >(crc);
}

// ─── Slot read + validate ────────────────────────────────────────────────────

std::pair< sisl::byte_array, nvme_sb_slot_header >
NvmeSuperblockManager::read_and_validate_slot(uint64_t offset) {
    // Allocate buffer for the full slot
    auto buf = sisl::make_byte_array(m_slot_size, 512 /* alignment */);
    std::memset(buf->bytes(), 0, m_slot_size);

    auto ec = m_device_io->sync_read(buf->bytes(), m_slot_size, offset);
    if (ec) {
        LOGWARN("NVMe slot read failed at offset={}: {}", offset, ec.message());
        return {nullptr, {}};
    }

    // Parse header
    if (m_slot_size < sizeof(nvme_sb_slot_header)) {
        return {nullptr, {}};
    }

    nvme_sb_slot_header hdr;
    std::memcpy(&hdr, buf->cbytes(), sizeof(hdr));

    // Validate magic
    if (hdr.magic != nvme_sb_slot_header::MAGIC) {
        LOGWARN("NVMe slot at offset={}: bad magic {:#x} (expected {:#x})", offset, hdr.magic,
                nvme_sb_slot_header::MAGIC);
        return {nullptr, {}};
    }

    // Validate version
    if (hdr.version != nvme_sb_slot_header::CURRENT_VERSION) {
        LOGWARN("NVMe slot at offset={}: unsupported version {} (expected {})", offset, hdr.version,
                nvme_sb_slot_header::CURRENT_VERSION);
        return {nullptr, {}};
    }

    // Validate payload size fits in slot
    if (hdr.payload_size == 0 ||
        hdr.payload_size + sizeof(nvme_sb_slot_header) > m_slot_size) {
        LOGWARN("NVMe slot at offset={}: invalid payload_size={} (slot_size={})", offset, hdr.payload_size,
                m_slot_size);
        return {nullptr, {}};
    }

    // CRC-32C validation
    const uint8_t* payload_ptr = buf->cbytes() + sizeof(nvme_sb_slot_header);
    auto expected_crc = compute_crc(hdr, payload_ptr, hdr.payload_size);
    if (hdr.checksum != expected_crc) {
        LOGWARN("NVMe slot at offset={}: CRC mismatch — stored={:#x}, computed={:#x}", offset, hdr.checksum,
                expected_crc);
        return {nullptr, {}};
    }

    // Extract payload
    auto payload = sisl::make_byte_array(hdr.payload_size);
    std::memcpy(payload->bytes(), payload_ptr, hdr.payload_size);

    LOGINFO("NVMe slot at offset={} valid: seq={}, payload_size={}, crc={:#x}", offset, hdr.sequence_number,
            hdr.payload_size, hdr.checksum);

    return {std::move(payload), hdr};
}

// ─── Slot write ──────────────────────────────────────────────────────────────

void NvmeSuperblockManager::write_slot(uint64_t offset, const uint8_t* buf, uint32_t total_size) {
    m_device_io->sync_write(buf, total_size, offset);
}

// ─── S3 fallback recovery ────────────────────────────────────────────────────

sisl::byte_array NvmeSuperblockManager::recover_from_s3(S3ObjectStore& s3_store, const std::string& volume_id) {
    // Download the pdev_s3 superblock from S3.  The S3 superblock is
    // written last during each CP and acts as the commit point, so it
    // always represents a consistent state.
    auto key = volume_id + "/pdev_superblock.bin";
    LOGWARN("Both NVMe superblock slots corrupt — attempting S3 fallback (key={})", key);

    auto fut = s3_store.get_object(key);
    auto [result, data] = std::move(fut).get();

    if (!result.ok()) {
        LOGERROR("S3 fallback failed: status={}, error={}", result.status_code, result.error_message);
        return nullptr;
    }

    if (data.size() == 0) {
        LOGERROR("S3 fallback returned empty payload for key={}", key);
        return nullptr;
    }

    // Wrap into byte_array
    auto payload = sisl::make_byte_array(data.size());
    std::memcpy(payload->bytes(), data.cbytes(), data.size());

    LOGWARN("S3 fallback succeeded: recovered {} bytes from key={}", data.size(), key);
    return payload;
}

// ─── Load (startup) ──────────────────────────────────────────────────────────

std::pair< sisl::byte_array, SuperblockLoadResult >
NvmeSuperblockManager::load(S3ObjectStore* s3_store, const std::string& volume_id) {
    SuperblockLoadResult lr;

    // Read both slots
    auto [payload_a, hdr_a] = read_and_validate_slot(m_slot_a_offset);
    auto [payload_b, hdr_b] = read_and_validate_slot(m_slot_b_offset);

    lr.slot_a_valid = (payload_a != nullptr);
    lr.slot_b_valid = (payload_b != nullptr);

    // Pick the winner: valid slot with higher sequence number
    sisl::byte_array chosen_payload{nullptr};
    nvme_sb_slot_header chosen_hdr{};
    SuperblockSource chosen_source{SuperblockSource::NONE};

    if (lr.slot_a_valid && lr.slot_b_valid) {
        // Both valid — pick higher sequence
        if (hdr_a.sequence_number >= hdr_b.sequence_number) {
            chosen_payload = std::move(payload_a);
            chosen_hdr = hdr_a;
            chosen_source = SuperblockSource::NVME_SLOT_A;
        } else {
            chosen_payload = std::move(payload_b);
            chosen_hdr = hdr_b;
            chosen_source = SuperblockSource::NVME_SLOT_B;
        }
        LOGINFO("Both NVMe slots valid — chose {} (seq_a={}, seq_b={})",
                chosen_source == SuperblockSource::NVME_SLOT_A ? "Slot A" : "Slot B",
                hdr_a.sequence_number, hdr_b.sequence_number);
    } else if (lr.slot_a_valid) {
        chosen_payload = std::move(payload_a);
        chosen_hdr = hdr_a;
        chosen_source = SuperblockSource::NVME_SLOT_A;
        LOGWARN("Only NVMe Slot A is valid (Slot B corrupted) — using Slot A (seq={})", hdr_a.sequence_number);
    } else if (lr.slot_b_valid) {
        chosen_payload = std::move(payload_b);
        chosen_hdr = hdr_b;
        chosen_source = SuperblockSource::NVME_SLOT_B;
        LOGWARN("Only NVMe Slot B is valid (Slot A corrupted) — using Slot B (seq={})", hdr_b.sequence_number);
    }

    // If we have a valid NVMe slot, use it
    if (chosen_source != SuperblockSource::NONE) {
        m_current_seq = chosen_hdr.sequence_number;
        m_active_slot = (chosen_source == SuperblockSource::NVME_SLOT_A) ? uint8_t{0} : uint8_t{1};
        lr.source = chosen_source;
        lr.sequence_number = chosen_hdr.sequence_number;
        return {std::move(chosen_payload), lr};
    }

    // Both NVMe slots corrupt — try S3 fallback
    if (s3_store != nullptr && !volume_id.empty()) {
        auto s3_payload = recover_from_s3(*s3_store, volume_id);
        if (s3_payload != nullptr) {
            // S3 recovery succeeded. We don't know the sequence number from S3
            // (it stores pdev_s3 superblock format, not the NVMe slot format).
            // Start at sequence 1 so the next write goes to a deterministic slot.
            m_current_seq = 0;
            m_active_slot = std::nullopt;  // Next write will bootstrap both slots.

            lr.source = SuperblockSource::S3_FALLBACK;
            lr.sequence_number = 0;
            LOGCRITICAL("ALERT: Recovered superblock from S3 — both NVMe slots were corrupt");
            return {std::move(s3_payload), lr};
        }
    }

    // Total failure
    lr.source = SuperblockSource::NONE;
    lr.error_message = "All superblock sources exhausted: both NVMe slots corrupt";
    if (s3_store == nullptr) {
        lr.error_message += ", S3 fallback not configured";
    } else {
        lr.error_message += ", S3 fallback also failed";
    }

    LOGERROR("{}", lr.error_message);
    return {nullptr, lr};
}

// ─── Write (CP path) ────────────────────────────────────────────────────────

void NvmeSuperblockManager::write(const sisl::byte_array& payload) {
    RELEASE_ASSERT(payload != nullptr, "Cannot write null superblock payload");
    RELEASE_ASSERT(payload->size() + sizeof(nvme_sb_slot_header) <= m_slot_size,
                   "Payload size {} + header {} exceeds slot_size {}",
                   payload->size(), sizeof(nvme_sb_slot_header), m_slot_size);

    ++m_current_seq;

    // Build the on-disk buffer: header + payload
    uint32_t total_size = sizeof(nvme_sb_slot_header) + payload->size();
    auto buf = sisl::make_byte_array(m_slot_size, 512 /* alignment */);
    std::memset(buf->bytes(), 0, m_slot_size);

    nvme_sb_slot_header hdr;
    hdr.magic = nvme_sb_slot_header::MAGIC;
    hdr.version = nvme_sb_slot_header::CURRENT_VERSION;
    hdr.sequence_number = m_current_seq;
    hdr.payload_size = static_cast< uint32_t >(payload->size());
    hdr.reserved = 0;
    hdr.checksum = compute_crc(hdr, payload->cbytes(), hdr.payload_size);

    std::memcpy(buf->bytes(), &hdr, sizeof(hdr));
    std::memcpy(buf->bytes() + sizeof(hdr), payload->cbytes(), payload->size());

    if (!m_active_slot.has_value()) {
        // First write ever (bootstrap or post-S3 recovery) — write both slots.
        LOGINFO("Bootstrap: writing superblock to BOTH NVMe slots (seq={})", m_current_seq);
        write_slot(m_slot_a_offset, buf->cbytes(), m_slot_size);
        write_slot(m_slot_b_offset, buf->cbytes(), m_slot_size);
        m_active_slot = 0;  // Both have the same data; mark A as "active"
    } else {
        // Ping-pong: write to the INACTIVE slot
        uint8_t target_slot = (*m_active_slot == 0) ? 1 : 0;
        uint64_t target_offset = (target_slot == 0) ? m_slot_a_offset : m_slot_b_offset;

        LOGDEBUG("Ping-pong write to Slot {} (seq={}, offset={})",
                 target_slot == 0 ? "A" : "B", m_current_seq, target_offset);

        write_slot(target_offset, buf->cbytes(), m_slot_size);
        m_active_slot = target_slot;
    }
}

} // namespace homestore
