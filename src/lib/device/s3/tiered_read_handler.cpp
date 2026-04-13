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

#include <chrono>

#include <homestore/s3/chunk_hydration_manager.h>
#include <homestore/s3/tiered_read_handler.h>

SISL_LOGGING_DECL(s3)

namespace homestore {

TieredReadHandler::TieredReadHandler(S3PhysicalDev* s3_pdev,
                                     std::shared_ptr< NvmeDeviceIO > nvme_io,
                                     TieredReadConfig config,
                                     std::shared_ptr< ChunkHydrationManager > hydration_mgr)
    : m_s3_pdev{s3_pdev}, m_nvme_io{std::move(nvme_io)}, m_hydration_mgr{std::move(hydration_mgr)}, m_config{config} {
    RELEASE_ASSERT(m_s3_pdev != nullptr, "S3PhysicalDev must not be null");
    RELEASE_ASSERT(m_nvme_io != nullptr, "NvmeDeviceIO must not be null");
    LOGDEBUGMOD(s3, "TieredReadHandler created: fallback={} hydrate={}",
                config.s3_read_fallback_enabled, config.s3_hydrate_on_read);
}

folly::Future< std::pair< std::error_code, sisl::byte_array > >
TieredReadHandler::async_read(chunk_id_t chunk_id, uint64_t offset_in_chunk, uint64_t size) {
    // Snapshot config once to avoid races with set_config()
    auto cfg = config();

    // Fast path: chunk is on NVMe
    if (m_nvme_io->is_chunk_on_nvme(chunk_id)) {
        auto start = std::chrono::steady_clock::now();

        auto buf = sisl::make_byte_array(static_cast< uint32_t >(size), 0);
        auto ec = m_nvme_io->nvme_read(chunk_id, offset_in_chunk,
                                        reinterpret_cast< char* >(buf->bytes()), size);

        auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
            std::chrono::steady_clock::now() - start).count();
        HISTOGRAM_OBSERVE(m_metrics, nvme_read_latency_us, elapsed_us);

        if (!ec) {
            COUNTER_INCREMENT(m_metrics, nvme_read_count, 1);
            LOGTRACEMOD(s3, "Tiered read: NVMe hit chunk_id={} offset={} size={} ({}us)",
                        chunk_id, offset_in_chunk, size, elapsed_us);
            return folly::makeFuture(std::make_pair(std::error_code{}, std::move(buf)));
        }

        // NVMe read failed — if S3 fallback disabled, return error
        if (!cfg.s3_read_fallback_enabled) {
            LOGERRORMOD(s3, "Tiered read: NVMe read failed for chunk_id={}, S3 fallback disabled", chunk_id);
            COUNTER_INCREMENT(m_metrics, tiered_read_errors, 1);
            return folly::makeFuture(std::make_pair(ec, sisl::byte_array{}));
        }

        LOGDEBUGMOD(s3, "Tiered read: NVMe read failed for chunk_id={}, falling through to S3", chunk_id);
    }

    // S3 fallback path
    if (!cfg.s3_read_fallback_enabled) {
        LOGERRORMOD(s3, "Tiered read: chunk_id={} not on NVMe and S3 fallback disabled", chunk_id);
        COUNTER_INCREMENT(m_metrics, tiered_read_errors, 1);
        return folly::makeFuture(std::make_pair(
            std::make_error_code(std::errc::no_such_device), sisl::byte_array{}));
    }

    auto start = std::chrono::steady_clock::now();

    // Read from S3 via S3PhysicalDev → ChunkStore::get()
    return m_s3_pdev->read(chunk_id, offset_in_chunk, size).thenValue(
        [this, chunk_id, offset_in_chunk, size, start, cfg](
            std::pair< S3Result, sisl::byte_array >&& result) mutable
            -> std::pair< std::error_code, sisl::byte_array > {

            auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
                std::chrono::steady_clock::now() - start).count();
            HISTOGRAM_OBSERVE(m_metrics, s3_read_fallback_latency_us, elapsed_us);

            auto& [s3_result, data] = result;

            if (!s3_result.ok()) {
                LOGERRORMOD(s3, "Tiered read: S3 fallback failed for chunk_id={}: {}",
                            chunk_id, s3_result.error_message);
                COUNTER_INCREMENT(m_metrics, tiered_read_errors, 1);
                return {std::make_error_code(std::errc::io_error), sisl::byte_array{}};
            }

            COUNTER_INCREMENT(m_metrics, s3_read_fallback_count, 1);
            LOGDEBUGMOD(s3, "Tiered read: S3 fallback hit chunk_id={} offset={} size={} ({}us)",
                        chunk_id, offset_in_chunk, size, elapsed_us);

            // Optional: hydrate to NVMe for fast subsequent reads
            if (cfg.s3_hydrate_on_read && data) {
                if (m_hydration_mgr) {
                    // Async chunk-level hydration — non-blocking, returns immediately
                    auto chunk_entry = m_s3_pdev->superblock().find_chunk(chunk_id);
                    uint64_t cs = chunk_entry ? chunk_entry->chunk_size : data->size();
                    m_hydration_mgr->schedule_hydration(chunk_id, cs);
                } else {
                    // Fallback: inline block-level write (legacy path)
                    auto hydrate_start = std::chrono::steady_clock::now();
                    auto write_ec = m_nvme_io->nvme_write(
                        chunk_id, offset_in_chunk,
                        reinterpret_cast< const char* >(data->cbytes()), data->size());
                    auto hydrate_us = std::chrono::duration_cast< std::chrono::microseconds >(
                        std::chrono::steady_clock::now() - hydrate_start).count();
                    HISTOGRAM_OBSERVE(m_metrics, s3_hydrate_latency_us, hydrate_us);

                    if (!write_ec) {
                        COUNTER_INCREMENT(m_metrics, s3_hydrate_count, 1);
                        LOGDEBUGMOD(s3, "Tiered read: hydrated chunk_id={} offset={} size={} to NVMe ({}us)",
                                    chunk_id, offset_in_chunk, data->size(), hydrate_us);
                    } else {
                        COUNTER_INCREMENT(m_metrics, s3_hydrate_failures, 1);
                        LOGWARNMOD(s3, "Tiered read: NVMe hydration failed for chunk_id={}: {}",
                                   chunk_id, write_ec.message());
                    }
                }
            }

            return {std::error_code{}, std::move(data)};
        });
}

std::error_code TieredReadHandler::sync_read(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                                              char* buf, uint64_t size) {
    auto cfg = config();

    // Fast path: NVMe
    if (m_nvme_io->is_chunk_on_nvme(chunk_id)) {
        auto start = std::chrono::steady_clock::now();
        auto ec = m_nvme_io->nvme_read(chunk_id, offset_in_chunk, buf, size);
        auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
            std::chrono::steady_clock::now() - start).count();
        HISTOGRAM_OBSERVE(m_metrics, nvme_read_latency_us, elapsed_us);

        if (!ec) {
            COUNTER_INCREMENT(m_metrics, nvme_read_count, 1);
            return {};
        }

        if (!cfg.s3_read_fallback_enabled) {
            COUNTER_INCREMENT(m_metrics, tiered_read_errors, 1);
            return ec;
        }
    } else if (!cfg.s3_read_fallback_enabled) {
        COUNTER_INCREMENT(m_metrics, tiered_read_errors, 1);
        return std::make_error_code(std::errc::no_such_device);
    }

    // S3 fallback (blocking)
    auto [ec, data] = read_from_s3_and_hydrate(chunk_id, offset_in_chunk, size);
    if (!ec && data) {
        auto copy_sz = std::min(static_cast< uint64_t >(data->size()), size);
        std::memcpy(buf, data->cbytes(), copy_sz);
        if (copy_sz < size) {
            std::memset(buf + copy_sz, 0, size - copy_sz);
        }
    }
    return ec;
}

std::error_code TieredReadHandler::hydrate(chunk_id_t chunk_id, uint64_t offset_in_chunk, uint64_t size) {
    LOGDEBUGMOD(s3, "Force-hydrating chunk_id={} offset={} size={}", chunk_id, offset_in_chunk, size);

    // Read from S3
    auto [s3_result, data] = m_s3_pdev->read(chunk_id, offset_in_chunk, size).get();
    if (!s3_result.ok()) {
        LOGERRORMOD(s3, "Hydration failed: S3 read error for chunk_id={}: {}", chunk_id, s3_result.error_message);
        return std::make_error_code(std::errc::io_error);
    }

    if (!data) {
        return std::make_error_code(std::errc::no_message_available);
    }

    // Write to NVMe
    auto write_ec = m_nvme_io->nvme_write(
        chunk_id, offset_in_chunk,
        reinterpret_cast< const char* >(data->cbytes()), data->size());

    if (!write_ec) {
        COUNTER_INCREMENT(m_metrics, s3_hydrate_count, 1);
        LOGDEBUGMOD(s3, "Hydrated chunk_id={} offset={} size={} to NVMe", chunk_id, offset_in_chunk, size);
    } else {
        COUNTER_INCREMENT(m_metrics, s3_hydrate_failures, 1);
        LOGERRORMOD(s3, "Hydration NVMe write failed for chunk_id={}: {}", chunk_id, write_ec.message());
    }
    return write_ec;
}

std::pair< std::error_code, sisl::byte_array >
TieredReadHandler::read_from_s3_and_hydrate(chunk_id_t chunk_id, uint64_t offset_in_chunk, uint64_t size) {
    auto start = std::chrono::steady_clock::now();

    // NOTE: .get() blocks the calling thread. This is the sync_read path,
    // so blocking is expected.
    auto [s3_result, data] = m_s3_pdev->read(chunk_id, offset_in_chunk, size).get();

    auto elapsed_us = std::chrono::duration_cast< std::chrono::microseconds >(
        std::chrono::steady_clock::now() - start).count();
    HISTOGRAM_OBSERVE(m_metrics, s3_read_fallback_latency_us, elapsed_us);

    if (!s3_result.ok()) {
        LOGERRORMOD(s3, "Tiered sync_read: S3 fallback failed for chunk_id={}: {}",
                    chunk_id, s3_result.error_message);
        COUNTER_INCREMENT(m_metrics, tiered_read_errors, 1);
        return {std::make_error_code(std::errc::io_error), sisl::byte_array{}};
    }

    COUNTER_INCREMENT(m_metrics, s3_read_fallback_count, 1);

    // Optional hydration
    auto cfg = config();
    if (cfg.s3_hydrate_on_read && data) {
        if (m_hydration_mgr) {
            auto chunk_entry = m_s3_pdev->superblock().find_chunk(chunk_id);
            uint64_t cs = chunk_entry ? chunk_entry->chunk_size : data->size();
            m_hydration_mgr->schedule_hydration(chunk_id, cs);
        } else {
            auto hydrate_start = std::chrono::steady_clock::now();
            auto write_ec = m_nvme_io->nvme_write(
                chunk_id, offset_in_chunk,
                reinterpret_cast< const char* >(data->cbytes()), data->size());
            auto hydrate_us = std::chrono::duration_cast< std::chrono::microseconds >(
                std::chrono::steady_clock::now() - hydrate_start).count();
            HISTOGRAM_OBSERVE(m_metrics, s3_hydrate_latency_us, hydrate_us);

            if (!write_ec) {
                COUNTER_INCREMENT(m_metrics, s3_hydrate_count, 1);
            } else {
                COUNTER_INCREMENT(m_metrics, s3_hydrate_failures, 1);
                LOGWARNMOD(s3, "Tiered sync_read: hydration failed for chunk_id={}", chunk_id);
            }
        }
    }

    return {std::error_code{}, std::move(data)};
}

} // namespace homestore
