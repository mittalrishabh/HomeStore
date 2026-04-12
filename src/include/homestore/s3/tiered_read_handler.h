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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include <folly/futures/Future.h>
#include <sisl/fds/buffer.hpp>
#include <sisl/logging/logging.h>
#include <sisl/metrics/metrics.hpp>

#include <homestore/s3/chunk_store.h>
#include <homestore/s3/s3_physical_dev.h>

namespace homestore {

/**
 * @brief Configuration for tiered read behavior.
 */
struct TieredReadConfig {
    bool s3_read_fallback_enabled{true};   ///< Enable S3 fallback when NVMe read fails
    bool s3_hydrate_on_read{true};         ///< Write S3 data back to NVMe after fetch
};

/**
 * @brief Metrics for tiered read operations.
 */
class TieredReadMetrics : public sisl::MetricsGroupWrapper {
public:
    explicit TieredReadMetrics() : sisl::MetricsGroupWrapper{"TieredRead", "tiered_read"} {
        REGISTER_COUNTER(s3_read_fallback_count, "Reads served from S3 fallback (NVMe miss)");
        REGISTER_COUNTER(s3_hydrate_count, "Chunks hydrated to NVMe after S3 read");
        REGISTER_COUNTER(s3_hydrate_failures, "Failed NVMe hydration attempts");
        REGISTER_COUNTER(nvme_read_count, "Reads served from NVMe (fast path)");
        REGISTER_COUNTER(tiered_read_errors, "Total tiered read errors");

        REGISTER_HISTOGRAM(s3_read_fallback_latency_us, "S3 fallback read latency in us",
                           HistogramBucketsType(OpLatecyBuckets));
        REGISTER_HISTOGRAM(nvme_read_latency_us, "NVMe read latency in us",
                           HistogramBucketsType(OpLatecyBuckets));

        register_me_to_farm();
    }

    ~TieredReadMetrics() { deregister_me_from_farm(); }
};

/**
 * @brief Callback interface for NVMe I/O operations used by the tiered read handler.
 *
 * Isolates TieredReadHandler from direct PhysicalDev coupling so it can be
 * tested independently.
 */
class NvmeDeviceIO {
public:
    virtual ~NvmeDeviceIO() = default;

    /// @brief Read data from NVMe at the given chunk + offset.
    /// @return error_code: success (no error) or failure code
    virtual std::error_code nvme_read(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                                      char* buf, uint64_t size) = 0;

    /// @brief Write data to NVMe at the given chunk + offset (for hydration).
    /// @return error_code: success or failure
    virtual std::error_code nvme_write(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                                       const char* buf, uint64_t size) = 0;

    /// @brief Check if a chunk is currently on NVMe (not evicted).
    virtual bool is_chunk_on_nvme(chunk_id_t chunk_id) const = 0;
};

/**
 * @brief TieredReadHandler — NVMe-first, S3-fallback read path.
 *
 * This handler implements the tiered read logic for HomeStore's S3-native
 * design:
 *
 * 1. **NVMe fast path**: If the chunk is on NVMe, read directly (fast).
 * 2. **S3 fallback**: If the chunk is S3-only (evicted or never hydrated),
 *    fetch from S3 via S3PhysicalDev::read() → ChunkStore::get().
 * 3. **Hydration**: After S3 fetch, optionally write the data back to NVMe
 *    so subsequent reads are fast. This is "cache-on-read" behavior.
 *
 * ## Integration Point:
 *
 * VirtualDev's read path calls into TieredReadHandler instead of directly
 * calling PhysicalDev::async_read(). The handler decides which device to
 * use based on chunk placement.
 *
 * ## Thread Safety:
 *
 * All operations are thread-safe. The handler is stateless except for
 * metrics and configuration.
 */
class TieredReadHandler {
public:
    /**
     * @brief Construct a TieredReadHandler.
     *
     * @param s3_pdev       S3PhysicalDev for S3 fallback reads
     * @param nvme_io       NVMe device I/O interface
     * @param config        Tiered read configuration
     */
    TieredReadHandler(S3PhysicalDev* s3_pdev,
                      std::shared_ptr< NvmeDeviceIO > nvme_io,
                      TieredReadConfig config = {});

    ~TieredReadHandler() = default;

    // Non-copyable
    TieredReadHandler(const TieredReadHandler&) = delete;
    TieredReadHandler& operator=(const TieredReadHandler&) = delete;

    /**
     * @brief Read data using tiered strategy: NVMe first, S3 fallback.
     *
     * If the chunk is on NVMe, reads directly. If not (evicted/S3-only),
     * falls back to S3 via ChunkStore::get(). Optionally hydrates to NVMe.
     *
     * @param chunk_id          Target chunk
     * @param offset_in_chunk   Byte offset within the chunk
     * @param size              Number of bytes to read
     * @return Future resolving to (error_code, data). Error code is
     *         success (default) on success, or an error on failure.
     */
    folly::Future< std::pair< std::error_code, sisl::byte_array > >
    async_read(chunk_id_t chunk_id, uint64_t offset_in_chunk, uint64_t size);

    /**
     * @brief Synchronous tiered read into a pre-allocated buffer.
     *
     * @param chunk_id          Target chunk
     * @param offset_in_chunk   Byte offset within the chunk
     * @param buf               Pre-allocated buffer (must be >= size bytes)
     * @param size              Number of bytes to read
     * @return error_code: success or failure
     */
    std::error_code sync_read(chunk_id_t chunk_id, uint64_t offset_in_chunk,
                              char* buf, uint64_t size);

    /**
     * @brief Force-hydrate a chunk from S3 to NVMe.
     *
     * Downloads the chunk range from S3 and writes to NVMe.
     * Useful for pre-warming the NVMe cache.
     *
     * @param chunk_id          Target chunk
     * @param offset_in_chunk   Start offset
     * @param size              Size to hydrate
     * @return error_code: success or failure
     */
    std::error_code hydrate(chunk_id_t chunk_id, uint64_t offset_in_chunk, uint64_t size);

    /// Update configuration at runtime
    void set_config(TieredReadConfig config) { m_config = config; }
    TieredReadConfig config() const { return m_config; }

    /// Accessors
    TieredReadMetrics& metrics() { return m_metrics; }
    S3PhysicalDev* s3_pdev() { return m_s3_pdev; }

private:
    /// S3 fallback read + optional hydration
    std::pair< std::error_code, sisl::byte_array >
    read_from_s3_and_hydrate(chunk_id_t chunk_id, uint64_t offset_in_chunk, uint64_t size);

    S3PhysicalDev* m_s3_pdev;
    std::shared_ptr< NvmeDeviceIO > m_nvme_io;
    TieredReadConfig m_config;
    TieredReadMetrics m_metrics;
};

} // namespace homestore
