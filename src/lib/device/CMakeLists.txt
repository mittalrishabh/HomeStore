include_directories (BEFORE ..)
include_directories (BEFORE .)

add_library(hs_device OBJECT)
target_sources(hs_device PRIVATE
      physical_dev.cpp
      device_manager.cpp
      virtual_dev.cpp
      journal_vdev.cpp
      chunk.cpp
      round_robin_chunk_selector.cpp
      vchunk.cpp
      s3/full_chunk_store.cpp
      s3/pdev_s3_superblock.cpp
      s3/s3_physical_dev.cpp
      s3/s3_cp.cpp
      s3/tiered_read_handler.cpp
      s3/s3_recovery.cpp
      s3/on_demand_recovery.cpp
      s3/chunk_hydration_manager.cpp
    )
target_link_libraries(hs_device hs_common ${COMMON_DEPS})
