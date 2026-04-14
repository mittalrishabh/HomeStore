include_directories (BEFORE ..)
include_directories (BEFORE .)

add_library(hs_s3 OBJECT)
target_sources(hs_s3 PRIVATE
    mock_s3_object_store.cpp
    nvme_superblock_fallback.cpp
)
target_link_libraries(hs_s3 hs_common ${COMMON_DEPS})

if (ENABLE_MINIO_TESTS)
    find_package(AWSSDK REQUIRED COMPONENTS s3)
    add_library(hs_s3_aws OBJECT)
    target_sources(hs_s3_aws PRIVATE
        aws_s3_object_store.cpp
    )
    target_include_directories(hs_s3_aws PRIVATE ${AWSSDK_INCLUDE_DIRS})
    target_link_libraries(hs_s3_aws hs_common ${COMMON_DEPS} ${AWSSDK_LINK_LIBRARIES})
endif()
