include_directories (BEFORE ..)
include_directories (BEFORE .)

add_library(hs_s3 OBJECT)
target_sources(hs_s3 PRIVATE
    mock_s3_object_store.cpp
    s3_physical_dev.cpp
)
target_link_libraries(hs_s3 hs_common ${COMMON_DEPS})
