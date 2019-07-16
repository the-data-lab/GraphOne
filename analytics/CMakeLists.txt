
set(analytics_source_files
    communicator.cpp
    communicator.h
)

add_library(analytics32 ${analytics_source_files})
set_target_properties(analytics32 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS32}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})

add_library(analytics64 ${analytics_source_files})
set_target_properties(analytics64 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS64}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})
