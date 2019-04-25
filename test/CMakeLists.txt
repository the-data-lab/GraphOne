set(test_source_files
    all.h
    csv_to_edge.cpp
    csv_to_edge.h
    lanl15_test.cpp
    multi_graph.cpp
    multi_graph.h
    multigraph_test.cpp
    nt_to_edge.cpp
    nt_to_edge.h
    plaingraph_test.cpp
    plain_to_edge.h
    util.cpp
    util.h
)

set(test_source64_files
    ${test_source_files}
    ldbc_test.cpp
    lubm_test.cpp
)


add_library(test32 ${test_source_files})
set_target_properties(test32 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS32}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})
target_link_libraries(test32 src32)
add_library(test64 ${test_source64_files})
set_target_properties(test64 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS64}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})
target_link_libraries(test64 src64)
