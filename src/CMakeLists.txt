set(src_source_files
    cf_info.cpp
    cf_info.h
    edge_sharding.h
    enumkv.cpp
    enumkv.h
    graph.cpp
    graph.h
    mixkv.h
    new_func.h
    new_type.h
    numberkv.h
    prop_encoder.cpp
    prop_encoder.h
    sgraph2.h
    sgraph.cpp
    sgraph.h
    skv.h
    str2sid.h
    str.h
    stringkv.cpp
    stringkv.h
    type.h
    typekv.cpp
    typekv.h
    wgraph.h
    wtime.h
)

add_library(src32 ${src_source_files})
set_target_properties(src32 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS32}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})

add_library(src64 ${src_source_files})
set_target_properties(src64 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS64}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})
