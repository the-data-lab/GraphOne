set(src_source_files
    bitmap.h
    cf_info.cpp
    cf_info.h
    CMakeLists.txt
    edge_sharding.h
    enumkv.cpp
    enumkv.h
    graph_base.h
    graph.cpp
    graph.h
    graph_view.h
    labelkv.h
    mem_iterative_analytics.h
    mixkv.h
    new_func.h
    new_type.h
    numberkv.h
    num.h
    onegraph.h
    onekv.h
    prop_encoder.cpp
    prop_encoder.h
    sgraph2.h
    sgraph.cpp
    sgraph.h
    skv.h
    sstream_view.h
    static_view.h
    str2sid.h
    str.cpp
    stream_analytics.h
    stream_view.h
    str.h
    stringkv.cpp
    stringkv.h
    type.h
    typekv.cpp
    typekv.h
    wgraph.h
    wsstream_view.h
    wtime.h
)

add_library(src32 ${src_source_files})
set_target_properties(src32 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS32}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})

add_library(src64 ${src_source_files})
set_target_properties(src64 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS64}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})
