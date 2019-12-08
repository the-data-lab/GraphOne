cmake_minimum_required(VERSION 2.8 FATAL_ERROR)
project(graphone)

#set(CMAKE_CXX_STANDARD 17)  # enable C++11 standard
#set(CMAKE_CXX_STANDARD_REQUIRED ON)
#set(CMAKE_CXX_EXTENSIONS ON)

set(COMPILE_FLAGS_COMMON "-std=c++1y -O3 -g -DOVER_COMMIT -DTBB -DPLAIN_GRAPH -Wno-unused-result -fopenmp")
#set(COMPILE_FLAGS_COMMON "-std=gnu++1z -g -DOVER_COMMIT -DPLAIN_GRAPH -fopenmp")

set(COMPILE_FLAGS32 "${COMPILE_FLAGS_COMMON} -DB32")
set(COMPILE_FLAGS64 "${COMPILE_FLAGS_COMMON} -DB64")
set(LINK_FLAGS_COMMON "-fopenmp")

include_directories(include)
include_directories(src)
include_directories(gview)
include_directories(onedata)
include_directories(analytics)
include_directories(test)

add_subdirectory(src)
add_subdirectory(onedata)
add_subdirectory(test)


set(graphone_source_files main.cpp)
add_executable(graphone32 ${graphone_source_files})
set_target_properties(graphone32 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS32}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})
#target_link_libraries(graphone32 src32 onedata32 test32)
target_link_libraries(graphone32 src32 onedata32 test32 -ltbb)
#target_link_libraries(graphone32 src32 onedata32 test32 -ltbb -ltcmalloc_minimal)

add_executable(graphone64 ${graphone_source_files})
set_target_properties(graphone64 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS64}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})
#target_link_libraries(graphone64 src64 onedata64 test64)
target_link_libraries(graphone64 src64 onedata64 test64 -ltbb)
#target_link_libraries(graphone64 src64 onedata64 test64 -ltbb -ltcmalloc_minimal)
