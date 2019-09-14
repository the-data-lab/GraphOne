cmake_minimum_required(VERSION 2.8 FATAL_ERROR)
project(graphone)

find_package(MPI REQUIRED)

#set(CMAKE_CXX_STANDARD 17)  # enable C++11 standard
#set(CMAKE_CXX_STANDARD_REQUIRED ON)
#set(CMAKE_CXX_EXTENSIONS ON)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DB32 -DTBB -Wall -O3 -fopenmp -DOVER_COMMIT -ltbb")

#set(COMPILE_FLAGS_COMMON "-std=c++1y -O3 -g -DOVER_COMMIT -DTBB -DPLAIN_GRAPH -fopenmp")
set(COMPILE_FLAGS_COMMON "-std=c++1y -O3 -g -DOVER_COMMIT -DTBB -DPLAIN_GRAPH -D_MPI -fopenmp")
#set(COMPILE_FLAGS_COMMON "-std=gnu++1z -g -DOVER_COMMIT -DPLAIN_GRAPH -D_MPI -fopenmp")
#set(COMPILE_FLAGS_COMMON "-std=gnu++1z -g -DOVER_COMMIT -DPLAIN_GRAPH -fopenmp")

set(COMPILE_FLAGS32 "${COMPILE_FLAGS_COMMON} -DB32")
set(COMPILE_FLAGS64 "${COMPILE_FLAGS_COMMON} -DB64")
set(LINK_FLAGS_COMMON "-ltbb -fopenmp")
#set(LINK_FLAGS_COMMON "-ltbb -fopenmp -ltcmalloc_minimal")

include_directories(SYSTEM ${MPI_INCLUDE_PATH})
include_directories(include)
include_directories(src)
include_directories(gview)
include_directories(onedata)
include_directories(analytics)

add_subdirectory(src)
add_subdirectory(onedata)
add_subdirectory(analytics)
add_subdirectory(test)


set(graphone_source_files main.cpp)
add_executable(graphone32 ${graphone_source_files})
set_target_properties(graphone32 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS32}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})
target_link_libraries(graphone32 src32 onedata32 test32 ${MPI_CXX_LIBRARIES})

add_executable(graphone64 ${graphone_source_files})
set_target_properties(graphone64 PROPERTIES  COMPILE_FLAGS ${COMPILE_FLAGS64}
                      LINK_FLAGS ${LINK_FLAGS_COMMON})
target_link_libraries(graphone64 src64 onedata64 test64 ${MPI_CXX_LIBRARIES})
