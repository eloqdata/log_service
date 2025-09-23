cmake_minimum_required(VERSION 3.8)
project(log_service C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(WITH_CLOUD_AZ_INFO "With Cloud Availability Zone Info" OFF)
message(NOTICE "With Cloud Availability Zone Info: ${WITH_CLOUD_AZ_INFO}")

if (WITH_CLOUD_AZ_INFO)
    add_definitions(-DWITH_CLOUD_AZ_INFO)
endif()

SET(LOG_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
SET(LOG_PROTO_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tx-log-protos)
message(NOTICE "log_service source dir: ${LOG_SOURCE_DIR}")

option(TEST_LOG_SERVICE "Test log service" ON)
message(NOTICE "TEST_LOG_SERVICE : ${TEST_LOG_SERVICE}")

option(BRPC_WITH_GLOG "With glog" ON)
message(NOTICE "BRPC_WITH_GLOG : ${BRPC_WITH_GLOG}")

option(LINK_TCMALLOC "Link tcmalloc if possible" OFF)
message(NOTICE "LINK_TCMALLOC : ${LINK_TCMALLOC}")

option(WITH_ASAN "Enable memory sanitize" OFF)
message(NOTICE "WITH ASAN: ${WITH_ASAN}")

option(DISABLE_CODE_LINE_IN_LOG "Enable DISABLE_CODE_LINE_IN_LOG" OFF)
message(NOTICE "DISABLE_CODE_LINE_IN_LOG : ${DISABLE_CODE_LINE_IN_LOG}")
if (DISABLE_CODE_LINE_IN_LOG)
    add_definitions(-DDISABLE_CODE_LINE_IN_LOG)
endif()

find_path(BRPC_INCLUDE_PATH NAMES brpc/stream.h)
find_library(BRPC_LIB NAMES brpc)
if ((NOT BRPC_INCLUDE_PATH) OR (NOT BRPC_LIB))
    message(FATAL_ERROR "Fail to find brpc")
endif ()
include_directories(${BRPC_INCLUDE_PATH})

find_path(BRAFT_INCLUDE_PATH NAMES braft/raft.h)
find_library(BRAFT_LIB NAMES braft)
if ((NOT BRAFT_INCLUDE_PATH) OR (NOT BRAFT_LIB))
    message(FATAL_ERROR "Fail to find braft")
endif ()
include_directories(${BRAFT_INCLUDE_PATH})

find_path(GFLAGS_INCLUDE_PATH gflags/gflags.h)
find_library(GFLAGS_LIBRARY NAMES gflags libgflags)
if ((NOT GFLAGS_INCLUDE_PATH) OR (NOT GFLAGS_LIBRARY))
    message(FATAL_ERROR "Fail to find gflags")
endif ()
include_directories(${GFLAGS_INCLUDE_PATH})

if (BRPC_WITH_GLOG)
    find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
    find_library(GLOG_LIB NAMES glog)
    if ((NOT GLOG_INCLUDE_PATH) OR (NOT GLOG_LIB))
        message(FATAL_ERROR "Fail to find glog")
    endif ()
    include_directories(${GLOG_INCLUDE_PATH})
    set(LOG_LIB ${LOG_LIB} ${GLOG_LIB})
endif ()

execute_process(
        COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
)
if (${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    execute_process(
            COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
            OUTPUT_VARIABLE GFLAGS_NS
    )
endif ()

if (LINK_TCMALLOC)
    find_path(GPERFTOOLS_INCLUDE_DIR NAMES gperftools/heap-profiler.h)
    find_library(GPERFTOOLS_LIBRARIES NAMES tcmalloc_and_profiler)
    if (GPERFTOOLS_INCLUDE_DIR AND GPERFTOOLS_LIBRARIES)
        set(CMAKE_CXX_FLAGS "-DBRPC_ENABLE_CPU_PROFILER")
        include_directories(${GPERFTOOLS_INCLUDE_DIR})
    else ()
        set(GPERFTOOLS_LIBRARIES "")
    endif ()
endif ()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CPP_FLAGS} -DGFLAGS_NS=${GFLAGS_NS} -O2 -D__const__=__unused__ -pipe -W -Wall -Wno-unused-parameter -fPIC -fno-omit-frame-pointer")
if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++11")
    endif ()
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++11")
    endif ()
else ()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif ()

if (WITH_ASAN)
# Enable AddressSanitizer
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")  
endif()

find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
find_library(LEVELDB_LIB NAMES leveldb)
if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
    message(FATAL_ERROR "Fail to find leveldb")
endif ()
include_directories(${LEVELDB_INCLUDE_PATH})

set(LOG_SHIPPING_THREADS_NUM 8)

if (OVERRIDE_GFLAGS_NAMESPACE)
    add_compile_definitions(OVERRIDE_GFLAGS_NAMESPACE)
endif ()

find_path(ROCKSDB_INCLUDE_PATH NAMES rocksdb/db.h)
if (NOT ROCKSDB_INCLUDE_PATH)
message(FATAL_ERROR "Fail to find RocksDB include path")
endif ()
message(STATUS "ROCKSDB_INCLUDE_PATH: ${ROCKSDB_INCLUDE_PATH}")

find_library(ROCKSDB_LIB NAMES rocksdb)
if (NOT ROCKSDB_LIB)
  message(FATAL_ERROR "Fail to find RocksDB lib path")
endif ()
message(STATUS "ROCKSDB_LIB: ${ROCKSDB_LIB}")

set(LOG_INCLUDE_DIR
        ${LOG_INCLUDE_DIR}
        ${ROCKSDB_INCLUDE_PATH}
        )
set(LOG_LIB
        ${LOG_LIB}
        ${ROCKSDB_LIB}
        )

# add preprocessor definition USE_ROCKSDB_LOG_STATE
add_compile_definitions(USE_ROCKSDB_LOG_STATE)
# one shipping thread is enough for rocksdb version log state
set(LOG_SHIPPING_THREADS_NUM 1)


find_package(Protobuf REQUIRED)

set(PROTO_SRC ${LOG_PROTO_SOURCE_DIR})
set(PROTO_NAME log)
execute_process(
	COMMAND protoc ./${PROTO_NAME}.proto --cpp_out=./
	WORKING_DIRECTORY ${PROTO_SRC}
)

set(LOG_INCLUDE_DIR
        ${LOG_INCLUDE_DIR}
        ${LOG_SOURCE_DIR}/include
	${LOG_PROTO_SOURCE_DIR}
        )

set(LOG_LIB
        ${LOG_LIB}
        ${CMAKE_THREAD_LIBS_INIT}
        ${GFLAGS_LIBRARY}
        ${PROTOBUF_LIBRARY}
        ${GPERFTOOLS_LIBRARIES}
        ${LEVELDB_LIB}
        ${BRAFT_LIB}
        ${BRPC_LIB}
        dl
        z
        )

add_library(logservice STATIC
        ${LOG_PROTO_SOURCE_DIR}/log.pb.cc
        ${LOG_PROTO_SOURCE_DIR}/log_agent.cpp
        ${LOG_SOURCE_DIR}/src/log_server.cpp
        ${LOG_SOURCE_DIR}/src/log_state_rocksdb_impl.cpp
        ${LOG_SOURCE_DIR}/src/fault_inject.cpp
        ${LOG_SOURCE_DIR}/src/INIReader.cpp
        ${LOG_SOURCE_DIR}/src/ini.c
        ${LOG_SOURCE_DIR}/src/open_log_service.cpp
        ${LOG_SOURCE_DIR}/src/open_log_task.cpp)

target_compile_definitions(logservice PUBLIC LOG_SHIPPING_THREADS_NUM=${LOG_SHIPPING_THREADS_NUM})
target_include_directories(logservice PUBLIC ${LOG_INCLUDE_DIR})

target_link_libraries(logservice PUBLIC ${LOG_LIB})

if (TEST_LOG_SERVICE)
    add_executable(launch_sv ${LOG_SOURCE_DIR}/src/launch_sv.cpp)
    target_link_libraries(launch_sv PUBLIC logservice)

    add_executable(launch_replay_service ${LOG_SOURCE_DIR}/test/launch_replay_service.cpp)
    target_link_libraries(launch_replay_service PUBLIC logservice)

    add_executable(async_write_log_test ${LOG_SOURCE_DIR}/test/async_write_log_test.cpp)
    target_link_libraries(async_write_log_test PUBLIC logservice)

    if (USE_ROCKSDB_LOG_STATE)
       add_executable(rocksdb_test ${LOG_SOURCE_DIR}/test/rocksdb_test.cpp)
       target_link_libraries(rocksdb_test PUBLIC logservice)
    endif ()
endif ()
