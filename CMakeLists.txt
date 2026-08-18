cmake_minimum_required(VERSION 3.16)
project(distdb CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Debug)
endif()

if(MINGW)
    # Statically link the MinGW runtime (libstdc++/libgcc/libwinpthread)
    # so the built .exe runs standalone, regardless of whether the
    # toolchain's bin directory happens to be on PATH for whatever shell
    # launches it - which matters a lot for the raft executable, since
    # tests launch several copies of it as independent background
    # processes.
    add_link_options(-static)
endif()

add_library(distdb_storage
    src/storage/crc32.cpp
    src/storage/wal.cpp
    src/storage/memtable.cpp
    src/storage/sstable.cpp
    src/storage/engine.cpp
)
target_include_directories(distdb_storage PUBLIC src)

add_library(distdb_sql
    src/sql/lexer.cpp
    src/sql/parser.cpp
    src/sql/schema.cpp
    src/sql/executor.cpp
)
target_include_directories(distdb_sql PUBLIC src)
target_link_libraries(distdb_sql PUBLIC distdb_storage)

add_executable(distdb src/main.cpp)
target_link_libraries(distdb PRIVATE distdb_storage distdb_sql)

find_package(Threads REQUIRED)

add_library(distdb_raft
    src/raft/message.cpp
    src/raft/transport.cpp
    src/raft/persistent_state.cpp
    src/raft/log.cpp
    src/raft/conf_change.cpp
    src/raft/membership_state.cpp
    src/raft/node.cpp
    src/raft/client.cpp
)
target_include_directories(distdb_raft PUBLIC src)
target_link_libraries(distdb_raft PUBLIC Threads::Threads)
if(WIN32)
    target_link_libraries(distdb_raft PUBLIC ws2_32)
endif()

add_library(distdb_shard
    src/shard/routing.cpp
)
target_include_directories(distdb_shard PUBLIC src)
target_link_libraries(distdb_shard PUBLIC distdb_raft)

add_library(distdb_txn
    src/txn/txn.cpp
    src/txn/participant.cpp
)
target_include_directories(distdb_txn PUBLIC src)

add_executable(raftnode src/raft_main.cpp)
target_link_libraries(raftnode PRIVATE distdb_raft distdb_storage distdb_sql distdb_shard distdb_txn)
