#
# This file is a part of LevelDB Preferences
# Copyright © Vyacheslav Krylov (slavone@protonmail.ch) 2014
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# File created on 18/01/2021
#

cmake_minimum_required(VERSION 3.9 FATAL_ERROR)
cmake_policy(SET CMP0048 NEW)
project(leveldbjni VERSION 1.0.0 LANGUAGES C CXX)

# Common flags

set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--gc-sections,--icf=safe -Wl,--build-id=none")

# Build leveldb

set(LEVELDB_SRC_DIR "leveldb")
set(LEVELDB_BUILD_DIR "${PROJECT_SOURCE_DIR}/leveldb/out/${ANDROID_ABI}")

file(MAKE_DIRECTORY ${LEVELDB_BUILD_DIR})

set(LEVELDB_BUILD_BENCHMARKS OFF CACHE BOOL "Build LevelDB's benchmarks" FORCE)
set(LEVELDB_BUILD_TESTS OFF CACHE BOOL "Build LevelDB's unit tests" FORCE)
add_subdirectory(
  ${LEVELDB_SRC_DIR}
  ${LEVELDB_BUILD_DIR}
)
add_library(leveldb-lib STATIC IMPORTED)
set_target_properties( leveldb-lib PROPERTIES IMPORTED_LOCATION
  ${LEVELDB_BUILD_DIR}/libleveldb.a
)
if(${CMAKE_VERSION} VERSION_GREATER_EQUAL "3.11.0")
  target_include_directories(leveldb-lib INTERFACE
    ${LEVELDB_BUILD_DIR}/include
    ${LEVELDB_SRC_DIR}/include
  )
endif()

# Build jni-utils

add_subdirectory(
  jni-utils
)

# Build shared library

add_library(leveldbjni SHARED
  db_jni.cpp
)
if(${CMAKE_VERSION} VERSION_LESS "3.11.0")
  target_include_directories(leveldbjni PRIVATE
    ${LEVELDB_SRC_DIR}/include
    ${LEVELDB_BUILD_DIR}/include
  )
endif()

target_link_libraries(leveldbjni
  leveldb-lib
  jni-utils
  android
  log
)

if (${ANDROID_ABI} STREQUAL "arm64-v8a" OR ${ANDROID_ABI} STREQUAL "x86_64")
  # Enable 16 KB ELF alignment.
  target_link_options(leveldbjni PRIVATE
    "-Wl,-z,max-page-size=16384"
  )
endif()