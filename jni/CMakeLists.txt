#
# This file is a part of LevelDB Preferences
# Copyright © Vyacheslav Krylov (slavone@protonmail.ch) 2014-2021
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

set_target_properties( leveldb-lib PROPERTIES
  IMPORTED_LOCATION ${LEVELDB_BUILD_DIR}/libleveldb.a
)

# Build jni-utils

add_subdirectory(
  jni-utils
)

# Build shared library

add_library(leveldbjni SHARED
  db_jni.cpp
)

include_directories(
  ${LEVELDB_SRC_DIR}/include
  ${LEVELDB_BUILD_DIR}/include
  jni-utils/include
)

target_link_libraries(leveldbjni
  leveldb-lib
  jni-utils
  android
  log
)