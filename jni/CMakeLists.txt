cmake_minimum_required(VERSION 3.22.1 FATAL_ERROR)
cmake_policy(SET CMP0048 NEW)
project(leveldbjni VERSION 1.0.0 LANGUAGES C CXX)
message("${CMAKE_PROJECT_NAME} platform: ${ANDROID_PLATFORM} flavor: ${TGX_FLAVOR}")

if (NOT DEFINED ANDROID_MIN_SDK_VERSION)
  message(FATAL_ERROR "ANDROID_MIN_SDK_VERSION is not set")
endif()

# Common flags

add_compile_options(
  "$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-ffunction-sections>"
  "$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-fdata-sections>"
  "$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-fvisibility=hidden>"
  "$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-fvisibility-inlines-hidden>"
  "$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-flto=full>"
  "$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-fno-unwind-tables>"
  "$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-fno-asynchronous-unwind-tables>"
)

if (ANDROID_MIN_SDK_VERSION GREATER_EQUAL 23)
  add_link_options(
    "-Wl,--pack-dyn-relocs=android"
  )
endif()

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

target_link_options(leveldbjni PRIVATE
  "$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-Wl,--gc-sections,--icf=safe>"
  "$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-Wl,--exclude-libs,ALL>"
  "$<$<CONFIG:Release,RelWithDebInfo,MinSizeRel>:-flto=full>"
)

if (${ANDROID_ABI} STREQUAL "arm64-v8a" OR ${ANDROID_ABI} STREQUAL "x86_64")
  # Enable 16 KB ELF alignment.
  target_link_options(leveldbjni PRIVATE
    "-Wl,-z,max-page-size=16384"
  )
endif()

if (ANDROID_MIN_SDK_VERSION GREATER_EQUAL 23)
  target_link_options(leveldbjni PRIVATE
    "-Wl,--pack-dyn-relocs=android"
  )
endif()