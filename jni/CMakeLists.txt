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

file(REAL_PATH "${ANDROID_NDK}" ANDROID_NDK_REAL_PATH)

add_compile_options(
  "-ffile-prefix-map=${PROJECT_SOURCE_DIR}/="
  "-ffile-prefix-map=${ANDROID_NDK}/=ndk/"
  "-ffile-prefix-map=${ANDROID_NDK_REAL_PATH}/=ndk/"
  "-ffile-prefix-map=${CMAKE_BINARY_DIR}=.cxx"
)

if (ANDROID_MIN_SDK_VERSION GREATER_EQUAL 30)
  add_link_options(
    "-Wl,--pack-dyn-relocs=android+relr"
    "-Wl,--no-use-android-relr-tags"
  )
elseif (ANDROID_MIN_SDK_VERSION GREATER_EQUAL 28)
  add_link_options(
    "-Wl,--pack-dyn-relocs=android+relr"
    "-Wl,--use-android-relr-tags"
  )
elseif (ANDROID_MIN_SDK_VERSION GREATER_EQUAL 23)
  add_link_options(
    "-Wl,--pack-dyn-relocs=android"
  )
endif()

# Build leveldb

set(LEVELDB_SRC_DIR "leveldb")
set(LEVELDB_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/leveldb")

set(LEVELDB_BUILD_BENCHMARKS OFF CACHE BOOL "Build LevelDB's benchmarks" FORCE)
set(LEVELDB_BUILD_TESTS OFF CACHE BOOL "Build LevelDB's unit tests" FORCE)
set(LEVELDB_INSTALL OFF CACHE BOOL "Install LevelDB's header and library" FORCE)
add_subdirectory(
  ${LEVELDB_SRC_DIR}
  ${LEVELDB_BUILD_DIR}
  EXCLUDE_FROM_ALL
)

# Build jni-utils

add_subdirectory(
  jni-utils
)

# Build shared library

add_library(leveldbjni SHARED
  db_jni.cpp
)

target_link_libraries(leveldbjni
  leveldb
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