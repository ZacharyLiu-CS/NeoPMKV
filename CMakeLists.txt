#1. specify the version of cmake
cmake_minimum_required(VERSION 3.10)

#2. define the name of project
project(ycsbc)

#3. specify c++ compiler version
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-std=c++11 -pthread")

if( CMAKE_BUILD_TYPE STREQUAL "Debug" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
    add_compile_definitions(ENABLE_DEBUG)
else( CMAKE_BUILD_TYPE STREQUAL "Release" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fopenmp")
endif( CMAKE_BUILD_TYPE STREQUAL "Debug" )

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBPMEMKV REQUIRED libpmemkv)
pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)

option(ENGINE_TREE3 "enable experimental tree3 engine" ON)


#4. head file path
include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/core
  ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/include
  ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include
  ${PROJECT_SOURCE_DIR}/db_impl/leveldb/include
  ${PROJECT_SOURCE_DIR}/db_impl/rocksdb/include
  ${PROJECT_SOURCE_DIR}/db_impl/FAST_FAIR/
  ${LIBPMEMKV_INCLUDE_DIRS}
  ${LIBPMEMOBJ_INCLUDE_DIRS}
  )

#5. third path link path
link_directories(
  ${PROJECT_SOURCE_DIR}/third_party/yaml-cpp/build/
  ${PROJECT_SOURCE_DIR}/third_party/googletest/build/lib/
  ${PROJECT_SOURCE_DIR}/db_impl/leveldb/build/
  ${PROJECT_SOURCE_DIR}/db_impl/rocksdb/
  ${PROJECT_SOURCE_DIR}/db_impl/FAST_FAIR/concurrent_pmdk/
  ${LIBPMEMKV_LIBRARY_DIRS}
  ${LIBPMEMOBJ_LIBRARY_DIRS}
  )

#6. source code file path
file(GLOB MAIN_FILE
  ${PROJECT_SOURCE_DIR}/ycsbc.cc
  )
file(GLOB SOURCE_FILE
  ${PROJECT_SOURCE_DIR}/core/*.cc
  ${PROJECT_SOURCE_DIR}/db/*.cc
  ${PROJECT_SOURCE_DIR}/db_impl/FAST_FAIR/concurrent_pmdk/src/*.cc
  )

#7. define the executable
add_executable(${PROJECT_NAME} ${MAIN_FILE} ${SOURCE_FILE})
target_link_libraries(${PROJECT_NAME}
  ${LIBPMEMOBJ_LIBRARIES}
  pmemobj
  pthread
  yaml-cpp
  gtest
  leveldb
  rocksdb
  pmemkv
  fastfair
  snappy
  zstd
  lz4
  bz2
  z
  dl
  )

#8. define gtest path
set(GTEST_LIBRARY ${PROJECT_SOURCE_DIR}/third_party/googletest/build/lib)
set(GTEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include)
set(GTEST_MAIN_LIBRARY ${GTEST_LIBRARY})

#9. compile test files
find_package(GTest)
enable_testing()
if (GTEST_FOUND)
  file(GLOB TEST_FILE ${PROJECT_SOURCE_DIR}/tests/*.cc)
  foreach(testsourcefile ${TEST_FILE})
    string(REGEX MATCH "[^/]+$" testsourcefilewithoutpath ${testsourcefile})
    string(REPLACE ".cc" "" testname ${testsourcefilewithoutpath})
    add_executable( ${testname} ${testsourcefile} ${SOURCE_FILE})
    target_link_libraries(${testname}
      pthread
      pmemobj
      yaml-cpp
      gtest
      leveldb
      rocksdb
      pmemkv
      snappy
      zstd
      lz4
      bz2
      z
      dl
      )
    add_test(NAME ${testname} COMMAND ${testname})
  ENDFOREACH(testsourcefile ${TEST_FILE})
endif()

#10. generate compile commands
add_custom_command(TARGET ${PROJECT_NAME}
  POST_BUILD
  COMMAND bash -x ${PROJECT_SOURCE_DIR}/gen_compile_commands.sh
  VERBATIM
  )

