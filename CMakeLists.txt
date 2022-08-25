#1. specify the version of cmake
cmake_minimum_required(VERSION 3.10)

#2. define the name of project
project(neopmkv)

#3. specify c++ compiler version
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-std=c++17 -pthread")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if( CMAKE_BUILD_TYPE STREQUAL "Debug" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
    add_compile_definitions(ENABLE_DEBUG)
else( CMAKE_BUILD_TYPE STREQUAL "Release" )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fopenmp")
endif( CMAKE_BUILD_TYPE STREQUAL "Debug" )

# find_package(PkgConfig REQUIRED)
# pkg_check_modules(LIBPMEMKV REQUIRED libpmemkv)
# pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)
find_package (fmt REQUIRED)


#4. head file path
include_directories(
  ${PROJECT_SOURCE_DIR}/include
  )

#5. third path link path
link_directories(
  )

#6. source code file path
file(GLOB MAIN_FILE
  ${PROJECT_SOURCE_DIR}/examples/example.cc
  )
file(GLOB SOURCE_FILE
  ${PROJECT_SOURCE_DIR}/src/*.cc
  )

#7. define the executable
add_executable(example ${MAIN_FILE} ${SOURCE_FILE})
target_link_libraries(example
  pthread
  pmem
  fmt::fmt
  )

#8. define the static library
add_library(${PROJECT_NAME} STATIC ${SOURCE_FILE})
target_link_libraries( ${PROJECT_NAME}
  pthread
  pmem
  fmt::fmt
  )

#9. define gtest path
set(GTEST_LIBRARY ${PROJECT_SOURCE_DIR}/third_party/googletest/build/lib)
set(GTEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include)
set(GTEST_MAIN_LIBRARY ${GTEST_LIBRARY})

#10. compile test files
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
      pmem
      gtest
      fmt::fmt
      )
    add_test(NAME ${testname} COMMAND ${testname})
  ENDFOREACH(testsourcefile ${TEST_FILE})
endif()


