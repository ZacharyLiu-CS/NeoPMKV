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
elseif( CMAKE_BUILD_TYPE STREQUAL "Release" )
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -fopenmp")
endif( CMAKE_BUILD_TYPE STREQUAL "Debug" )

# Set default log level to DEBUG if not specified
set(LOG_LEVEL "DEBUG" CACHE STRING "Log level") 

# Allow user to override default via cmake -DLOG_LEVEL=<level>
set_property(CACHE LOG_LEVEL PROPERTY STRINGS "DEBUG" "INFO" "ERROR" "DISABLE")

message(STATUS "Using log level: ${LOG_LEVEL}")
# Add compile definition based on log level
if(LOG_LEVEL STREQUAL "DEBUG")
  add_compile_definitions(LOG_LEVEL DEBUG)
elseif(LOG_LEVEL STREQUAL "INFO") 
  add_compile_definitions(LOG_LEVEL INFO)
elseif(LOG_LEVEL STREQUAL "ERROR")
  add_compile_definitions(LOG_LEVEL ERROR) 
elseif(LOG_LEVEL STREQUAL "DISABLE")
  add_compile_definitions(LOG_LEVEL DISABLE)
endif(LOG_LEVEL STREQUAL "DEBUG")

option(BREAKDOWN "Enable PBRB Breakdown" OFF)
if(BREAKDOWN)
  message("Enable PBRB Breakdown!")
  add_compile_definitions(ENABLE_BREAKDOWN)
endif(BREAKDOWN)

option(STATISTICS "Enable Statistic of pmem and pbrb" OFF)
if(STATISTICS)
  message("Enable NeoPMKV Statistic!")
  add_compile_definitions(ENABLE_STATISTICS)
endif(STATISTICS)

# find_package(PkgConfig REQUIRED)
# pkg_check_modules(LIBPMEMKV REQUIRED libpmemkv)
# pkg_check_modules(LIBPMEMOBJ REQUIRED libpmemobj)
find_package (fmt REQUIRED)
find_package (TBB REQUIRED)

# enable test
option(BUILD_TESTS "Build test programs" ON)
if(BUILD_TESTS)
  message("Compile Tests")
endif(BUILD_TESTS)
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
  TBB::tbb
  tbbmalloc
  )

#8. define the static library
add_library(${PROJECT_NAME} STATIC ${SOURCE_FILE})
target_link_libraries( ${PROJECT_NAME}
  pthread
  pmem
  fmt::fmt
  TBB::tbb
  tbbmalloc
  )
#9. define the install path
set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR}/neopmkv)
MESSAGE("Install prefix is: ${CMAKE_INSTALL_PREFIX}")
file(GLOB HEADERS ${PROJECT_SOURCE_DIR}/include/*.h)
install(TARGETS ${PROJECT_NAME} DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include)

set(CMAKE_INSTALL_PREFIX ${CMAKE_BINARY_DIR}/neopmkv)
MESSAGE("Install prefix is: ${CMAKE_INSTALL_PREFIX}")
file(GLOB HEADERS ${PROJECT_SOURCE_DIR}/include/*.h)
install(TARGETS ${PROJECT_NAME} DESTINATION lib)
install(FILES ${HEADERS} DESTINATION include)
  
#9. define gtest path
set(GTEST_LIBRARY ${PROJECT_SOURCE_DIR}/third_party/googletest/build/lib)
set(GTEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/third_party/googletest/googletest/include)
set(GTEST_MAIN_LIBRARY ${GTEST_LIBRARY})

#10. compile test files
if(BUILD_TESTS)
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
      TBB::tbb
      tbbmalloc
      )
    add_test(NAME ${testname} COMMAND ${testname})
  ENDFOREACH(testsourcefile ${TEST_FILE})
endif()
endif(BUILD_TESTS)
