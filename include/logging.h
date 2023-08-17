//
//  log.h
//  PROJECT log
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <fmt/compile.h>
#include <fmt/printf.h>
#include <gtest/internal/gtest-port.h>
#include <thread>

#ifdef ENABLE_DEBUG
#define NKV_LOG_D(output, fmt_str, ...)                                      \
  do {                                                                       \
    fmt::print(output,                                                       \
               FMT_STRING("[{}:{} @{} DEBUG] Thread:[{}] " fmt_str "\n"),    \
               __FILE__, __LINE__, __FUNCTION__, std::this_thread::get_id(), \
               ##__VA_ARGS__);                                               \
  } while (0)
#else
#define NKV_LOG_D(fmt_str, ...) \
  do {                          \
  } while (0)
#endif

#define NKV_LOG_I(output, fmt_str, ...)                                      \
  do {                                                                       \
    fmt::print(output,                                                       \
               FMT_STRING("[{}:{} @{} INFO] Thread:[{}] " fmt_str "\n"),     \
               __FILE__, __LINE__, __FUNCTION__, std::this_thread::get_id(), \
               ##__VA_ARGS__);                                               \
  } while (0)
#define NKV_LOG_E(output, fmt_str, ...)                                      \
  do {                                                                       \
    fmt::print(output,                                                       \
               FMT_STRING("[{}:{} @{} ERROR] Thread:[{}] " fmt_str "\n"),    \
               __FILE__, __LINE__, __FUNCTION__, std::this_thread::get_id(), \
               ##__VA_ARGS__);                                               \
  } while (0)

// #define NKV_LOG_I(fmt_str, ...) \
//   do {                          \
//   } while (0)

// #define NKV_LOG_E(fmt_str, ...) \
//   do {                          \
//   } while (0)

#ifdef ENABLE_STATISTICS

#define POINT_PROFILE_START(var_name) \
  PointProfiler var_name;             \
  do {                                \
    var_name.start();                 \
  } while (0)
#define POINT_PROFILE_END(var_name) \
  do {                              \
    var_name.end();                 \
  } while (0)
#define PROFILER_ATMOIC_ADD(var_name, add_count) \
  do {                                           \
    var_name.fetch_add(add_count);               \
  } while (0)

#else

#define POINT_PROFILE_START(var_name) \
  do {                                \
  } while (0)

#define POINT_PROFILE_END(var_name) \
  do {                              \
  } while (0)

#define PROFILER_ATMOIC_ADD(var_name, add_count) \
  do {                                           \
  } while (0)

#endif