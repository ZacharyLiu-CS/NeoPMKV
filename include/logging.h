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

#define NKV_LOG(output, level, fmt_str, ...)                            \
  do {                                                                  \
    fmt::print(output,                                                  \
               FMT_STRING("[{}:{} @{} #"#level"] Thread:[{}] " fmt_str "\n"), \
               __FILE__, __LINE__, __FUNCTION__,                \
               std::this_thread::get_id(), ##__VA_ARGS__);              \
  } while (0)
#define EMPTY(...) \
  do {             \
  } while (0)

#if defined(LOG_LEVEL) && (LOG_LEVEL == DEBUG)
#define NKV_LOG_D(output, fmt_str, ...) \
  NKV_LOG(output, DEBUG, fmt_str, ##__VA_ARGS__)
#define NKV_LOG_I(output, fmt_str, ...) \
  NKV_LOG(output, INFO, fmt_str, ##__VA_ARGS__)
#define NKV_LOG_E(output, fmt_str, ...) \
  NKV_LOG(output, ERROR, fmt_str, ##__VA_ARGS__)
#elif defined(LOG_LEVEL) && (LOG_LEVEL == INFO)
#define NKV_LOG_D(...) EMPTY(...)
#define NKV_LOG_I(output, fmt_str, ...) \
  NKV_LOG(output, INFO, fmt_str, ##__VA_ARGS__)
#define NKV_LOG_E(output, fmt_str, ...) \
  NKV_LOG(output, ERROR, fmt_str, ##__VA_ARGS__)
#elif defined(LOG_LEVEL) && (LOG_LEVEL == ERROR)
#define NKV_LOG_D(...) EMPTY(...)
#define NKV_LOG_I(...) EMPTY(...)
#define NKV_LOG_E(output, fmt_str, ...) \
  NKV_LOG(output, ERROR, fmt_str, ##__VA_ARGS__)
#else
#define NKV_LOG_D(...) EMPTY(...)
#define NKV_LOG_I(...) EMPTY(...)
#define NKV_LOG_E(...) EMPTY(...)
#endif

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