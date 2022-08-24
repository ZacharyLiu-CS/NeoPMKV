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

#ifdef ENABLE_DEBUG
#define NKV_LOG_D(output, fmt_str, ...)                   \
  do {                                                    \
    fmt::print(output,                                    \
        FMT_STRING("[{}:{} @{} DEBUG] " fmt_str "\n"),    \
        __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); \
  } while (0)
#else
#define NKV_LOG_D(fmt_str, ...) \
  do {                          \
  } while (0)
#endif

#define NKV_LOG_I(output, fmt_str, ...)                   \
  do {                                                    \
    fmt::print(output,                                    \
        FMT_STRING("[{}:{} @{} INFO] " fmt_str "\n"),     \
        __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); \
  } while (0)
#define NKV_LOG_E(output, fmt_str, ...)                   \
  do {                                                    \
    fmt::print(output,                                    \
        FMT_STRING("[{}:{} @{} ERROR] " fmt_str "\n"),    \
        __FILE__, __LINE__, __FUNCTION__, ##__VA_ARGS__); \
  } while (0)
