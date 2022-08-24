//
//  profiler.h
//  PROJECT profiler.h 
//
//  Created by zhenliu on 18/05/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <cpuid.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <iostream>

#include <stdint.h>  // uint64_t

#if defined(_M_X64) || defined(_M_IX86) || defined(__x86_64) || defined(__i386)
#ifdef _WIN32
#include <intrin.h>  // __rdtsc
#else
#include <x86intrin.h>  // __rdtsc
#endif
#define HAS_HW_RDTSC 1
#else
#include <chrono>  // std::chrono::high_resolution_clock
#define HAS_HW_RDTSC 0
#endif

#define TSC_KHz 2095078ull

inline uint64_t rte_rdtsc() {
  union {
    uint64_t tsc_64;
    struct {
      uint32_t lo_32;
      uint32_t hi_32;
    };
  } tsc;
  asm volatile("rdtsc":
      "=a"(tsc.lo_32),
      "=d"(tsc.hi_32));
  return tsc.tsc_64;
}

inline uint64_t rdtsc() {
#if HAS_HW_RDTSC
  // _mm_lfence() might be used to serialize the instruction stream,
  // and it would guarantee that RDTSC will not be reordered with
  // other instructions.  However, measurements show that the overhead
  // may be too big (easily 15 to 30 CPU cycles) for profiling
  // purposes: if reordering matters, the overhead matters too!

  // Forbid the compiler from reordering instructions
#ifdef _MSC_VER
  _ReadWriteBarrier();
#else
  __asm__ __volatile__("" : : : "memory");
#endif

  uint64_t result = __rdtsc();

  // Forbid the compiler from reordering instructions
#ifdef _MSC_VER
  _ReadWriteBarrier();
#else
  __asm__ __volatile__("" : : : "memory");
#endif

  return result;
#else
  auto now = std::chrono::high_resolution_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             now.time_since_epoch())
      .count();
#endif
}

class PointProfiler {
 public:
  inline PointProfiler() {}
  ~PointProfiler() {}
  static double now(){return rte_rdtsc();}

  inline void start() { start_tick_ = rte_rdtsc(); }
  inline double end() {
    uint64_t end_tick = rte_rdtsc();
    return duration_ = static_cast<double>(end_tick - start_tick_) /
           static_cast<double>(TSC_KHz / 1000000ull);
  }

  inline double duration(){
    return duration_;
  }

 private:
  uint64_t start_tick_ = 0;
  double duration_ = 0;
};
