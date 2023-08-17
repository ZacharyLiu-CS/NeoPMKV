//
//  MemPool.h
//
//  Created by zhenliu on 10/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <unistd.h>
#include "logging.h"
#ifndef TBB_PREVIEW_MEMORY_POOL
#define TBB_PREVIEW_MEMORY_POOL 1
#endif

#include <fmt/format.h>
#include <oneapi/tbb/memory_pool.h>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

namespace NKV {

class MemPool {
 public:
  MemPool(uint32_t page_size, uint32_t page_count)
      : _page_size(page_size), _page_count(page_count) {
    _pool_size = page_size * page_count;
    _memory_pool_buffer = (char *)malloc(_pool_size);
   
    _my_pool = new tbb::fixed_pool(_memory_pool_buffer, _pool_size);
  }
  void *AllocatePage() {
    _allocate_count.fetch_add(1, std::memory_order_relaxed);
    return _my_pool->malloc(_page_size);
  }
  void *Malloc(uint32_t size) {
    _allocate_count.fetch_add(1, std::memory_order_relaxed);
    void *ptr= _my_pool->malloc(size);
    NKV_LOG_I(std::cout,"Allocate address: {}", (uint64_t)ptr);
    return ptr;
  }
  void Free(void *ptr) {
    _free_count.fetch_add(1, std::memory_order_relaxed);
    NKV_LOG_I(std::cout,"Free address: {}", (uint64_t)ptr);
    _my_pool->free(ptr);
  }

  void RecyclePool() { _my_pool->recycle(); }
  ~MemPool() {
    _my_pool->recycle();
    delete _my_pool;
    free(_memory_pool_buffer);
  }
  void OutputStatus() {
    NKV_LOG_I(std::cout, "Allocate count: {}, Free count: {}",
              _allocate_count.load(std::memory_order_relaxed),
              _free_count.load(std::memory_order_relaxed));
  }
  uint32_t GetAllocateCount() {
    return _allocate_count.load(std::memory_order_relaxed);
  }

  uint32_t GetFreeCount() {
    return _free_count.load(std::memory_order_relaxed);
  }

 private:
  uint32_t _page_size = 0;
  uint32_t _page_count = 0;
  uint32_t _pool_size = 0;
  std::atomic_uint32_t _allocate_count = 0;
  std::atomic_uint32_t _free_count = 0;

  char *_memory_pool_buffer = nullptr;
  tbb::fixed_pool *_my_pool = nullptr;
};

}  // end of namespace NKV
