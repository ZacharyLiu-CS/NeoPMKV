//
//  mempool_test.cc
//
//  Created by zhenliu on 09/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//
#ifndef TBB_PREVIEW_MEMORY_POOL
#define TBB_PREVIEW_MEMORY_POOL 1
#endif

#include <oneapi/tbb/memory_pool.h>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <random>
#include <string>
#include "gtest/gtest.h"
#include "logging.h"

namespace NKV {
class MempoolTest : public testing::Test {
 public:
  void SetUp() override {
    _memory_pool_buffer = (char *)malloc(_pool_size);
    // std::cout << "The buffer address is [ " << (uint64_t)_memory_pool_buffer
    //   << " , " << (uint64_t)_memory_pool_buffer + _pool_size << " ]"
    //   << std::endl;
    _my_pool = new tbb::fixed_pool(_memory_pool_buffer, _pool_size);
  }
  bool AllocatePage(int size, int count) {
    for (uint32_t i = 0; i < count; i++) {
      void *tmp = nullptr;
      tmp = _my_pool->malloc(size);
      if (tmp == nullptr) {
        // std::cout << "stop at time " << i + 1 << std::endl;
        return false;
      }
      assert(tmp > _memory_pool_buffer &&
             tmp < _memory_pool_buffer + _pool_size);
      //   std::cout << "allocate address : " << (uint64_t)tmp << std::endl;
      _allocated_ptr.push_back(tmp);
    }
    return true;
  }
  void FreeSpaceToPool(int count) {
    for (uint32_t i = 0; i < count; i++) {
      _my_pool->free(_allocated_ptr.back());
      _allocated_ptr.pop_back();
    }
  }
  void RecyclePool() {
    _my_pool->recycle();
    _allocated_ptr.clear();
  }
  void TearDown() override {
    _my_pool->recycle();
    delete _my_pool;
    free(_memory_pool_buffer);
  }

 public:
  uint32_t _page_size = 4096;
  uint32_t _page_count = 64;
  std::vector<void *> _allocated_ptr;
  uint32_t _pool_size = _page_size * _page_count;
  char *_memory_pool_buffer = nullptr;
  tbb::fixed_pool *_my_pool = nullptr;
};

TEST_F(MempoolTest, TestAllocate) {
  RecyclePool();
  EXPECT_TRUE(AllocatePage(_page_size, 1));
  EXPECT_TRUE(AllocatePage(_page_size, _page_count / 2));
  EXPECT_FALSE(AllocatePage(_page_size, _page_count / 2));
  EXPECT_FALSE(AllocatePage(_page_size, 1));
}

TEST_F(MempoolTest, TestFree) {
  RecyclePool();
  AllocatePage(_page_size, _page_count);
  FreeSpaceToPool(1);
  EXPECT_TRUE(AllocatePage(_page_size, 1));
}

}  // namespace NKV
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
