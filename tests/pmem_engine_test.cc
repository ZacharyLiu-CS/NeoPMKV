//
//  pmem_engine_test.cc
//
//  Created by zhenliu on 24/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "pmem_engine.h"
#include <cstdlib>
#include <future>
#include "gtest/gtest.h"
#include "pmem_log.h"
#include "schema.h"

std::string testBaseDir = "/mnt/pmem0/NKV-TEST";
NKV::PmemEngine *engine_ptr = nullptr;

void CleanTestFile() {
  bool status = false;
  if (std::filesystem::exists(testBaseDir)) {
    status = std::filesystem::remove_all(testBaseDir);
  } else {
    status = true;
  }
  ASSERT_TRUE(status == true);
}
TEST(GenerateTest, CreatePLOG) {
  NKV::PmemEngineConfig plogConfig;
  plogConfig.chunk_size = 4ULL << 20;
  plogConfig.engine_capacity = 1ULL << 30;
  strcpy(plogConfig.engine_path, testBaseDir.c_str());

  if (std::filesystem::exists(testBaseDir)) {
    std::filesystem::remove_all(testBaseDir);
  }
  auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
  ASSERT_TRUE(status.is2xxOK());
  delete engine_ptr;
}

TEST(OpenTest, OpenExistedPLOG) {
  NKV::PmemEngineConfig plogConfig;
  strcpy(plogConfig.engine_path, testBaseDir.c_str());
  auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
  ASSERT_TRUE(plogConfig.chunk_size == 4ULL << 20);
  ASSERT_TRUE(plogConfig.engine_capacity == 1ULL << 30);
  delete engine_ptr;
}

TEST(AppendTest, AppendSmallData) {
  NKV::PmemEngineConfig plogConfig;
  strcpy(plogConfig.engine_path, testBaseDir.c_str());
  auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
  ASSERT_TRUE(status.is2xxOK());
  int value_length = 128;
  char *value = (char *)std::malloc(value_length);
  memset(value, 43, value_length);
  NKV::PmemAddress pmem_addr;
  auto result = engine_ptr->append(pmem_addr, value, value_length);
  ASSERT_TRUE(result.is2xxOK());
  ASSERT_TRUE(pmem_addr == 0);
  delete engine_ptr;
  free(value);
}

TEST(ReadTest, ReadSmallData) {
  NKV::PmemEngineConfig plogConfig;
  strcpy(plogConfig.engine_path, testBaseDir.c_str());
  auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
  ASSERT_TRUE(status.is2xxOK());
  int value_length = 128;

  char *old_value = (char *)std::malloc(value_length);
  memset(old_value, 43, value_length);

  std::string new_value;

  NKV::PmemAddress pmem_addr = 0;
  auto result = engine_ptr->read(pmem_addr, new_value);
  ASSERT_EQ(memcmp(new_value.c_str(), old_value, value_length), 0);

  ASSERT_TRUE(result.is2xxOK());
  delete engine_ptr;
  free(old_value);
}
TEST(AppendTest, AppendLargeData) {
  NKV::PmemEngineConfig plogConfig;
  strcpy(plogConfig.engine_path, testBaseDir.c_str());
  auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
  ASSERT_TRUE(status.is2xxOK());

  // first large write
  int value_length = 2 << 20;
  char *value = (char *)std::malloc(value_length);
  memset(value, 44, value_length);
  NKV::PmemAddress pmem_addr;
  auto result = engine_ptr->append(pmem_addr, value, value_length);
  ASSERT_TRUE(result.is2xxOK());
  ASSERT_EQ(pmem_addr, 132);

  // second larege write
  memset(value, 45, value_length);
  result = engine_ptr->append(pmem_addr, value, value_length);
  ASSERT_TRUE(result.is2xxOK());
  ASSERT_EQ(pmem_addr, 2 * value_length);

  delete engine_ptr;
  free(value);
}
TEST(ReadTest, ReadLargeData) {
  NKV::PmemEngineConfig plogConfig;
  strcpy(plogConfig.engine_path, testBaseDir.c_str());
  auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
  ASSERT_TRUE(status.is2xxOK());
  int value_length = 2 << 20;
  char *old_value = (char *)std::malloc(value_length);

  memset(old_value, 44, value_length);

  std::string new_value;
  NKV::PmemAddress pmem_addr = 132;
  auto result = engine_ptr->read(pmem_addr, new_value);
  ASSERT_EQ(memcmp(new_value.c_str(), old_value, value_length), 0);

  ASSERT_TRUE(result.is2xxOK());
  delete engine_ptr;
  free(old_value);
  CleanTestFile();
}


TEST(MultiThreadTest, ConcurrentAccessPmemLog) {
  NKV::PmemEngineConfig plogConfig;
  plogConfig.chunk_size = 1ull << 10;
  strcpy(plogConfig.engine_path, testBaseDir.c_str());
  auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
  ASSERT_TRUE(status.is2xxOK());
  int value_length = 64 - 4;
  int num_threads = 16;
  int total_ops = 1ull << 10;
  auto writeToPmemLog = [=](int num_ops) {
    std::string value;
    for (auto i = 0; i < num_ops; i++) {
      value.resize(value_length);
      NKV::PmemAddress addr = 0;
      engine_ptr->append(addr, value.c_str(), value_length);
      ASSERT_EQ(addr % (value_length+4), 0);
    }
  };
  std::vector<std::future<void>> future_pool;
  for (auto i = 0; i < num_threads; i++) {
    future_pool.push_back(
        std::async(std::launch::async, writeToPmemLog, total_ops/num_threads));
  }
  for (auto &i : future_pool) {
    i.wait();
  }
  delete engine_ptr;
  CleanTestFile();
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}