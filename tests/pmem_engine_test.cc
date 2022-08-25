//
//  tests/pmem_engine_test.cc
//  PROJECT tests/pmem_engine_test
//
//  Created by zhenliu on 24/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "pmem_engine.h"
#include <cstdlib>
#include "gtest/gtest.h"
#include "pmem_log.h"
#include "schema.h"

std::string testBaseDir = "/mnt/pmem0/NKV-TEST";
NKV::PmemEngine *engine_ptr = nullptr;

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
  NKV::ValueContent *value = (NKV::ValueContent *)std::malloc(
      sizeof(NKV::ValueContent) + value_length);
  value->size = value_length;
  memset(value->fieldData, 43, value_length);
  NKV::PmemAddress pmem_addr;
  auto result = engine_ptr->append(pmem_addr, value);
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
  NKV::ValueContent *old_value = (NKV::ValueContent *)std::malloc(
      sizeof(NKV::ValueContent) + value_length);
  old_value->size = value_length;
  memset(old_value->fieldData, 43, value_length);

  NKV::ValueContent *new_value = (NKV::ValueContent *)std::malloc(
      sizeof(NKV::ValueContent) + value_length);
  new_value->size = value_length;

  NKV::PmemAddress pmem_addr = 0;
  auto result = engine_ptr->read(pmem_addr, new_value);
  ASSERT_EQ(memcmp(new_value->fieldData, old_value->fieldData, value_length),
            0);

  ASSERT_TRUE(result.is2xxOK());
  delete engine_ptr;
  free(new_value);
  free(old_value);
}
TEST(AppendTest, AppendLargeData) {
  NKV::PmemEngineConfig plogConfig;
  strcpy(plogConfig.engine_path, testBaseDir.c_str());
  auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
  ASSERT_TRUE(status.is2xxOK());

  //first large write
  int value_length = 2 << 20;
  NKV::ValueContent *value = (NKV::ValueContent *)std::malloc(
      sizeof(NKV::ValueContent) + value_length);
  value->size = value_length;
  memset(value->fieldData, 44, value_length);
  NKV::PmemAddress pmem_addr;
  auto result = engine_ptr->append(pmem_addr, value);
  ASSERT_TRUE(result.is2xxOK());
  ASSERT_TRUE(pmem_addr == 132);

  // second larege write
  memset(value->fieldData, 45, value_length);
  result = engine_ptr->append(pmem_addr, value);
  ASSERT_TRUE(result.is2xxOK());
  ASSERT_EQ(pmem_addr, 2*value_length );

  delete engine_ptr;
  free(value);
}
TEST(ReadTest, ReadLargeData) {
  NKV::PmemEngineConfig plogConfig;
  strcpy(plogConfig.engine_path, testBaseDir.c_str());
  auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
  ASSERT_TRUE(status.is2xxOK());
  int value_length = 2 << 20;
  NKV::ValueContent *old_value = (NKV::ValueContent *)std::malloc(
      sizeof(NKV::ValueContent) + value_length);
  old_value->size = value_length;
  memset(old_value->fieldData, 44, value_length);

  NKV::ValueContent *new_value = (NKV::ValueContent *)std::malloc(
      sizeof(NKV::ValueContent) + value_length);
  new_value->size = value_length;

  NKV::PmemAddress pmem_addr = 132;
  auto result = engine_ptr->read(pmem_addr, new_value);
  ASSERT_EQ(memcmp(new_value->fieldData, old_value->fieldData, value_length),
            0);

  ASSERT_TRUE(result.is2xxOK());
  delete engine_ptr;
  free(new_value);
  free(old_value);
}

TEST(EndTest, CleanTestFile) {
  bool status = false;
  if (std::filesystem::exists(testBaseDir)) {
    status = std::filesystem::remove_all(testBaseDir);
  } else {
    status = true;
  }
  ASSERT_TRUE(status == true);
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}