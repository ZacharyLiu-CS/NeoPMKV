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

#define HEADER_SIZE NKV::ROW_META_HEAD_SIZE

class PmemEngineTest : public testing::Test {
 public:
  void SetUp() override {}
  void TearDown() override {}
  void SetFullData(char *src, uint32_t size, uint32_t content) {
    ((NKV::RowMetaHead *)src)
        ->setMeta(size - NKV::ROW_META_HEAD_SIZE, NKV::RowType::FULL_DATA, 0,
                  0);
    memset(src + HEADER_SIZE, content, size - HEADER_SIZE);
  }
  NKV::Status DeleteThenCreateEngine() {
    NKV::PmemEngineConfig plogConfig;
    plogConfig.chunk_size = 4ULL << 20;
    plogConfig.engine_capacity = 1ULL << 30;
    strcpy(plogConfig.engine_path, testBaseDir.c_str());

    if (std::filesystem::exists(testBaseDir)) {
      std::filesystem::remove_all(testBaseDir);
    }
    auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
    delete engine_ptr;
    return status;
  }

  NKV::Status OpenExistedPLOG() {
    NKV::PmemEngineConfig plogConfig;
    strcpy(plogConfig.engine_path, testBaseDir.c_str());
    auto status = NKV::PmemEngine::open(plogConfig, &engine_ptr);
    return status;
  }

  bool CleanTestFile() {
    bool status = false;
    if (std::filesystem::exists(testBaseDir)) {
      status = std::filesystem::remove_all(testBaseDir);
    } else {
      status = true;
    }
    return status;
  }

 public:
  NKV::PmemEngine *engine_ptr = nullptr;
  std::string testBaseDir = "/mnt/pmem0/NKV-TEST";
};

TEST_F(PmemEngineTest, CreatePLOG) {
  ASSERT_TRUE(DeleteThenCreateEngine().is2xxOK());
}

TEST_F(PmemEngineTest, OpenExistedPLOG) {
  ASSERT_TRUE(OpenExistedPLOG().is2xxOK());
}

TEST_F(PmemEngineTest, AppendSmallData) {
  ASSERT_TRUE(OpenExistedPLOG().is2xxOK());
  int value_length = 128;
  std::string value(value_length, 0);
  uint32_t content = 43;
  SetFullData(value.data(), value_length, content);

  NKV::PmemAddress pmem_addr;
  auto result = engine_ptr->append(pmem_addr, value.c_str(), value_length);
  ASSERT_TRUE(result.is2xxOK());
  ASSERT_TRUE(pmem_addr == 0);
  delete engine_ptr;
}

TEST_F(PmemEngineTest, ReadSmallData) {
  ASSERT_TRUE(OpenExistedPLOG().is2xxOK());
  int value_length = 128;

  std::string old_value(value_length, 0);
  
  uint32_t content = 43;
  SetFullData(old_value.data(), value_length, content);
  std::string new_value;

  NKV::PmemAddress pmem_addr = 0;
  auto result = engine_ptr->read(pmem_addr, new_value);
  EXPECT_STREQ(old_value.c_str(), new_value.c_str());

  ASSERT_TRUE(result.is2xxOK());
  delete engine_ptr;
}
TEST_F(PmemEngineTest, AppendLargeData) {
  ASSERT_TRUE(OpenExistedPLOG().is2xxOK());
  // first large write
  int value_length = 2 << 20;
  std::string value(value_length, 0);
  uint32_t content = 44;
  SetFullData(value.data(), value_length, content);
  
  NKV::PmemAddress pmem_addr;
  auto result = engine_ptr->append(pmem_addr, value.c_str(), value_length);
  ASSERT_TRUE(result.is2xxOK());
  ASSERT_EQ(pmem_addr, 128);

  // second large write
  content = 45;
  SetFullData(value.data(), value_length, content);
  result = engine_ptr->append(pmem_addr, value.c_str(), value_length);
  ASSERT_TRUE(result.is2xxOK());
  ASSERT_EQ(pmem_addr, 2 * value_length);

  delete engine_ptr;
}
TEST_F(PmemEngineTest, ReadLargeData) {
  ASSERT_TRUE(OpenExistedPLOG().is2xxOK());
  int value_length = 2 << 20;
  std::string old_value(value_length, 0);
  uint32_t content = 44;
  SetFullData(old_value.data(), value_length, content);

  std::string new_value;
  NKV::PmemAddress pmem_addr = 128;
  auto result = engine_ptr->read(pmem_addr, new_value);
  EXPECT_STREQ(old_value.c_str(), new_value.c_str());

  ASSERT_TRUE(result.is2xxOK());
  delete engine_ptr;
}

TEST_F(PmemEngineTest, ConcurrentAccessPmemLog) {
  ASSERT_TRUE(OpenExistedPLOG().is2xxOK());
  int value_length = 64;
  int num_threads = 16;
  int total_ops = 1ull << 10;
  uint32_t content = 32;
  auto writeToPmemLog = [=](int num_ops) {
    std::string value;
    for (auto i = 0; i < num_ops; i++) {
      value.resize(value_length);
      SetFullData(value.data(), value_length, content);
      NKV::PmemAddress addr = 0;
      engine_ptr->append(addr, value.c_str(), value_length);
      ASSERT_EQ(addr % (value_length), 0);
    }
  };
  std::vector<std::future<void>> future_pool;
  for (auto i = 0; i < num_threads; i++) {
    future_pool.push_back(std::async(std::launch::async, writeToPmemLog,
                                     total_ops / num_threads));
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