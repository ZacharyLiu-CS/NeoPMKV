//
//  neopmemkv_test.cc
//  PROJECT neopmkv_test
//
//  Created by zhenliu on 24/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//
#include <unistd.h>
#include <iostream>

#include <cstdlib>
#include <random>
#include <string>
#include <thread>
#include "gtest/gtest.h"
#include "logging.h"
#include "neopmkv.h"
#include "pbrb.h"
#include "pmem_engine.h"
#include "schema.h"

using namespace NKV;
const std::string db_path = "/mnt/pmem0/tmp-neopmkv-test";
std::string clean_cmd = std::string("rm -rf ") + std::string(db_path);
std::string mkdir_cmd = std::string("mkdir -p ") + std::string(db_path);

std::vector<SchemaField> Fields{SchemaField(FieldType::INT64T, "pk"),
                                SchemaField(FieldType::STRING, "f1", 8),
                                SchemaField(FieldType::STRING, "f2", 16)};
const uint64_t chunk_size = 128ull << 20;
const uint64_t db_size = 1ull << 30;
void cleanFile(std::string file_path) {
  bool status = false;
  if (std::filesystem::exists(file_path)) {
    status = std::filesystem::remove_all(file_path);
  } else {
    status = true;
  }
  ASSERT_TRUE(status == true);
}
void PBRBTest(bool enablePBRB = false, bool asyncPBRB = false) {
  int res = system(clean_cmd.c_str());
  res = system(mkdir_cmd.c_str());

  NKV::NeoPMKV *neopmkv_ = nullptr;
  if (neopmkv_ == nullptr) {
    neopmkv_ = new NKV::NeoPMKV(db_path, chunk_size, db_size, enablePBRB, asyncPBRB);
  }
  SchemaId sid = neopmkv_->createSchema(Fields, 0, "test1");

  // Generate KVs
  std::vector<Value> values;
  uint32_t length = 300;

  auto BuildValue = [](uint64_t i) -> Value {
    std::string num_str = std::to_string(i);
    int zero_padding = 44 - num_str.size();
    Value v;
    v.append(zero_padding, '0').append(num_str);
    return v;
  };
  auto BuildKey = [sid](uint64_t i) -> Key { return Key(sid, i); };

  auto BuildUpdatedValue = [](uint64_t i) -> Value {
    std::string num_str = std::to_string(i + 33);
    int zero_padding = 44 - num_str.size();
    Value v;
    v.append(zero_padding, '0').append(num_str);
    return v;
  };
  auto BuildUpdates = [](uint64_t i) -> Value {
    std::string num_str = std::to_string(i + 33);
    int zero_padding = 16 - num_str.size();
    Value v;
    v.append(zero_padding, '0').append(num_str);
    return v;
  };

  // test the put operation
  for (uint64_t i = 0; i < length; i++) {
    auto key = BuildKey(i);
    auto value = BuildValue(i);
    neopmkv_->put(key, value);
  }
  // test the get the operation
  for (uint64_t i = 0; i < length; i++) {
    auto key = BuildKey(i);
    auto expect_value = BuildValue(i);
    Value read_value;
    neopmkv_->get(key, read_value);
    ASSERT_EQ(read_value, expect_value);
  }
  // Add a get round (read from PBRB)
  for (uint64_t i = 0; i < length; i++) {
    auto key = BuildKey(i);
    auto expect_value = BuildValue(i);
    Value read_value;
    neopmkv_->get(key, read_value);
    if( read_value != expect_value){
      std::cout<< i<< " read value:   " << read_value << std::endl;
      std::cout<< i<< " expect value: " << expect_value << std::endl;
      std::cout<< "----"<< std::endl;
    }
    // ASSERT_EQ(read_value, expect_value);
  }
  /*
  // test the update correctness
  for (uint64_t i = 0; i < length; i++) {
    auto key = BuildKey(i);
    std::vector<std::pair<uint32_t, std::string>> update_values;
    update_values.push_back({2, BuildUpdates(i)});
    neopmkv_->update(key, update_values);
  }
  for (uint64_t i = 0; i < length; i++) {
    auto key = BuildKey(i);
    auto expect_value = BuildUpdatedValue(i);
    Value read_value;
    neopmkv_->get(key, read_value);
    ASSERT_EQ(read_value, expect_value);
  }
  // test the remove operation
  int remove_length = 100;
  for (uint64_t i = 0; i < remove_length; i++) {
    auto key = BuildKey(i);
    neopmkv_->remove(key);
  }
  for (uint64_t i = 0; i < length; i++) {
    auto key = BuildKey(i);
    auto expect_value = BuildUpdatedValue(i);
    Value read_value;
    bool res = neopmkv_->get(key, read_value);
    if (i < remove_length)
      ASSERT_EQ(false, res);
    else{
      if (read_value != expect_value)
      NKV_LOG_E(std::cerr, "value [{}]:read_value: {} expect_value: {}", i,read_value, expect_value);
    }

      // ASSERT_EQ(read_value, expect_value);
  }
  Key start_key = BuildKey(1);
  std::vector<Value> value_list;
  int scan_length = 100;
  neopmkv_->scan(start_key, value_list, scan_length);
  for (uint64_t i = 0; i < value_list.size(); i++) {
    auto expect_value = BuildUpdatedValue(i + remove_length);
    NKV_LOG_D(std::cout, "Key [{}] expect value: {} read value: {}",
              i + remove_length, expect_value, value_list[i]);
    ASSERT_EQ(expect_value, value_list[i]);
  }
  */
  delete neopmkv_;
}
// TEST(NEOPMKVTEST, DisablePBRBTest) {
//   PBRBTest(false);
//   cleanFile(db_path);
// }

// TEST(NEOPMKVTEST, SYNCPBRBTest) {
//   PBRBTest(true, false);
//   cleanFile(db_path);
// }

TEST(NEOPMKVTEST, ASYNCPBRBTest) {
  PBRBTest(true, true);
  cleanFile(db_path);
}
/*
TEST(NEOPMKVTEST, TimeStaticsTest) {
  int res = system(clean_cmd.c_str());
  res = system(mkdir_cmd.c_str());

  NKV::NeoPMKV *neopmkv_ = nullptr;
  if (neopmkv_ == nullptr) {
    neopmkv_ =
        new NKV::NeoPMKV(db_path, chunk_size, db_size, true, false, true, 1ull << 14);
  }
  SchemaId sid = neopmkv_->createSchema(Fields, 0, "test1");

  // Generate KVs
  std::vector<Value> values;
  uint32_t length = 1 << 20;

  auto BuildValue = [](uint64_t i) -> Value {
    std::string num_str = std::to_string(i);
    int zero_padding = 44 - num_str.size();
    Value v;
    v.append(zero_padding, '0').append(num_str);
    return v;
  };
  auto BuildKey = [sid](uint64_t i) -> Key { return Key(sid, i); };

  for (uint64_t i = 0; i < length; i++) {
    auto key = BuildKey(i);
    auto value = BuildValue(i);
    neopmkv_->put(key, value);
  }
  uint32_t maxReadRounds = 20;
  for (uint32_t round = 0; round < maxReadRounds; round++) {
    NKV_LOG_I(std::cout, "========== Round {} ==========", round);
    for (uint64_t i = 0; i < length; i++) {
      auto key = BuildKey(i);
      auto expect_value = BuildValue(i);
      Value read_value;
      neopmkv_->get(key, read_value);
      ASSERT_EQ(read_value, expect_value);
    }
  }
  delete neopmkv_;
}
*/
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}