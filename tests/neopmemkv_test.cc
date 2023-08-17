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
class NeoPMKVTest : public testing::Test {
 public:
  // clean the old files and create path
  void SetUp() override {
    int res = system(clean_cmd.c_str());
    res = system(mkdir_cmd.c_str());
  }
  // clean db files
  void TearDown() override {
    delete neopmkv_;
    bool status = false;
    if (std::filesystem::exists(db_path)) {
      status = std::filesystem::remove_all(db_path);
    } else {
      status = true;
    }
  }
  Value BuildFieldValue(uint32_t i, uint32_t field, uint32_t size) {
    std::string num_str = std::to_string(i + field);
    int zero_padding = size - num_str.size();
    Value v;
    v.append(zero_padding, '0').append(num_str);
    return v;
  };

  Key BuildKey(uint32_t i, SchemaId sid) { return Key(sid, i); }

  std::vector<Value> BuildValue(uint32_t i, uint32_t seed) {
    std::vector<Value> value;
    value.push_back(BuildFieldValue(i + seed, 0, 8));
    value.push_back(BuildFieldValue(i + seed, 1, 16));
    value.push_back(BuildFieldValue(i + seed, 2, 16));
    return value;
  }

  void PrepareData(uint32_t i, uint32_t seed) {
    auto key = BuildKey(i, sid);
    auto value = BuildValue(i, seed);
    neopmkv_->Put(key, value);
  }

  void PartialUpdateData(uint32_t i, Value &fieldValue, uint32_t fieldId) {
    auto key = BuildKey(i, sid);
    neopmkv_->PartialUpdate(key, fieldValue, 2);
  }

  Value GetData(uint32_t i) {
    Value value;
    auto key = BuildKey(i, sid);
    neopmkv_->Get(key, value);
    return value;
  }

  Value PartialGetData(uint32_t i, uint32_t fieldId) {
    Value value;
    auto key = BuildKey(i, sid);
    neopmkv_->PartialGet(key, value, fieldId);
    return value;
  }

  bool RemoveData(uint32_t i) {
    auto key = BuildKey(i, sid);
    return neopmkv_->Remove(key);
  }

  void SetNeoPMKV(bool enablePBRB = false, bool asyncPBRB = false) {
    if (neopmkv_ != nullptr) delete neopmkv_;
    if (neopmkv_ == nullptr) {
      neopmkv_ =
          new NKV::NeoPMKV(db_path, chunk_size, db_size, enablePBRB, asyncPBRB);
    }
    sid = neopmkv_->CreateSchema(fields, 0, "test1");
  }

 private:
  const std::string db_path = "/mnt/pmem0/tmp-neopmkv-test";
  std::string clean_cmd = std::string("rm -rf ") + std::string(db_path);
  std::string mkdir_cmd = std::string("mkdir -p ") + std::string(db_path);
  std::vector<SchemaField> fields{SchemaField(FieldType::INT64T, "pk"),
                                  SchemaField(FieldType::STRING, "f1", 16),
                                  SchemaField(FieldType::STRING, "f2", 16)};
  const uint64_t chunk_size = 128ull << 20;
  const uint64_t db_size = 1ull << 30;
  NKV::NeoPMKV *neopmkv_ = nullptr;
  SchemaId sid = 0;
};

TEST_F(NeoPMKVTest, DisablePBRBTest) {
  SetNeoPMKV(false, false);
  uint32_t count = 5;
  uint32_t seed = 84987;
  for (uint32_t i = 0; i < count; i++) {
    PrepareData(i, seed);
  }
  for (uint32_t i = 0; i < count; i++) {
    auto ev = BuildFieldValue(i + seed, 2, 16);
    auto pv = PartialGetData(i, 2);
    EXPECT_STREQ(ev.data(), pv.data());
  }
  seed = 95465;
  for (uint32_t i = 0; i < count; i++) {
    auto ev = BuildFieldValue(i + seed, 2, 16);
    // std::cout << ev << std::endl;
    PartialUpdateData(i, ev, 2);
    auto pv = PartialGetData(i, 2);
    auto fv = GetData(i);
    // std::cout << fv << std::endl;
    EXPECT_STREQ(ev.data(), pv.data());
  }
}

TEST_F(NeoPMKVTest, SYNCPBRBTest) {
  SetNeoPMKV(true, false);
  uint32_t count = 5;
  uint32_t seed = 84987;
  for (uint32_t i = 0; i < count; i++) {
    PrepareData(i, seed);
    GetData(i);
  }
  for (uint32_t i = 0; i < count; i++) {
    auto ev = BuildFieldValue(i + seed, 2, 16);
    auto pv = PartialGetData(i, 2);
    EXPECT_STREQ(ev.data(), pv.data());
  }
  seed = 95465;
  for (uint32_t i = 0; i < count; i++) {
    auto ev = BuildFieldValue(i + seed, 2, 16);
    // std::cout << ev << std::endl;
    PartialUpdateData(i, ev, 2);
    auto pv = PartialGetData(i, 2);
    auto fv = GetData(i);
    // std::cout << fv << std::endl;
    EXPECT_STREQ(ev.data(), pv.data());
  }
}

TEST_F(NeoPMKVTest, ASYNCPBRBTest) {
  SetNeoPMKV(true, true);
  uint32_t count = 5;
  uint32_t seed = 84987;
  for (uint32_t i = 0; i < count; i++) {
    PrepareData(i, seed);
  }
  for (uint32_t i = 0; i < count; i++) {
    auto ev = BuildFieldValue(i + seed, 2, 16);
    auto pv = PartialGetData(i, 2);
    EXPECT_STREQ(ev.data(), pv.data());
  }
  for (uint32_t i = 0; i < count; i++) {
    auto ev = BuildFieldValue(i + seed, 2, 16);
    auto pv = PartialGetData(i, 2);
    EXPECT_STREQ(ev.data(), pv.data());
  }
  seed = 95465;
  for (uint32_t i = 0; i < count; i++) {
    auto ev = BuildFieldValue(i + seed, 2, 16);
    // std::cout << ev << std::endl;
    PartialUpdateData(i, ev, 2);
    auto pv = PartialGetData(i, 2);
    auto fv = GetData(i);
    // std::cout << fv << std::endl;
    EXPECT_STREQ(ev.data(), pv.data());
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}