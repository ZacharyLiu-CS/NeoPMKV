//
//  variable_field_test.cc
//
//  Created by zhenliu on 09/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//
#include <iostream>

#include <cstdlib>
#include <memory>
#include <random>
#include <string>
#include "gtest/gtest.h"
#include "logging.h"
#include "neopmkv.h"
#include "schema.h"

namespace NKV {
class VariableFieldTest : public testing::Test {
 public:
  void SetUp() override {
    int res = system(clean_cmd_.c_str());
    res = system(mkdir_cmd_.c_str());
    if (neopmkv_ == nullptr) {
      neopmkv_ = new NKV::NeoPMKV(db_path_, chunk_size_, db_size_, enable_pbrb_,
                                  async_cache_);
    }
  }
  void TearDown() override {
    delete neopmkv_;
    int res = system(clean_cmd_.c_str());
  }

  NKV::VarStrFieldContent GetVariableField() {
    return NKV::VarStrFieldContent();
  }

 public:
  NKV::NeoPMKV *neopmkv_ = nullptr;
  bool enable_pbrb_ = true;
  bool async_cache_ = false;

  const uint64_t chunk_size_ = 128ull << 20;
  const uint64_t db_size_ = 1ull << 30;
  const std::string db_path_ = "/mnt/pmem0/tmp-neopmkv-test";
  std::string clean_cmd_ = std::string("rm -rf ") + std::string(db_path_);
  std::string mkdir_cmd_ = std::string("mkdir -p ") + std::string(db_path_);
};

TEST_F(VariableFieldTest, TestSize) {
  EXPECT_EQ(sizeof(VarStrFieldContent), FTSize[(uint8_t)FieldType::VARSTR]);
}

TEST_F(VariableFieldTest, TestCreation) {
  std::vector<NKV::SchemaField> fields{
      NKV::SchemaField(NKV::FieldType::INT64T, "pk"),
      NKV::SchemaField(NKV::FieldType::VARSTR, "f2", 16)};
  auto schemaid = neopmkv_->createSchema(fields, 0, "test0");
  std::vector<NKV::SchemaField> fields2{
      NKV::SchemaField(NKV::FieldType::STRING, "pk", 16),
      NKV::SchemaField(NKV::FieldType::VARSTR, "f2", 16)};
  schemaid = neopmkv_->createSchema(fields2, 0, "test1");
  std::vector<NKV::SchemaField> fields3{
      NKV::SchemaField(NKV::FieldType::INT64T, "pk"),
      NKV::SchemaField(NKV::FieldType::INT64T, "pk"),
      NKV::SchemaField(NKV::FieldType::VARSTR, "f2", 32)};
  schemaid = neopmkv_->createSchema(fields3, 0, "test2");
}

TEST_F(VariableFieldTest, TestVarStrConent) {
  auto varField0 = GetVariableField();
  char smallField[8] = "1234567";
  varField0.encode(smallField, sizeof(smallField));
  EXPECT_EQ(varField0.getSize(), 8);
  EXPECT_EQ(varField0.getType(), VariableFieldType::FULL_DATA);
  EXPECT_NE(varField0.getContent(),smallField);
  EXPECT_EQ(std::string(varField0.getContent()),std::string(smallField));

  char largeField[16] = "123456787654321";
  varField0.encode(largeField, sizeof(largeField));
  EXPECT_EQ(varField0.getSize(), 16);
  EXPECT_EQ(varField0.getType(), VariableFieldType::ONLY_PONTER);
  EXPECT_EQ(varField0.getContent(), largeField);

}
}  // namespace NKV

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
