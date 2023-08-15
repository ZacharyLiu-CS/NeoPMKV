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
#include "mempool.h"
#include "neopmkv.h"
#include "schema.h"
#include "schema_parser.h"

namespace NKV {

// outer test
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

  NKV::VarFieldContent GetVariableField() { return NKV::VarFieldContent(); }

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
  EXPECT_EQ(sizeof(VarFieldContent), FTSize[(uint8_t)FieldType::VARSTR]);
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
  EncodeToVarFieldFullData(&varField0, smallField, sizeof(smallField));
  EXPECT_EQ(GetVarFieldSize(&varField0), 8);
  EXPECT_EQ(GetVarFieldType(&varField0), VarFieldType::FULL_DATA);
  EXPECT_NE(GetVarFieldContent(&varField0), smallField);
  EXPECT_EQ(std::string(GetVarFieldContent(&varField0)),
            std::string(smallField));

  char largeField[16] = "123456787654321";
  EncodeToVarFieldOnlyPointer(&varField0, largeField, sizeof(largeField));
  EXPECT_EQ(GetVarFieldSize(&varField0), 16);
  EXPECT_EQ(GetVarFieldType(&varField0), VarFieldType::ONLY_PONTER);
  EXPECT_EQ(GetVarFieldContent(&varField0), largeField);
}

// internal test
class ParserTest : public testing::Test {
 public:
  void SetUp() override {
    _memPoolPtr = new MemPool(4096, 64);
    _parser = new SchemaParser(_memPoolPtr);
  }
  Schema BuildSchema(std::string name, std::vector<NKV::SchemaField> &fields) {
    return _schemaAllocator.createSchema(name, 0, fields);
  }
  std::string FromUserToSeqRow(Schema *schemaPtr, std::vector<Value> &values) {
    return _parser->ParseFromUserWriteToSeq(schemaPtr, values);
  }
  char *FromSeqToTwoPart(Schema *schemaPtr, std::string &seqValue) {
    return _parser->ParseFromSeqToTwoPart(schemaPtr, seqValue);
  }
  std::string FromTwoPartToSeq(Schema *schemaPtr, char *rowFiexdPart,
                               char *rowVarPart) {
    return _parser->ParseFromTwoPartToSeq(schemaPtr, rowFiexdPart, rowVarPart);
  }
  void TearDown() override {
    delete _memPoolPtr;
    delete _parser;
  }

 private:
  SchemaAllocator _schemaAllocator;
  MemPool *_memPoolPtr = nullptr;
  SchemaParser *_parser = nullptr;
};

TEST_F(ParserTest, TestSeqRowFormat) {
  std::vector<NKV::SchemaField> fields1{
      NKV::SchemaField(NKV::FieldType::INT64T, "pk"),
      NKV::SchemaField(NKV::FieldType::INT64T, "pk"),
      NKV::SchemaField(NKV::FieldType::VARSTR, "f2")};
  Schema test1 = BuildSchema("test1", fields1);
  ValueReader valueReader(&test1);

  std::vector<Value> values1 = {"gg1", "gg2", "var2"};
  // row format :  | row size | field 0 | field 1 | field 2|
  std::string res = FromUserToSeqRow(&test1, values1);
  Value field0, field1, field2;
  valueReader.ExtractFieldFromRow(res.data(), 0, field0);
  valueReader.ExtractFieldFromRow(res.data(), 1, field1);
  valueReader.ExtractFieldFromRow(res.data(), 2, field2);
  EXPECT_STREQ(values1[0].data(), field0.data());
  EXPECT_STREQ(values1[1].data(), field1.data());
  EXPECT_STREQ(values1[2].data(), field2.data());
  // for (auto& el : res)
  // 	std::cout << std::setfill('0') << std::setw(2) << std::hex << (0xff &
  // (unsigned int)el) << " "; std::cout << '\n';

  EXPECT_EQ(*(uint32_t *)res.data(), 28);
  EXPECT_EQ(res.size(), 32);
  EXPECT_EQ(((VarFieldContent *)(res.data() + 20))->contentSize, 4);
  EXPECT_EQ(((VarFieldContent *)(res.data() + 20))->contentType,
            VarFieldType::FULL_DATA);

  std::vector<NKV::SchemaField> fields2{
      NKV::SchemaField(NKV::FieldType::INT64T, "pk"),
      NKV::SchemaField(NKV::FieldType::INT64T, "pk"),
      NKV::SchemaField(NKV::FieldType::VARSTR, "f2")};
  Schema test2 = BuildSchema("test2", fields2);
  std::vector<Value> values2 = {"gg1", "gg2", "var234567890123455678"};
  std::string res2 = FromUserToSeqRow(&test2, values2);

  std::vector<Value> fieldValues(3);
  ValueReader valueReader2(&test2);
  valueReader2.ExtractFieldFromRow(res2.data(), 0, fieldValues[0]);
  valueReader2.ExtractFieldFromRow(res2.data(), 1, fieldValues[1]);
  valueReader2.ExtractFieldFromRow(res2.data(), 2, fieldValues[2]);
  EXPECT_STREQ(values2[0].data(), fieldValues[0].data());
  EXPECT_STREQ(values2[1].data(), fieldValues[1].data());
  EXPECT_STREQ(values2[2].data(), fieldValues[2].data());

  // row format :  | row size| row type | field 0 | field 1 | field 2 head|
  // field 2 content |
  //               0         2         4         12        20            32
  EXPECT_EQ(*(uint32_t *)res2.data(), 49);
  EXPECT_EQ(res2.size(), 53);
  EXPECT_EQ(((VarFieldContent *)(res2.data() + 20))->contentSize, 21);
  EXPECT_EQ(((VarFieldContent *)(res2.data() + 20))->contentType,
            VarFieldType::ROW_OFFSET);
  EXPECT_EQ(((VarFieldContent *)(res2.data() + 20))->contentOffset, 12);

  //   for (auto& el : res2)
  // 	std::cout << std::setfill('0') << std::setw(2) << std::hex << (0xff &
  // (unsigned int)el) << " "; std::cout << '\n';
  char *varPart = FromSeqToTwoPart(&test2, res2);
  //  for (auto& el : res2)
  // 	std::cout << std::setfill('0') << std::setw(2) << std::hex << (0xff &
  // (unsigned int)el) << " "; std::cout << '\n'; printf("variable part: %s\n",
  // varPart);
  std::vector<Value> fieldValues2(3);
  valueReader2.ExtractFieldFromRow(res2.data(), 0, fieldValues2[0]);
  valueReader2.ExtractFieldFromRow(res2.data(), 1, fieldValues2[1]);
  valueReader2.ExtractFieldFromRow(res2.data(), 2, fieldValues2[2]);
  EXPECT_STREQ(values2[0].data(), fieldValues2[0].data());
  EXPECT_STREQ(values2[1].data(), fieldValues2[1].data());
  EXPECT_STREQ(values2[2].data(), fieldValues2[2].data());

  EXPECT_EQ(*(uint32_t *)res2.data(), 49);
  EXPECT_EQ(res2.size(), 32);
  EXPECT_NE(varPart, nullptr);

  std::string res3 = FromTwoPartToSeq(&test2, res2.data(), varPart);
  EXPECT_EQ(*(uint32_t *)res3.data(), 49);
  EXPECT_EQ(res3.size(), 53);
  EXPECT_EQ(res2.substr(0, 24), res3.substr(0, 24));
  //  for (auto& el : res3)
  // 	std::cout << std::setfill('0') << std::setw(2) << std::hex << (0xff &
  // (unsigned int)el) << " "; std::cout << '\n';
  std::vector<Value> fieldValues3(3);
  valueReader2.ExtractFieldFromRow(res3.data(), 0, fieldValues3[0]);
  valueReader2.ExtractFieldFromRow(res3.data(), 1, fieldValues3[1]);
  valueReader2.ExtractFieldFromRow(res3.data(), 2, fieldValues3[2]);
  EXPECT_STREQ(values2[0].data(), fieldValues3[0].data());
  EXPECT_STREQ(values2[1].data(), fieldValues3[1].data());
  EXPECT_STREQ(values2[2].data(), fieldValues3[2].data());
}

}  // namespace NKV

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
