//
// schema_test.cc
//
// Created by Zacharyliu-CS on 08/25/2023.
// Copyright (c) 2023 liuzhenm@mail.ustc.edu.cn.
//

#include "schema.h"
#include <gtest/gtest.h>
#include <iostream>
#include "field_type.h"
class SchemaTest : public testing::Test {
 private:
  NKV::SchemaAllocator _schemaAllocator;
  std::vector<NKV::SchemaField> _oldSchema = {
      NKV::SchemaField{NKV::FieldType::INT64T, "pk"},
      NKV::SchemaField{NKV::FieldType::INT64T, "f1"}};
};
TEST(TEST, Test) {}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
