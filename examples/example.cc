//
//  example.cc
//  PROJECT example
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "logging.h"
#include "neopmkv.h"

const std::string db_path = "/mnt/pmem0/tmp-neopmkv-test";
std::string clean_cmd = std::string("rm -rf ") + std::string(db_path);
std::string mkdir_cmd = std::string("mkdir -p ") + std::string(db_path);

int main() {
  int res = system(clean_cmd.c_str());
  res = system(mkdir_cmd.c_str());
  const uint64_t chunk_size = 128ull << 20;
  const uint64_t db_size = 1ull << 30;
  bool enablePBRB = false;
  bool asyncPBRB = false;
  NKV::NeoPMKV *neopmkv = nullptr;
  if (neopmkv == nullptr) {
    neopmkv =
        new NKV::NeoPMKV(db_path, chunk_size, db_size, enablePBRB, asyncPBRB);
  }
  std::vector<NKV::SchemaField> fields{
      NKV::SchemaField(NKV::FieldType::INT64T, "pk"),
      NKV::SchemaField(NKV::FieldType::STRING, "f2", 16)};
  auto schemaid = neopmkv->CreateSchema(fields, 0, "test");
  NKV::Key key1{schemaid, 1};
  std::vector<NKV::Value> value1 = {"14", "1dsadadasdad1"};
  NKV_LOG_I(std::cout, "value1 [0]:{} [1]:{}", value1[0], value1[1]);
  NKV::Key key2{schemaid, 2};
  
  std::vector<NKV::Value> value2 = {"15", "22-09kjlksjdlajlsd"};
  NKV_LOG_I(std::cout, "value2 [0]:{} [1]:{}", value2[0], value2[1]);
  neopmkv->Put(key1, value1);
  neopmkv->Put(key2, value2);

  std::string read_value = "";
  read_value.clear();
  neopmkv->Get(key1, read_value);
  NKV_LOG_I(std::cout, "value: {}", read_value);
  read_value.clear();
  neopmkv->Get(key2, read_value);
  NKV_LOG_I(std::cout, "value: {}", read_value);
  delete neopmkv;
  res = system(clean_cmd.c_str());
  return 0;
}