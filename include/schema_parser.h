//
//  schema_parser.h
//
//  Created by zhenliu on 09/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <fmt/format.h>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include "pmem_engine.h"
#include "profiler.h"

namespace NKV {

class Schema;

class DataMovementPlan {
  struct MovementTask {
    char *des = nullptr;
    char *src = nullptr;
    uint32_t size = 0;
    MovementTask(char *des_, char *src_, uint32_t size_)
        : des(des_), src(src_), size(size_) {}
    void apply() { memcpy(des, src, size); }
  };

 public:
  void BuildMovementTask(Schema *schema_ptr) {

  }
  void Apply(){
    for(auto &i : _mov_plan){
      i.apply();
    }
  }

 private:
  std::vector<MovementTask> _mov_plan;
};

class VolatileParser {
 public:
  VolatileParser(Schema *schema_ptr) : _schema_ptr(schema_ptr) {}

 private:
  Schema *_schema_ptr = nullptr;
};

class NonvolatileParser {
 public:
  NonvolatileParser() {}

  std::string encode(const std::vector<Value> &values) {
    std::stringstream data_buf;
    data_buf.clear();
    for (const auto &value : values) {
      data_buf << value.size() << value;
    }
    return data_buf.str();
  }
};

}  // end of namespace NKV