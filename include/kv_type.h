//
//  kv_type.h
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include "field_type.h"
#include "timestamp.h"

namespace NKV {

using PmemAddress = uint64_t;
using PmemSize = uint64_t;
using RowAddr = void *;
const uint32_t ERRMASK = 1 << 31;

struct Key {
  uint32_t schemaId;
  uint64_t primaryKey;

  Key(uint32_t schemaId, uint64_t pkValue) {
    this->schemaId = schemaId;
    primaryKey = pkValue;
  }

  bool operator<(const Key &k) const {
    if (this->schemaId < k.schemaId)
      return true;
    else if (this->schemaId == k.schemaId && this->primaryKey < k.primaryKey)
      return true;
    return false;
  }
};

using Value = std::string;

class ValuePtr {
 public:
  ValuePtr() {}
  ValuePtr(PmemAddress pmAddr, TimeStamp ts);
  ValuePtr(RowAddr rowAddr, TimeStamp ts);
  ~ValuePtr() {}
  ValuePtr(const ValuePtr &valuePtr);

 private:
  PmemAddress _pmemAddr = 0;
  RowAddr _pbrbAddr = 0;
  std::atomic<TimeStamp> _timestamp{{0}};
  std::atomic_bool _isHot{false};

 public:
  TimeStamp getTimestamp() const {
    return _timestamp.load(std::memory_order_acquire);
  }

  PmemAddress getPmemAddr() const { return _pmemAddr; }

  RowAddr getPBRBAddr() const { return _pbrbAddr; }

  std::pair<bool, TimeStamp> getHotStatus() const;

  bool isHot() const;

  void setColdPmemAddr(PmemAddress pmAddr, TimeStamp newTS = TimeStamp());
  void evictToCold();

  bool setHotTimeStamp(TimeStamp oldTS, TimeStamp newTS);

  bool setHotPBRBAddr(RowAddr rowAddr, TimeStamp oldTS, TimeStamp newTS);
};

}  // namespace NKV