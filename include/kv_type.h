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
#include <numeric>
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
const uint16_t ERRMASK = UINT16_MAX;
using SchemaId = uint16_t;
using SchemaVer = uint16_t;

struct Key {
  SchemaId schemaId;
  SchemaVer version;
  uint64_t primaryKey;

  Key( SchemaId sid, uint64_t pkValue, SchemaVer ver = 0) {
    schemaId = sid;
    primaryKey = pkValue;
    version = ver;
  }

  bool operator<(const Key &k) const {
    if (this->schemaId < k.schemaId) return true;
    if (this->schemaId > k.schemaId) return false;
    if (this->primaryKey < k.primaryKey) return true;
    if (this->primaryKey > k.primaryKey) return false;
    if (this->version < k.version) return true;
    if (this->version > k.version) return false;
    return true;
  }
  SchemaId getSchemaId()const{return schemaId;}
  SchemaVer getVersion()const{return version;}
  uint64_t getPrimaryKey()const{return primaryKey;}
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
  // identify the previous record count
  // 0 : full record, no prev record
  // 1 ~ N : partial record, has prev records
  std::atomic_uint8_t _prevItemCount{0};
  std::atomic_bool _isHot{false};

 public:
  TimeStamp getTimestamp() const {
    return _timestamp.load(std::memory_order_acquire);
  }

  PmemAddress getPmemAddr() const { return _pmemAddr; }

  RowAddr getPBRBAddr() const { return _pbrbAddr; }

  std::pair<bool, TimeStamp> getHotStatus() const;

  bool isHot() const;

  void setFullColdPmemAddr(PmemAddress pmAddr, TimeStamp newTS = TimeStamp());

  void setPartialColdPmemAddr(PmemAddress pmAddr,
                              TimeStamp newTS = TimeStamp());

  uint8_t getPrevItemCount() const {
    return _prevItemCount.load(std::memory_order_relaxed);
  }
  bool isFullRecord() const {
    return _prevItemCount.load(std::memory_order_relaxed) == 0;
  }

  void evictToCold();

  bool setHotTimeStamp(TimeStamp oldTS, TimeStamp newTS);

  bool setHotPBRBAddr(RowAddr rowAddr, TimeStamp oldTS, TimeStamp newTS);
};

}  // namespace NKV

template <>
struct fmt::formatter<NKV::Key> {
  constexpr auto parse(format_parse_context &ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  // Formats the point p using the parsed format specification (presentation)
  // stored in this formatter.
  template <typename FormatContext>
  auto format(const NKV::Key &key, FormatContext &ctx) const
      -> decltype(ctx.out()) {
    // ctx.out() is an output iterator to write to.
    return fmt::format_to(ctx.out(), "schemaid:{}, pkey:{}, version:{} ",
                          key.schemaId, key.primaryKey, key.version);
  }
};
template <>
struct fmt::formatter<NKV::Value> {
  constexpr auto parse(format_parse_context &ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  // Formats the point p using the parsed format specification (presentation)
  // stored in this formatter.
  template <typename FormatContext>
  auto format(const NKV::Value &value, FormatContext &ctx) const
      -> decltype(ctx.out()) {
    // ctx.out() is an output iterator to write to.
    return fmt::format_to(ctx.out(), "{}", value);
  }
};

template <>
struct fmt::formatter<NKV::ValuePtr> {
  constexpr auto parse(format_parse_context &ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  // Formats the point p using the parsed format specification (presentation)
  // stored in this formatter.
  template <typename FormatContext>
  auto format(const NKV::ValuePtr &valuePtr, FormatContext &ctx) const
      -> decltype(ctx.out()) {
    // ctx.out() is an output iterator to write to.
    return fmt::format_to(
        ctx.out(), "Hot: {} PmemAddr: {}, PBRBAddr:{} TS: {} PrevCount: {}",
        valuePtr.isHot(), valuePtr.getPmemAddr(),
        (uint64_t)valuePtr.getPBRBAddr(), valuePtr.getTimestamp(),
        valuePtr.getPrevItemCount());
  }
};
