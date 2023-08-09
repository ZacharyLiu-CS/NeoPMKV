//
//  schema.h
//  PROJECT schema
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
#include "profiler.h"
#include "timestamp.h"

namespace NKV {

using SchemaId = uint32_t;
using SchemaVer = uint16_t;

using PmemAddress = uint64_t;
using PmemSize = uint64_t;
const uint32_t PmemEntryHead = sizeof(uint32_t);
const uint32_t FieldHeadSize = sizeof(uint32_t);
using RowAddr = void *;
const uint32_t ERRMASK = 1 << 31;

struct Key {
  uint32_t schemaId;
  uint64_t primaryKey;

  // template <typename T>
  // Key(uint32_t schemaId, T pkValue) {
  //   this->schemaId = schemaId;
  //   generatePK(pkValue);
  // }
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
  ValuePtr(PmemAddress pmAddr, TimeStamp ts) {
    _pmemAddr = pmAddr;
    _timestamp.store(ts, std::memory_order_release);
    _isHot.store(false, std::memory_order_release);
  }
  ValuePtr(RowAddr rowAddr, TimeStamp ts) {
    _pbrbAddr = rowAddr;
    _timestamp.store(ts, std::memory_order_release);
    _isHot.store(true, std::memory_order_release);
  }

  ~ValuePtr() {}

  ValuePtr(const ValuePtr &valuePtr) {
    _pmemAddr = valuePtr._pmemAddr;
    _pbrbAddr = valuePtr._pbrbAddr;
    _timestamp.store(valuePtr._timestamp, std::memory_order_release);
    _isHot.store(valuePtr._isHot.load(std::memory_order_acquire),
                 std::memory_order_release);
  }

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

  std::pair<bool, TimeStamp> getHotStatus() const {
    return {_isHot.load(std::memory_order_acquire),
            _timestamp.load(std::memory_order_acquire)};
  }
  bool isHot() const { return _isHot.load(std::memory_order_acquire); }

  void setColdPmemAddr(PmemAddress pmAddr, TimeStamp newTS = TimeStamp()) {
    _pmemAddr = pmAddr;
    _timestamp.store(newTS, std::memory_order_release);
    _isHot.store(false, std::memory_order_release);
  }

  void evictToCold() { _isHot.store(false, std::memory_order_release); }

  bool setHotTimeStamp(TimeStamp oldTS, TimeStamp newTS) {
    if (_timestamp.compare_exchange_weak(oldTS, newTS) == false) {
      return false;
    }
    _isHot.store(true, std::memory_order_release);
    return true;
  }

  bool setHotPBRBAddr(RowAddr rowAddr, TimeStamp oldTS, TimeStamp newTS) {
    this->_pbrbAddr = rowAddr;
    if (_timestamp.compare_exchange_weak(oldTS, newTS) == false) {
      return false;
    }
    _isHot.store(true, std::memory_order_release);
    return true;
  }
};

enum class FieldType : uint8_t {
  NULL_T = 0,
  INT16T,
  INT32T,
  INT64T,
  FLOAT,   // Not supported as key field for now
  DOUBLE,  // Not supported as key field for now
  BOOL,
  STRING,  // fixed characters in string is OK
  VARSTR,  // variable str which can be changed in the subsequent progress
};

const int32_t FTSize[256] = {
    0,                // NULL_T = 0,
    sizeof(int16_t),  // INT16T,
    sizeof(int32_t),  // INT32T,
    sizeof(int64_t),  // INT64T,
    sizeof(float),    // FLOAT, // Not supported as key field for now
    sizeof(double),   // DOUBLE,  // Not supported as key field for now
    sizeof(bool),     // BOOL,
    8,                // default size of string
    12,               // allocate 8 bytes default,
                      //      if the size is smaller than 8 bytes:
                      //            it is stored in the row
                      //      else:
                      //            store the ptr and point to the heap
};
enum class VariableFieldType : uint8_t {
  NULL_T = 0,
  FULL_DATA,
  ONLY_PONTER,
};

struct VarStrFieldContent {
  uint32_t contentSizeAndType;
  union {
    char contentData[8];
    void *contentPtr;
  };
  void encode(void *content, uint32_t size) {
    VariableFieldType type_ = VariableFieldType::NULL_T;
    if (size <= 8 && size > 0) type_ = VariableFieldType::FULL_DATA;
    if (size > 8) type_ = VariableFieldType::ONLY_PONTER;

    contentSizeAndType = (size | ((uint8_t)type_ << 16));

    if (type_ == VariableFieldType::FULL_DATA) {
      std::memcpy(contentData, content, size);
    } else {
      contentPtr = content;
    }
  }
  uint32_t getSize() {
    return (uint16_t)(contentSizeAndType);
  }
  VariableFieldType getType() {
    return (VariableFieldType)(contentSizeAndType >> 16);
  }
  auto getContent() {
    if (getType() == VariableFieldType::FULL_DATA) {
      return contentData;
    } else {
      return (char *)contentPtr;
    }
  }
} __attribute__((packed));

struct SchemaField {
  FieldType type;
  std::string name;
  uint32_t size = 0;

  SchemaField() = delete;
  SchemaField(FieldType type_, std::string name_, uint32_t size_)
      : type(type_), name(name_), size(size_) {
    assert(size % 4 == 0);
    if (type_ == FieldType::VARSTR) {
      size = FTSize[(uint8_t)type_];
    }
  }

  SchemaField(FieldType type, std::string name) {
    this->type = type;
    this->name = name;
    this->size = FTSize[(uint8_t)type];
  }
};

struct FieldMetaData {
  uint32_t fieldSize;
  uint32_t fieldOffset;
  bool isNullable;
  bool isVariable;
};

struct Schema {
  // define the schema name
  std::string name;
  SchemaVer version = 0;
  uint32_t schemaId = ERRMASK;
  // the primary key field id
  uint32_t primaryKeyField = 0;
  // define the schema data size
  uint32_t size = 0;
  // all field attributes
  // meta data of field in storage
  bool hasVariableField = false;
  std::vector<SchemaField> fields;
  std::vector<FieldMetaData> fieldsMeta;

  Schema(std::string name, uint32_t schemaId, uint32_t primaryKeyField,
         std::vector<SchemaField> &fields)
      : name(name),
        version(0),
        schemaId(schemaId),
        primaryKeyField(primaryKeyField),
        fields(fields) {
    (void)getSize();
    uint32_t currentOffset = 0;
    for (auto i : fields) {
      if (i.type == FieldType::VARSTR) {
        hasVariableField = true;
      }
      fieldsMeta.push_back(FieldMetaData());
      auto &field_meta = fieldsMeta.back();
      field_meta.fieldSize = i.size;
      field_meta.fieldOffset = currentOffset;
      field_meta.isNullable = false;
      field_meta.isVariable = false;
      currentOffset += i.size + FieldHeadSize;
    }
  }
  uint32_t getSize() {
    if (size != 0) return size;
    // Initializa the size
    for (auto i : fields) size += i.size + FieldHeadSize;
    return size;
  }
  inline uint32_t getSize(uint32_t fieldId) {
    return fieldsMeta[fieldId].fieldSize;
  }

  inline uint32_t getPmemOffset(uint32_t fieldId) {
    return fieldsMeta[fieldId].fieldOffset + PmemEntryHead;
  }
  inline uint32_t getPmemOffset(const std::string &fieldName) {
    uint32_t fieldId = 0;
    for (auto i : fields) {
      if (i.name == fieldName) break;
      fieldId += 1;
    }
    return getPmemOffset(fieldId);
  }

  inline uint32_t getPBRBOffset(uint32_t fieldId) {
    return fieldsMeta[fieldId].fieldOffset;
  }
  inline uint32_t getPBRBOffset(const std::string &fieldName) {
    uint32_t fieldId = 0;
    for (auto i : fields) {
      if (i.name == fieldName) break;
      fieldId += 1;
    }
    return getPBRBOffset(fieldId);
  }
};

class SchemaAllocator {
  std::atomic_uint32_t idx{1};

 public:
  Schema createSchema(std::string name, uint32_t primaryKeyField,
                      std::vector<SchemaField> &fields) {
    return Schema(name, idx.fetch_add(1), primaryKeyField, fields);
  }
};

class SchemaUMap {
 private:
  std::unordered_map<SchemaId, Schema> _umap;
  std::mutex _mutex;

 public:
  void addSchema(Schema schema) {
    std::lock_guard<std::mutex> guard(_mutex);
    _umap.insert({schema.schemaId, schema});
  }

  uint32_t getSchemaID(std::string schemaName) {
    // std::lock_guard<std::mutex> guard(_mutex);
    for (auto &item : _umap) {
      if (item.second.name == schemaName) return item.first;
    }
    return 0;
  }
  Schema *find(SchemaId schemaId) {
    // std::lock_guard<std::mutex> guard(_mutex);
    auto it = _umap.find(schemaId);
    return it == _umap.end() ? nullptr : &(it->second);
  }

  decltype(_umap.begin()) begin() { return _umap.begin(); }
  decltype(_umap.end()) end() { return _umap.end(); }
};

}  // end of namespace NKV

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
    return fmt::format_to(ctx.out(), "schemaid:{},pkey:{}", key.schemaId,
                          key.primaryKey);
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
    return fmt::format_to(ctx.out(), "Hot: {} PmemAddr: {}, PBRBAddr:{} TS: {}",
                          valuePtr.isHot(), valuePtr.getPmemAddr(),
                          (uint64_t)valuePtr.getPBRBAddr(),
                          valuePtr.getTimestamp());
  }
};
