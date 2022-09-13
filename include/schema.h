//
//  schema.h
//  PROJECT schema
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include "timestamp.h"

namespace NKV {

using SchemaId = uint32_t;
using SchemaVer = uint16_t;

using PmemAddress = uint64_t;
using PmemSize = uint64_t;

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
  Key(uint32_t schemaId, uint64_t pkValue){
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
  // TODO: need add judgement of type: check whether pod type
  // template <typename T,
  //           typename = typename std::enable_if<std::is_pod<T>::value>::type>
  // void generatePK(T pkValue) {
  //   primaryKey.resize(sizeof(T));
  //   *(T *)primaryKey.data() = pkValue;
  // }

  // void generatePK(std::string &pkValue) { primaryKey = pkValue; }
};

using Value = std::string;

class ValuePtr {
 public:
  ValuePtr() {
    updateLock_ = new std::mutex;
  }

  ~ValuePtr() {
    delete updateLock_;
  }

  ValuePtr(const ValuePtr &valuePtr) {
    updateLock_ = new std::mutex;
    _timestamp = valuePtr._timestamp;
    _addr = valuePtr._addr;
    _isHot = valuePtr._isHot;
  }

 private:
  std::mutex *updateLock_ = nullptr;
  TimeStamp _timestamp;
  union Addr {
    PmemAddress pmemAddr;
    RowAddr pbrbAddr;
  } _addr;
  bool _isHot = false;

 public:
  TimeStamp getTimestamp() { return _timestamp; }

  PmemAddress getPmemAddr() { return _addr.pmemAddr; }

  RowAddr getPBRBAddr() { return _addr.pbrbAddr; }

  bool isHot() { return _isHot; }

  void updateTS() { this->_timestamp.getNow(); }
  void updateTS(TimeStamp newTS) { this->_timestamp = newTS; }

  void updatePmemAddr(PmemAddress pmAddr, TimeStamp newTS) {
    std::lock_guard<std::mutex> lock(*updateLock_);
    this->_isHot = false;
    this->_addr.pmemAddr = pmAddr;
    this->_timestamp = newTS;
  }
  void updatePmemAddr(PmemAddress pmAddr) {
    std::lock_guard<std::mutex> lock(*updateLock_);
    this->_isHot = false;
    this->_addr.pmemAddr = pmAddr;
    this->_timestamp.getNow();
  }

  void updatePBRBAddr(RowAddr rowAddr, TimeStamp newTS) {
    std::lock_guard<std::mutex> lock(*updateLock_);
    this->_isHot = true;
    this->_addr.pbrbAddr = rowAddr;
    this->_timestamp = newTS;
  }
  void updatePBRBAddr(RowAddr rowAddr) {
    std::lock_guard<std::mutex> lock(*updateLock_);
    this->_isHot = true;
    this->_addr.pbrbAddr = rowAddr;
    this->_timestamp.getNow();
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
  STRING,  // NULL characters in string is OK
};

const uint32_t FTSize[256] = {
    0,                // NULL_T = 0,
    sizeof(int16_t),  // INT16T,
    sizeof(int32_t),  // INT32T,
    sizeof(int64_t),  // INT64T,
    sizeof(float),    // FLOAT, // Not supported as key field for now
    sizeof(double),   // DOUBLE,  // Not supported as key field for now
    sizeof(bool),     // BOOL,
    64,
};

struct SchemaField {
  FieldType type;
  std::string name;
  uint32_t size = 0;

  SchemaField() = delete;
  SchemaField(FieldType type, std::string name, uint32_t size) {
    this->type = type;
    this->name = name;
    this->size = size;
    // if (type == FieldType::STRING) {
    //   uint32_t maxSize = 1 << 20;
    //   if (size > maxSize)
    //     this->size = maxSize;
    //   else
    //     this->size = 1U << (32 - __builtin_clz(size - 1));
    // } else
    //   size = FTSize[(uint8_t)type];
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
  std::string name;
  SchemaVer version = 0;
  uint32_t schemaId = ERRMASK;
  uint32_t primaryKeyField = 0;
  std::vector<SchemaField> fields;
  uint32_t size = 0;
  Schema(std::string name, uint32_t schemaId, uint32_t primaryKeyField,
         std::vector<SchemaField> &fields)
      : name(name),
        version(0),
        schemaId(schemaId),
        primaryKeyField(primaryKeyField),
        fields(fields) {
    (void)getSize();
  }
  uint32_t getSize() {
    if (size != 0) {
      return size;
    }
    // Initializa the size
    for (auto i : fields) {
      size += i.size;
    }
    return size;
  }
};

struct SchemaAllocator {
  std::atomic_uint32_t idx{1};
  Schema createSchema(std::string name, uint32_t primaryKeyField,
                      std::vector<SchemaField> &fields) {
    return Schema(name, idx.fetch_add(1), primaryKeyField, fields);
  }
};
class SchemaUMap {
 private:
  std::unordered_map<SchemaId, Schema> _umap;
  std::mutex mutex_;

 public:
  void addSchema(Schema schema) {
    std::lock_guard<std::mutex> guard(mutex_);
    _umap.insert({schema.schemaId, schema});
  }

  uint32_t getSchemaID(std::string schemaName) {
    std::lock_guard<std::mutex> guard(mutex_);
    for (auto &item : _umap) {
      if (item.second.name == schemaName) return item.first;
    }
    return 0;
  }
  Schema *find(SchemaId schemaId) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = _umap.find(schemaId);
    return it == _umap.end() ? nullptr : &(it->second);
  }
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
