//
//  schema.h
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <bits/stdint-uintn.h>
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
#include "kv_type.h"
#include "mempool.h"
#include "profiler.h"
#include "timestamp.h"

namespace NKV {

using SchemaId = uint16_t;
using SchemaVer = uint16_t;
using std::vector;

enum RowType : uint16_t {
  FULL_FIELD = 0,
  FULL_DATA,
  PARTIAL_FIELD,
};
// Sequential Row format:
// | Row Meta Head |  field0 content   |   field1 content |
//  <---- 8B ---->  <-- field size -->  <-- field size -->
struct RowMetaHead {
 private:
  uint16_t rowSize;
  RowType rowType;
  SchemaId schemaId;
  SchemaVer schemaVersion;

 public:
  void setMeta(uint16_t rSize, RowType rType, SchemaId sId, SchemaVer sVersion);
  uint16_t getSize() { return rowSize; }
  RowType getType() { return rowType; }
  SchemaId getSchemaId() { return schemaId; }
  SchemaVer getSchemaVer() { return schemaVersion; }
} __attribute__((packed));

const uint32_t ROW_META_HEAD_SIZE = sizeof(RowMetaHead);

inline RowMetaHead *RowMetaPtr(char *src) {
  return reinterpret_cast<RowMetaHead *>(src);
}
inline char *skipRowMeta(char *src) { return src + ROW_META_HEAD_SIZE; }

// Partial Row format
// | Row Meta Head |  Partial  Row   Meta |  field0 content  |   field1 content
// |
//  <--- 8B  --->   <-- viarbale size -->  <-- field size -->  <-- field size
//  -->
struct PartialRowMeta {
  PmemAddress prevRow;
  uint8_t metaSize;
  uint8_t fieldCount;
  uint8_t fieldArr[0];

 public:
  void setMeta(PmemAddress pRow, uint8_t mSize, vector<uint32_t> &fArr);
  PmemAddress getPmemAddr() { return prevRow; }
  uint8_t getMetaSize() { return metaSize; }
  uint8_t getFieldCount() { return fieldCount; }
  static uint32_t CalculateSize(uint32_t fieldCount);
} __attribute__((packed));

const uint32_t PARTIAL_ROW_META_SIZE = sizeof(PartialRowMeta);
inline PartialRowMeta *PartialRowMetaPtr(char *src) {
  return reinterpret_cast<PartialRowMeta *>(src);
}

inline char *skipPartialRowMeta(char *src) {
  return src + PARTIAL_ROW_META_SIZE + PartialRowMetaPtr(src)->fieldCount;
}

struct SchemaField {
  FieldType type;
  std::string name;
  uint32_t size = 0;

  SchemaField() = delete;
  SchemaField(FieldType type_, std::string name_, uint32_t size_)
      : type(type_), name(name_), size(size_) {
    // assert(size % 4 == 0);
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

class Schema;

class ParitalSchema {
 private:
  uint32_t allFieldSize;
  bool hasVariableField = false;
  std::unordered_map<uint32_t, uint32_t> fMap;

 public:
  ParitalSchema(Schema *fullSchemaPtr, uint8_t* fields, uint8_t fieldCount);
  uint32_t getOffset(uint32_t schemaId) { return fMap[schemaId]; }
  bool checkExisted(uint32_t schemaId) { return fMap.find(schemaId) != fMap.end(); }
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
  uint32_t allFieldSize = 0;
  // all field attributes
  // meta data of field in storage
  bool hasVariableField = false;
  std::vector<SchemaField> fields;
  std::vector<FieldMetaData> fieldsMeta;

  Schema(std::string name, uint32_t schemaId, uint32_t primaryKeyField,
         std::vector<SchemaField> &fields);

  Schema buildPartialSchema(PartialRowMeta *partialMetaPtr);

  uint32_t getAllFieldSize() { return allFieldSize; }

  uint32_t getSize() { return size; }

  inline uint32_t getSize(uint32_t fieldId) {
    return fieldsMeta[fieldId].fieldSize;
  }
  inline FieldType getFieldType(uint32_t fieldId) {
    return fields[fieldId].type;
  }
  uint32_t getFieldId(const std::string &fieldName);

  inline uint32_t getPmemOffset(uint32_t fieldId) {
    return fieldsMeta[fieldId].fieldOffset;
  }
  inline uint32_t getPmemOffset(const std::string &fieldName) {
    return getPmemOffset(getFieldId(fieldName));
  }

  inline uint32_t getPBRBOffset(uint32_t fieldId) {
    return fieldsMeta[fieldId].fieldOffset;
  }
  inline uint32_t getPBRBOffset(const std::string &fieldName) {
    return getPBRBOffset(getFieldId(fieldName));
  }
};

class SchemaAllocator {
  std::atomic_uint32_t idx{1};

 public:
  Schema CreateSchema(std::string name, uint32_t primaryKeyField,
                      std::vector<SchemaField> &fields) {
    return Schema(name, idx.fetch_add(1), primaryKeyField, fields);
  }
  void clear() { idx.store(1); }
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
  void clear() { _umap.clear(); }

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
