//
//  schema.h
//  PROJECT schema
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
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
  std::string primaryKey;
};

using Value = std::string;

struct ValuePtr {
  TimeStamp timestamp;
  union Addr {
    PmemAddress pmemAddr;
    RowAddr pbrbAddr;
  } addr;
  bool isHot;
};

enum class FieldType : uint8_t {
  NULL_T = 0,
  STRING,  // NULL characters in string is OK
  INT16T,
  INT32T,
  INT64T,
  FLOAT,   // Not supported as key field for now
  DOUBLE,  // Not supported as key field for now
  BOOL,
  FIELD_TYPE,  // The value refers to one of these types. Used in query filters.
  NOT_KNOWN = 254,
  NULL_LAST = 255
};

const uint32_t FTSize[256] = {
    0,                // NULL_T = 0,
    128,              // 128 STRING, // NULL characters in string is OK
    sizeof(int16_t),  // INT16T,
    sizeof(int32_t),  // INT32T,
    sizeof(int64_t),  // INT64T,
    sizeof(float),    // FLOAT, // Not supported as key field for now
    sizeof(double),   // DOUBLE,  // Not supported as key field for now
    sizeof(bool),     // BOOL,
    1  // FIELD_TYPE, // The value refers to one of these types. Used in query
       // filters. NOT_KNOWN = 254, NULL_LAST = 255
};

struct SchemaField {
  FieldType type;
  std::string name;
  uint32_t getSize() { return FTSize[(uint8_t)type]; }
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
      size += FTSize[(uint8_t)i.type];
    }
    return size;
  }
};

struct SchemaAllocator {
  uint32_t idx = 0;
  std::mutex mutex_;
  Schema createSchema(std::string name, uint32_t primaryKeyField,
                      std::vector<SchemaField> &fields) {
    std::lock_guard<std::mutex> guard(mutex_);
    idx += 1;
    return Schema(name, idx, primaryKeyField, fields);
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
