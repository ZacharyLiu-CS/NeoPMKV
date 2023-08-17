//
//  schema.cc
//
//  Created by zhenliu on 17/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

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
#include "mempool.h"
#include "profiler.h"
#include "timestamp.h"
#include "schema.h"

namespace NKV {

  // ValuePtr part
 ValuePtr::ValuePtr(PmemAddress pmAddr, TimeStamp ts) {
    _pmemAddr = pmAddr;
    _timestamp.store(ts, std::memory_order_release);
    _isHot.store(false, std::memory_order_release);
  }

  ValuePtr::ValuePtr(RowAddr rowAddr, TimeStamp ts) {
    _pbrbAddr = rowAddr;
    _timestamp.store(ts, std::memory_order_release);
    _isHot.store(true, std::memory_order_release);
  }

  ValuePtr::ValuePtr(const ValuePtr &valuePtr) {
    _pmemAddr = valuePtr._pmemAddr;
    _pbrbAddr = valuePtr._pbrbAddr;
    _timestamp.store(valuePtr._timestamp, std::memory_order_release);
    _isHot.store(valuePtr._isHot.load(std::memory_order_acquire),
                 std::memory_order_release);
  }

  std::pair<bool, TimeStamp> ValuePtr::getHotStatus() const {
    return {_isHot.load(std::memory_order_acquire),
            _timestamp.load(std::memory_order_acquire)};
  }
  bool ValuePtr::isHot() const { return _isHot.load(std::memory_order_acquire); }

  void ValuePtr::setColdPmemAddr(PmemAddress pmAddr, TimeStamp newTS) {
    _pmemAddr = pmAddr;
    _timestamp.store(newTS, std::memory_order_release);
    _isHot.store(false, std::memory_order_release);
  }

  void ValuePtr::evictToCold() { _isHot.store(false, std::memory_order_release); }

  bool ValuePtr::setHotTimeStamp(TimeStamp oldTS, TimeStamp newTS) {
    if (_timestamp.compare_exchange_weak(oldTS, newTS) == false) {
      return false;
    }
    _isHot.store(true, std::memory_order_release);
    return true;
  }
  bool ValuePtr::setHotPBRBAddr(RowAddr rowAddr, TimeStamp oldTS, TimeStamp newTS) {
    this->_pbrbAddr = rowAddr;
    if (_timestamp.compare_exchange_weak(oldTS, newTS) == false) {
      return false;
    }
    _isHot.store(true, std::memory_order_release);
    return true;
  }


  // Schema function part
    Schema::Schema(std::string name, uint32_t schemaId, uint32_t primaryKeyField,
         std::vector<SchemaField> &fields)
      : name(name),
        version(0),
        schemaId(schemaId),
        primaryKeyField(primaryKeyField),
        fields(fields) {
    size += AllFieldHeadSize;
    for (auto i : fields) {
      fieldsMeta.push_back(FieldMetaData());
      auto &field_meta = fieldsMeta.back();
      field_meta.fieldSize = i.size;
      field_meta.fieldOffset = size;
      field_meta.isNullable = false;
      field_meta.isVariable = false;
      size += i.size;
      fixedFieldSize += i.size;
      if (i.type == FieldType::VARSTR) {
        hasVariableField = true;
        field_meta.isVariable = true;
      }
    }
  }
   uint32_t Schema::getFieldId(const std::string &fieldName) {
    uint32_t fieldId = 0;
    for (auto &i : fields) {
      if (i.name == fieldName) break;
      fieldId += 1;
    }
    return fieldId;
  }
} // end of namespace NKV