//
//  schema_parser.cc
//
//  Created by zhenliu on 10/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "schema_parser.h"
#include <bits/stdint-uintn.h>
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
#include "mempool.h"
#include "pmem_engine.h"
#include "profiler.h"
#include "schema.h"
#include "schema_parser.h"

namespace NKV {

std::string SchemaParser::ParseFromUserWriteToSeq(
    Schema *schemaPtr, std::vector<Value> &fieldValues) {
  std::string result;
  uint32_t fixedPartSize =
      schemaPtr->getAllFixedFieldSize();  // record the fixed part size
  uint32_t seqRowSize = fixedPartSize;    // record the row size
  uint32_t userWriteCount = fieldValues.size();
  for (uint32_t i = 0; i < userWriteCount; i++) {
    // add the fixed part
    if (schemaPtr->fieldsMeta[i].isVariable == false) {
      continue;
    }
    uint32_t varFieldSize = fieldValues[i].size();
    if (varFieldSize > sizeof(VarFieldContent::contentData)) {
      seqRowSize += varFieldSize;
    }
    // add the variable part
  }
  result.resize(seqRowSize + sizeof(uint32_t));

  // do the movement
  char *destPtr = result.data();
  *reinterpret_cast<uint32_t *>(destPtr) = seqRowSize;
  destPtr += sizeof(uint32_t);
  char *startPtr = destPtr;
  uint32_t total_size = 0;
  char *varDestPtr = startPtr + fixedPartSize;

  for (uint32_t i = 0; i < userWriteCount; i++) {
    uint32_t fieldSize = schemaPtr->fieldsMeta[i].fieldSize;
    const char *src_ptr = fieldValues[i].c_str();
    // not variable field
    if (schemaPtr->fieldsMeta[i].isVariable == false) {
      memcpy(destPtr, src_ptr, fieldValues[i].size());
      destPtr += fieldSize;
      continue;
    }
    // is variable field, only need to construct the fiexed part
    if (fieldValues[i].size() <= 8) {
      EncodeToVarFieldFullData(destPtr, fieldValues[i].data(),
                               fieldValues[i].size());
      destPtr += schemaPtr->fieldsMeta[i].fieldSize;
    }
    // is variable field whose size is > 8, need to put them to row tail
    if (fieldValues[i].size() > 8) {
      memcpy(varDestPtr, fieldValues[i].data(), fieldValues[i].size());
      EncodeToVarFieldOffset(destPtr, varDestPtr - destPtr,
                             fieldValues[i].size());
      varDestPtr += fieldValues[i].size();
      destPtr += schemaPtr->fieldsMeta[i].fieldSize;
    }
  }
  return result;
}

bool SchemaParser::ParseFromSeqToTwoPart(Schema *schemaPtr,
                                         std::string &seqValue,
                                         bool loadVarPartToCache) {
  uint32_t rowFixedPartSize = schemaPtr->getSize();
  uint32_t rowVarPartSize = seqValue.size() - rowFixedPartSize;

  char *fieldPtr = seqValue.data() + sizeof(uint32_t);
  if (schemaPtr->hasVariableField == false) {
    seqValue.resize(rowFixedPartSize);
    return true;
  }
  if (loadVarPartToCache == false) {
    for (auto &i : schemaPtr->fieldsMeta) {
      if (i.isVariable == true) EncodeToVarFieldNotCache(fieldPtr);
      fieldPtr += i.fieldSize;
    }
    return true;
  }

  // copy the varbaile field content
  bool noSpace = false;
  char *rowVarPartPtr = seqValue.data() + rowFixedPartSize;
  for (auto &i : schemaPtr->fieldsMeta) {
    // not variable field, do nothing
    if (i.isVariable == false) {
      fieldPtr += i.fieldSize;
      continue;
    }
    // meet variable field
    uint32_t varFieldSize = GetVarFieldSize(fieldPtr);
    // data is in the field, no need to move
    if (varFieldSize <= 8) {
      fieldPtr += i.fieldSize;
      continue;
    }
    // there is no space for movement
    if (noSpace == true) {
      EncodeToVarFieldNotCache(fieldPtr);
      fieldPtr += i.fieldOffset;
      continue;
    }

    // let's start to move
    try {
      void *varSpacePtr = _globalPool->Malloc(varFieldSize);
      memcpy(varSpacePtr, rowVarPartPtr, varFieldSize);
      EncodeToVarFieldOnlyPointer(fieldPtr, varSpacePtr, varFieldSize);
    } catch (std::bad_alloc &ba) {
      noSpace = true;
      EncodeToVarFieldNotCache(fieldPtr);
    }
    fieldPtr += i.fieldSize;
  }
  seqValue.resize(rowFixedPartSize);
  if (noSpace == true) return false;
  return true;
}

bool SchemaParser::FreeTwoPartRow(Schema *schemaPtr, char *value) {
  if (schemaPtr->hasVariableField == false) return true;
  uint32_t rowFixedPartSize = schemaPtr->getSize();
  char *fieldPtr = value + sizeof(uint32_t);
  for (auto &i : schemaPtr->fieldsMeta) {
    if (i.isVariable == true) {
      VarFieldContent *varFieldPtr =
          reinterpret_cast<VarFieldContent *>(fieldPtr);
      if (varFieldPtr->contentType == VarFieldType::ONLY_PONTER) {
        _globalPool->Free(varFieldPtr->contentPtr);
      }
    }
    fieldPtr += i.fieldSize;
  }
  return true;
}
bool SchemaParser::ParseFromTwoPartToSeq(Schema *schemaPtr, string &newValue,
                                         char *oldPtr) {
  uint32_t rowFixedPartSize = schemaPtr->getSize();
  assert(oldPtr != nullptr);

  uint32_t rowVarPartSize =
      *(uint32_t *)oldPtr - schemaPtr->getAllFixedFieldSize();

  newValue.resize(rowFixedPartSize + rowVarPartSize);
  // copy the fixed size part
  memcpy(newValue.data(), oldPtr, rowFixedPartSize);

  if (rowVarPartSize == 0) {
    return true;
  }
  char *rowVarPart = newValue.data() + rowFixedPartSize;

  char *newFieldPtr = newValue.data() + sizeof(uint32_t);

  for (auto &i : schemaPtr->fieldsMeta) {
    if (i.isVariable == false) {
      newFieldPtr += i.fieldSize;
      continue;
    }
    VarFieldContent *varFieldPtr =
        reinterpret_cast<VarFieldContent *>(newFieldPtr);
    if (varFieldPtr->contentType == VarFieldType::ONLY_PONTER) {
      // copy the variable field into the row
      memcpy(rowVarPart, varFieldPtr->contentPtr, varFieldPtr->contentSize);
      varFieldPtr->contentOffset = rowVarPart - newFieldPtr;
      varFieldPtr->contentType = VarFieldType::ROW_OFFSET;
      rowVarPart += varFieldPtr->contentSize;
    }
    if (varFieldPtr->contentType == VarFieldType::NOT_CACHE) {
      return false;
    }
    newFieldPtr += i.fieldSize;
  }

  return true;
}

bool ValueReader::ExtractFieldFromPmemRow(PmemAddress rowPtr,
                                          PmemEngine *enginePtr,
                                          uint32_t fieldId, Value &value) {
  const Status s = enginePtr->read(rowPtr, value, _schemaPtr, fieldId);
  return s.is2xxOK();
}

bool ValueReader::ExtractFieldFromRow(char *rowPtr, uint32_t fieldId,
                                      Value &value) {
  // decide if it is variable
  bool isVariable = _schemaPtr->fieldsMeta[fieldId].isVariable;
  if (isVariable == false) {
    uint32_t fieldSize = _schemaPtr->getSize(fieldId);
    value.resize(fieldSize);
    memcpy(value.data(), rowPtr + _schemaPtr->getPBRBOffset(fieldId),
           fieldSize);
    return true;
  }
  // variable field and is smaller than 8B
  VarFieldContent *varFieldPtr = reinterpret_cast<VarFieldContent *>(
      rowPtr + _schemaPtr->getPBRBOffset(fieldId));
  uint32_t fieldSize = varFieldPtr->contentSize;
  VarFieldType fieldType = varFieldPtr->contentType;

  if (fieldType == VarFieldType::FULL_DATA) {
    value.resize(fieldSize);
    memcpy(value.data(), varFieldPtr->contentData, fieldSize);
    return true;
  }
  // store the value in seq row
  if (fieldType == VarFieldType::ROW_OFFSET) {
    value.resize(fieldSize, 0);
    memcpy(value.data(), (char *)varFieldPtr + varFieldPtr->contentOffset,
           fieldSize);
    return true;
  }
  // store the value in the heap space
  if (fieldType == VarFieldType::ONLY_PONTER) {
    value.resize(fieldSize);
    memcpy(value.data(), varFieldPtr->contentPtr, fieldSize);
    return true;
  }
  if (fieldType == VarFieldType::NULL_T) {
    return true;
  }
  // NOT in Cache
  return false;
}

}  // namespace NKV