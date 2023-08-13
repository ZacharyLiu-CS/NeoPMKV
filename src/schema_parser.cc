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

std::string Parser::ParseFromUserWriteToSeq(Schema *schemaPtr,
                                            std::vector<Value> &fieldValues) {
  std::string result;
  uint32_t seqRowSize = 0;     // record the row size
  uint32_t fixedPartSize = 0;  // record the fixed part size
  for (uint32_t i = 0; i < fieldValues.size(); i++) {
    // add the fixed part
    seqRowSize += schemaPtr->fieldsMeta[i].fieldSize;
    fixedPartSize += schemaPtr->fieldsMeta[i].fieldSize;
    if (schemaPtr->fieldsMeta[i].isVariable == false) {
      continue;
    }
    uint32_t varFieldSize = fieldValues[i].size();
    if (varFieldSize <= sizeof(VarFieldContent::contentData)) {
      continue;
    }
    // add the variable part
    seqRowSize += varFieldSize;
  }
  result.resize(seqRowSize + sizeof(uint32_t));

  // do the movement
  char *destPtr = result.data();
  *reinterpret_cast<uint32_t *>(destPtr) = seqRowSize;
  destPtr += sizeof(uint32_t);
  char *startPtr = destPtr;
  uint32_t total_size = 0;
  char *varDestPtr = startPtr + fixedPartSize;

  for (uint32_t i = 0; i < fieldValues.size(); i++) {
    uint32_t size = schemaPtr->fieldsMeta[i].fieldSize;
    const char *src_ptr = fieldValues[i].c_str();
    // not variable field
    if (schemaPtr->fieldsMeta[i].isVariable == false) {
      memcpy(destPtr, src_ptr, size);
      destPtr += size;
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

char *Parser::ParseFromSeqToTwoPart(Schema *schemaPtr, std::string &seqValue,
                                    bool loadVarPartToCache) {
  uint32_t rowFixedPartSize = schemaPtr->size + sizeof(uint32_t);
  uint32_t rowVarPartSize = seqValue.size() - rowFixedPartSize;

  char *fieldPtr = seqValue.data() + sizeof(uint32_t);
  if (rowVarPartSize == 0) {
    seqValue.resize(rowFixedPartSize);
    return nullptr;
  }
  if (loadVarPartToCache == false) {
    for (auto &i : schemaPtr->fieldsMeta) {
      if (i.isVariable == true) {
        EncodeToVarFieldNotCache(fieldPtr);
      }
      fieldPtr += i.fieldSize;
    }
    return nullptr;
  }

  // copy the varbaile field content
  char *rowVarPartPtr = seqValue.data() + rowFixedPartSize;
  char *varSpacePtr = (char *)_globalPool->Malloc(rowVarPartSize);
  memcpy(varSpacePtr, rowVarPartPtr, rowVarPartSize);

  char *varContentPtr = varSpacePtr;
  for (auto &i : schemaPtr->fieldsMeta) {
    if (i.isVariable == true) {
      uint32_t varFieldSize = GetVarFieldSize(fieldPtr);
      // must be pointer or offset
      if (varFieldSize > 8) {
        EncodeToVarFieldOnlyPointer(fieldPtr, varContentPtr, varFieldSize);
        varContentPtr += varFieldSize;
      }
    }
    fieldPtr += i.fieldSize;
  }
  seqValue.resize(rowFixedPartSize);
  return varSpacePtr;
}

std::string Parser::ParseFromTwoPartToSeq(Schema *schemaPtr, char *rowFiexdPart,
                                          char *rowVarPart) {
  std::string res;
  uint32_t rowFixedPartSize = schemaPtr->size + sizeof(uint32_t);
  assert(rowVarPart != nullptr);

  uint32_t rowVarPartSize = *(uint32_t *)rowFiexdPart - schemaPtr->size;
  res.resize(rowFixedPartSize + rowVarPartSize);
  memcpy(res.data(), rowFiexdPart, rowFixedPartSize);
  memcpy(res.data() + rowFixedPartSize, rowVarPart, rowVarPartSize);
  rowVarPart = res.data() + rowFixedPartSize;

  char *fieldPtr = rowFiexdPart + sizeof(uint32_t);

  for (auto &i : schemaPtr->fieldsMeta) {
    if (i.isVariable == false) {
      fieldPtr += i.fieldSize;
      continue;
    }
    VarFieldContent *varFieldPtr =
        reinterpret_cast<VarFieldContent *>(fieldPtr);
    if (varFieldPtr->contentType == VarFieldType::ONLY_PONTER) {
      varFieldPtr->contentPtr = rowVarPart;
      rowVarPart += varFieldPtr->contentSize;
    }

    fieldPtr += i.fieldSize;
  }

  return res;
}

}  // namespace NKV