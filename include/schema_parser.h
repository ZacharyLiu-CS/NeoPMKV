//
//  schema_parser.h
//
//  Created by zhenliu on 09/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <bits/stdint-uintn.h>
#include <fmt/format.h>
#include <sys/types.h>
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
#include "schema.h"

namespace NKV {

class Schema;
class MemPool;

enum MovementTpye : uint8_t {
  NO_CHANGE = 0,
  FROM_USER_TO_SEQ,
  FROM_SEQ_TO_CACHE,
  FROM_CACHE_TO_SEQ,
};

using Value = std::string;
using std::string;
using std::vector;

class SchemaParser {
 public:
  SchemaParser() { _globalPool = nullptr; }

  SchemaParser(MemPool *globalPool = nullptr) : _globalPool(globalPool) {}

  void Construct(MemPool *globalPool) { _globalPool = globalPool; }

  // parse from the user write to the string
  static string ParseFromUserWriteToSeq(Schema *schemaPtr,
                                        vector<Value> &fieldValues);
  // not only parse from the seq format, but also use memory pool to allocate
  // space and copy the variable part into it
  // not move the seqValue, just shrink the space
  bool ParseFromSeqToTwoPart(Schema *schemaPtr, Value &seqValue,
                             bool loadVarPartToCache = true);

  bool FreeTwoPartRow(Schema *schemaPtr, char *value);

  // combine the fixed part and variable part, and move the data into a new one
  static bool ParseFromTwoPartToSeq(Schema *schemaPtr, string &seqValue,
                                    char *rowPtr);

 private:
  MemPool *_globalPool = nullptr;
};

class ValueReader {
 public:
  ValueReader(Schema *schemaPtr) : _schemaPtr(schemaPtr) {}
  bool ExtractFieldFromRow(char *rowPtr, uint32_t fieldId, Value &value);

  bool ExtractFieldFromPmemRow(PmemAddress rowPtr, PmemEngine *enginePtr,
                               uint32_t fieldId, Value &value);

 private:
  Schema *_schemaPtr = nullptr;
};

}  // end of namespace NKV