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
#include "kv_type.h"
#include "pmem_engine.h"

namespace NKV {

class Schema;
class MemPool;

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
  static string ParseFromPartialUpdateToRow(Schema *schemaPtr,
                                            PmemAddress pmemAddr,
                                            vector<Value> &fieldValues,
                                            vector<uint32_t> &fields);
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
  bool ExtractFieldFromFullRow(char *rowPtr, uint32_t fieldId, Value &value);

  bool ExtractFieldFromPartialRow(char *rowPtr, uint32_t fieldId, Value &value);

  bool ExtractMultiFieldFromPartialRow(char *rowPtr, vector<uint32_t> &fieldId,
                                       vector<Value> &value);

  bool ExtractFieldFromPmemRow(PmemAddress rowPtr, PmemEngine *enginePtr,
                               uint32_t fieldId, Value &value);

 private:
  Schema *_schemaPtr = nullptr;
};

}  // end of namespace NKV
