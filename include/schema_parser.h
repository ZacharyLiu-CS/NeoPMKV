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
#include <queue>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include "kv_type.h"
#include "schema.h"

namespace NKV {

class Schema;
class MemPool;
class PmemEngine;

using Value = std::string;
using std::queue;
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
  static bool ParseFromTwoPartToSeq(Schema *schemaPtr, Value &seqValue,
                                    char *rowPtr);
  // the partial values : front [ new partial value1, new partial value2 , old
  // full value3 ] end
  static bool MergePartialUpdateToFullRow(Schema *schemaPtr, Value &seqValue,
                                          vector<Value> &partialValues);


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

  RowType ExtractRowTypeFromRow(char *rowPtr);

  PmemAddress ExtractPrevRowFromPartialRow(char *rowPtr);

  SchemaVer ExtractVersionFromRow(char *rowPtr);

 private:
  Schema *_schemaPtr = nullptr;
};

}  // end of namespace NKV
