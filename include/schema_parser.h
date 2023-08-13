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

class Parser {
 public:
  Parser(MemPool *globalPool) : _globalPool(globalPool) {}

  std::string ParseFromUserWriteToSeq(Schema *schemaPtr,
                                       std::vector<Value> &fieldValues);

  char* ParseFromSeqToTwoPart(Schema *schemaPtr, std::string& seqValue, bool loadVarPartToCache = true);

  std::string ParseFromTwoPartToSeq(Schema *schemaPtr, char* rowFiexdPart, char* rowVarPart);


 private:
  MemPool *_globalPool = nullptr;
};

}  // end of namespace NKV