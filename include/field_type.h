//
//  field_type.h
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
#include "mempool.h"

namespace NKV {

enum class FieldType : uint8_t {
  NULL_T = 0,
  INT16T,
  INT32T,
  INT64T,
  FLOAT,   // Not supported as key field for now
  DOUBLE,  // Not supported as key field for now
  BOOL,
  STRING,  // fixed characters in string is OK
  VARSTR,  // variable str which can be changed in the subsequent progress
};

const int32_t FTSize[256] = {
    0,                // NULL_T = 0,
    sizeof(int16_t),  // INT16T,
    sizeof(int32_t),  // INT32T,
    sizeof(int64_t),  // INT64T,
    sizeof(float),    // FLOAT, // Not supported as key field for now
    sizeof(double),   // DOUBLE,  // Not supported as key field for now
    sizeof(bool),     // BOOL,
    16,               // default size of string
    12,               // allocate 8 bytes default,
                      //      if the size is smaller than 8 bytes:
                      //            it is stored in the row
                      //      else:
                      //            store the ptr and point to the heap
};

enum class VarFieldType : uint16_t {
  NULL_T = 0,
  NOT_CACHE,
  FULL_DATA,
  ONLY_POINTER,
  ROW_OFFSET,
};

struct VarFieldContent {
  uint16_t contentSize;
  VarFieldType contentType;
  union {
    char contentData[8];
    void *contentPtr;
    uint64_t contentOffset;
  };
} __attribute__((packed));
inline VarFieldContent *VarFieldPtr(char *src) {
  return reinterpret_cast<VarFieldContent *>(src);
}
// encode the variable field in cache (two part row) with  full data or pointer
inline void EncodeToVarFieldFullData(void *dst, void *content, uint32_t size) {
  VarFieldContent *dstPtr = reinterpret_cast<VarFieldContent *>(dst);
  assert(size <= 8);
  VarFieldType type_ = VarFieldType::NULL_T;
  if (size != 0) type_ = VarFieldType::FULL_DATA;

  dstPtr->contentType = type_;
  dstPtr->contentSize = size;
  std::memcpy(dstPtr->contentData, content, size);
}

inline void EncodeToVarFieldNotCache(void *dst) {
  VarFieldContent *dstPtr = reinterpret_cast<VarFieldContent *>(dst);

  dstPtr->contentType = VarFieldType::NOT_CACHE;
  dstPtr->contentOffset = 0;
}
inline void EncodeToVarFieldOnlyPointer(void *dst, void *content,
                                        uint32_t size) {
  VarFieldContent *dstPtr = reinterpret_cast<VarFieldContent *>(dst);
  VarFieldType type_ = VarFieldType::ONLY_POINTER;
  assert(size > 8);
  dstPtr->contentType = type_;
  dstPtr->contentSize = size;
  dstPtr->contentPtr = content;
}
// encode the variable field in seq row
inline void EncodeToVarFieldOffset(void *dst, uint64_t contentOffset,
                                   uint32_t size) {
  VarFieldContent *dstPtr = reinterpret_cast<VarFieldContent *>(dst);
  VarFieldType type_ = VarFieldType::ROW_OFFSET;
  assert(size > 8);
  dstPtr->contentType = type_;
  dstPtr->contentSize = size;
  dstPtr->contentOffset = contentOffset;
}

inline uint32_t GetVarFieldSize(void *dst) {
  return reinterpret_cast<VarFieldContent *>(dst)->contentSize;
}
inline VarFieldType GetVarFieldType(void *dst) {
  return reinterpret_cast<VarFieldContent *>(dst)->contentType;
}
inline auto GetVarFieldContent(void *dst) {
  if (GetVarFieldType(dst) == VarFieldType::FULL_DATA) {
    return reinterpret_cast<VarFieldContent *>(dst)->contentData;
  } else {
    return (char *)reinterpret_cast<VarFieldContent *>(dst)->contentPtr;
  }
}
inline bool FreeVarFieldContent(void *dst, MemPool *poolPtr) {
  if (GetVarFieldType(dst) == VarFieldType::ONLY_POINTER) {
    poolPtr->Free(GetVarFieldContent(dst));
    return true;
  }
  return false;
}

}  // end of Namespace NKV