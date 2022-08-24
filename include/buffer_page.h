//
//  buffer_page.h
//  PROJECT buffer_page
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <cstdint>
#include <cstring>
#include "schema.h"

namespace NKV {
const int pageSize = 4 * 1024;  // 4KB

const long long mask = 0x0000000000000FFF;  // 0x0000000000000FFF;

struct BufferPage {
  unsigned char content[pageSize];
  // From srcPtr, write size byte(s) of data to pagePtr with offset.
  template <typename T>
  void writeToPage(size_t offset, const T *srcPtr, size_t size) {
    const void *sPtr = static_cast<const void *>(srcPtr);
    void *destPtr = static_cast<void *>(content + offset);
    memcpy(destPtr, sPtr, size);
  }

  // Read from pagePtr + offset with size to srcPtr;
  template <typename T>
  T readFromPage(size_t offset, size_t size) const {
    T result;
    void *dPtr = static_cast<void *>(&result);
    const void *sPtr = static_cast<const void *>(content + offset);
    memcpy(dPtr, sPtr, size);
    return result;
  }

  void readFromPage(size_t offset, size_t size, void *dPtr) const {
    const void *sPtr = static_cast<const void *>(content + offset);
    memcpy(dPtr, sPtr, size);
  }
};

}  // namespace NKV
