//
//  buffer_page.h
//  PROJECT buffer_page
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <assert.h>
#include <cstdint>
#include <cstring>
#include <iostream>
#include "logging.h"
#include "schema.h"

#include "gtest/gtest_prod.h"

#define PAGE_HEADER_SIZE sizeof(PageHeader)
#define ROW_HEADER_SIZE sizeof(RowHeader)
#define OCCUPANCY_BITMAP_SIZE sizeof(OccupancyBitmap)
namespace NKV {
const int pageSize = 4 * 1024;  // 4KB

const long long mask = 0x0000000000000FFF;  // 0x0000000000000FFF;

using RowOffset = uint32_t;
using CRC32 = uint32_t;

class BufferPage;

struct OccupancyBitmap {
  unsigned char bitmap[16];
} __attribute__((packed));

struct PageHeader {
  char magic[2];
  SchemaId schemaId;
  SchemaVer schemaVer;
  BufferPage *prevPagePtr;
  BufferPage *nextpagePtr;
  uint16_t howRowNum;
  char reserved[38];
} __attribute__((packed));

struct RowHeader {
  char CRC[4];
  TimeStamp timestamp;
  PmemAddress pmemAddr;
  ValuePtr *kvNodeAddr;
} __attribute__((packed));

class BufferPage {
 private:
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

  // set (magic, 0, 2)
  void setMagicPage(uint16_t magic);
  // get (magic, 0, 2)
  uint16_t getMagicPage();

  // set (schemaID, 2, 4)
  void setSchemaIDPage(uint32_t schemaID);

  // get (schemaID, 2, 4)
  SchemaId getSchemaIDPage();

  // set (schemaVer, 6, 2)
  void setSchemaVerPage(uint16_t schemaVer);

  // get (schemaVer, 6, 2)
  uint16_t getSchemaVerPage();

  // get (prevPagePtr, 8, 8)
  void setPrevPage(BufferPage *prevPagePtr);

  // set (prevPagePtr, 8, 8)
  BufferPage *getPrevPage();

  // set (nextPagePtr, 16, 8)
  void setNextPage(BufferPage *nextPagePtr);

  // get (nextPagePtr, 16, 8)
  BufferPage *getNextPage();

  // set (hotRowsNum, 24, 2)
  void setHotRowsNumPage(uint16_t hotRowsNum);

  // set (hotRowsNum, 24, 2)
  uint16_t getHotRowsNumPage();

  void setReservedHeader();

  void clearPageBitMap(uint32_t occuBitmapSize);

  // 1.2 Row get & set functions.

  // Row Struct:
  // CRC (4) | Timestamp (8) | PlogAddr (8) | KVNodeAddr(8)
  // CRC:
  uint32_t getCRCRow();
  void setCRCRow();

  // Timestamp: (RowAddr + 4, 8)
  TimeStamp getTimestampRow(RowAddr rAddr) {
    return ((RowHeader *)rAddr)->timestamp;
  }
  void setTimestampRow(RowAddr rAddr, TimeStamp &ts) {
    ((RowHeader *)rAddr)->timestamp = ts;
  }

  // PlogAddr: (RowAddr + 12, 8)
  PmemAddress getPlogAddrRow(RowAddr rAddr) {
    return ((RowHeader *)rAddr)->pmemAddr;
  }
  void setPlogAddrRow(RowAddr rAddr, PmemAddress plogAddr) {
    ((RowHeader *)rAddr)->pmemAddr = plogAddr;
  }

  // KVNodeAddr: (RowAddr + 20, 8)
  ValuePtr *getKVNodeAddrRow(RowAddr rAddr) {
    return ((RowHeader *)rAddr)->kvNodeAddr;
  }
  void setKVNodeAddrRow(RowAddr rAddr, ValuePtr *kvNodeAddr) {
    ((RowHeader *)rAddr)->kvNodeAddr = kvNodeAddr;
  }

  // Value:
  Value getValueRow(RowAddr rAddr, uint32_t valueSize) {
    char *valueAddr = (char *)rAddr + ROW_HEADER_SIZE;
    std::string value = std::string(valueAddr, valueSize);
    return value;
  }

  // need to
  bool setValueRow(RowAddr rAddr, const Value &value, uint32_t valueSize) {
    if (valueSize != value.size()) {
      NKV_LOG_E(
          std::cerr,
          "Provided value: \"{}\".size() = {}, conflict with valueSize: {}",
          value, value.size(), valueSize);
      return false;
    }
    char *valueAddr = (char *)rAddr + ROW_HEADER_SIZE;
    memcpy(valueAddr, value.c_str(), value.size());
    return true;
  }

  // test helper function: get rAddr by rowOffset:
  inline RowAddr _getRowAddr(RowOffset rowOffset, uint32_t rowSize) {
    return (RowAddr)(content + PAGE_HEADER_SIZE + OCCUPANCY_BITMAP_SIZE +
                     rowSize * rowOffset);
  }
  // 2. Occupancy Bitmap functions.

  // a bit for a row, page size = 64KB, row size = 128B, there are at most 512
  // rows, so 512 bits=64 Bytes is sufficient
  bool setRowBitMapPage(RowOffset rowOffset);

  bool clearRowBitMapPage(RowOffset rowOffset);
  bool isBitmapSet(RowOffset rowOffset);

  // Return the idx of first slot (0) in Bitmap
  // Input: bitmapSize (byte)
  // Output: offset [Position of first 0 in bitmap, UINT32_MAX represent no
  // empty slot]
  RowOffset getFirstZeroBit(uint32_t maxRowNumOfPage, uint32_t beginOffset = 0,
                            uint32_t endOffset = UINT32_MAX) {
    if (maxRowNumOfPage == getHotRowsNumPage()) return UINT32_MAX;
    if (endOffset > maxRowNumOfPage) endOffset = maxRowNumOfPage;
    if (beginOffset > endOffset)
      NKV_LOG_E(std::cerr, "beginOffset: {} > endOffset: {}", beginOffset,
                endOffset);

    // No __builtin implementation:
    uint32_t beginByte = beginOffset / 8;
    uint32_t endByte = endOffset / 8;

    for (uint32_t byteIdx = beginByte; byteIdx <= endByte; byteIdx++) {
      uint8_t beginBit = 0, endBit = 8;
      // First Byte
      if (byteIdx == beginByte) beginBit = beginOffset % 8;
      // Last Byte
      if (byteIdx == endByte) endBit = endOffset % 8;

      uint8_t byteContent = content[PAGE_HEADER_SIZE + byteIdx];
      // Fix Byte
      uint8_t byteMask = ((1 << beginBit) - 1) | ~((1 << endBit) - 1);
      byteContent |= byteMask;
      NKV_LOG_I(std::cout, "ByteIdx: {}, byteMask: {}, byteContent: {}",
                byteIdx, byteMask, byteContent);
      if (byteContent != UINT8_MAX)
        for (uint8_t bitIdx = beginBit; bitIdx < endBit; bitIdx++)
          if (((byteContent >> bitIdx) & (uint8_t)1) == 0)
            return byteIdx * 8 + bitIdx;
    }

    // No empty slot
    return UINT32_MAX;
  }
  // 3. Operations.
  // 3.1 Initialize a schema.

  void initializePage(uint32_t occuBitmapSize);

  friend class PBRB;

  FRIEND_TEST(BufferPageTest, Initialization);
  FRIEND_TEST(BufferPageTest, BasicFunctions);
};

}  // namespace NKV
