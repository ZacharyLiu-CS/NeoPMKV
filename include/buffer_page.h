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

namespace NKV {
constexpr int pageSize = 4 * 1024;  // 4KB

const long long mask = 0x0000000000000FFF;  // 0x0000000000000FFF;

using RowOffset = uint32_t;
using CRC32 = uint32_t;

class BufferPage;

struct OccupancyBitmap {
  unsigned char bitmap[16];
} __attribute__((packed));

struct PageHeader {
  uint16_t magic = 0;                 // 0 (2)
  SchemaId schemaId = 0;              // 2 (4)
  uint32_t howRowNum = 0;             // 6 (4)
  BufferPage *prevPagePtr = nullptr;  // 10 (8)
  BufferPage *nextpagePtr = nullptr;  // 18 (8)
  OccupancyBitmap occupancyBitmap;    // 26 (16)
  char reserved[22] = {'\0'};         // 42 (22)
} __attribute__((packed));

struct RowHeader {                    // size 24
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
  inline void writeToPage(size_t offset, const T *srcPtr, size_t size) {
    const void *sPtr = static_cast<const void *>(srcPtr);
    void *destPtr = static_cast<void *>(content + offset);
    memcpy(destPtr, sPtr, size);
  }

  // set (magic, 0, 2)
  inline void setMagicPage(uint16_t magic) {
    ((PageHeader *)content)->magic = magic;
  }
  // get (magic, 0, 2)
  inline uint16_t getMagicPage() { return ((PageHeader *)content)->magic; }

  // set (schemaID, 2, 4)
  inline void setSchemaIDPage(uint32_t schemaID) {
    ((PageHeader *)content)->schemaId = schemaID;
  }

  // get (schemaID, 2, 4)
  inline SchemaId getSchemaIDPage() {
    return ((PageHeader *)content)->schemaId;
  }

  // get (prevPagePtr, 8, 8)
  inline void setPrevPage(BufferPage *prevPagePtr) {
    ((PageHeader *)content)->prevPagePtr = prevPagePtr;
  }

  // set (prevPagePtr, 8, 8)
  inline BufferPage *getPrevPage() {
    return ((PageHeader *)content)->prevPagePtr;
  }

  // set (nextPagePtr, 16, 8)
  inline void setNextPage(BufferPage *nextPagePtr) {
    ((PageHeader *)content)->nextpagePtr = nextPagePtr;
  }

  // get (nextPagePtr, 16, 8)
  inline BufferPage *getNextPage() {
    return ((PageHeader *)content)->nextpagePtr;
  }

  // set (hotRowsNum, 24, 2)
  inline void setHotRowsNumPage(uint32_t hotRowsNum) {
    ((PageHeader *)content)->howRowNum = hotRowsNum;
  }

  // set (hotRowsNum, 24, 2)
  inline uint32_t getHotRowsNumPage() {
    return ((PageHeader *)content)->howRowNum;
  }

  inline void setReservedHeader() {  // reserved is 22 bytes
    memset(((PageHeader *)content)->reserved, 0, 22);
  }

  inline void clearPageBitMap(uint32_t occuBitmapSize) {
    memset(&((PageHeader *)content)->occupancyBitmap, 0, occuBitmapSize);
  }

  // 1.2 Row get & set functions.

  // Row Struct:
  // CRC (4) | Timestamp (8) | PlogAddr (8) | KVNodeAddr(8)

  inline uint32_t getCRCRow();
  inline void setCRCRow();

  // Timestamp: (RowAddr + 4, 8)
  inline TimeStamp getTimestampRow(RowAddr rAddr) {
    return ((RowHeader *)rAddr)->timestamp;
  }
  inline void setTimestampRow(RowAddr rAddr, TimeStamp &ts) {
    ((RowHeader *)rAddr)->timestamp = ts;
  }

  // PlogAddr: (RowAddr + 12, 8)
  inline PmemAddress getPlogAddrRow(RowAddr rAddr) {
    return ((RowHeader *)rAddr)->pmemAddr;
  }
  inline void setPlogAddrRow(RowAddr rAddr, PmemAddress plogAddr) {
    ((RowHeader *)rAddr)->pmemAddr = plogAddr;
  }

  // KVNodeAddr: (RowAddr + 20, 8)
  inline ValuePtr *getKVNodeAddrRow(RowAddr rAddr) {
    return ((RowHeader *)rAddr)->kvNodeAddr;
  }
  inline void setKVNodeAddrRow(RowAddr rAddr, ValuePtr *kvNodeAddr) {
    ((RowHeader *)rAddr)->kvNodeAddr = kvNodeAddr;
  }

  // Value:
  inline void getValueRow(RowAddr rAddr, uint32_t valueSize, Value &value) {
    char *valueAddr = (char *)rAddr + ROW_HEADER_SIZE;
    value.resize(valueSize);
    memcpy(value.data(), valueAddr, valueSize);
  }
  inline char* getValuePtr(RowAddr rAddr){
    return (char *)rAddr + ROW_HEADER_SIZE;
  }
  // fields -> vector<offset, size>
  inline void getValueRow(RowAddr rAddr, uint32_t valueSize, std::vector<Value> &values,
                          std::vector<std::pair<uint32_t, uint32_t>> &fields) {
    char *valueAddr = (char *)rAddr + ROW_HEADER_SIZE;
    uint32_t i = 0;
    for (auto& [fieldOffset, fieldSize] : fields) {
      values[i].resize(fieldOffset);
      memcpy(values[i].data() , valueAddr + fieldOffset,
             fieldSize + FieldHeadSize);
      i+=1;
    }
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
    memcpy(valueAddr, value.data(), value.size());
    return true;
  }

  // test helper function: get rAddr by rowOffset:
  inline RowAddr _getRowAddr(RowOffset rowOffset, uint32_t rowSize) {
    return (RowAddr)(content + PAGE_HEADER_SIZE + rowSize * rowOffset);
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
                            uint32_t endOffset = UINT32_MAX);
  // 3. Operations.
  // 3.1 Initialize a schema.

  void initializePage();

  friend class PBRB;
  friend class BufferListBySchema;

  FRIEND_TEST(BufferPageTest, Initialization);
  FRIEND_TEST(BufferPageTest, BasicFunctions);
};

}  // namespace NKV
