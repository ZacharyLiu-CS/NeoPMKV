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
#include "buffer_list.h"

namespace NKV {
const int pageSize = 4 * 1024;  // 4KB

const long long mask = 0x0000000000000FFF;  // 0x0000000000000FFF;

using RowOffset = uint32_t;
using RowAddr = void *;
using CRC32 = uint32_t;

class BufferPage;
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
  char *kvNodeAddr;
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
  void setMagicPage(BufferPage *pagePtr, uint16_t magic);
  // get (magic, 0, 2)
  uint16_t getMagicPage(const BufferPage *pagePtr);

  // set (schemaID, 2, 4)
  void setSchemaIDPage(BufferPage *pagePtr, uint32_t schemaID);

  // get (schemaID, 2, 4)
  SchemaId getSchemaIDPage(const BufferPage *pagePtr);

  // set (schemaVer, 6, 2)
  void setSchemaVerPage(BufferPage *pagePtr, uint16_t schemaVer);

  // get (schemaVer, 6, 2)
  uint16_t getSchemaVerPage(const BufferPage *pagePtr);

  // get (prevPagePtr, 8, 8)
  void setPrevPage(BufferPage *pagePtr, BufferPage *prevPagePtr);

  // set (prevPagePtr, 8, 8)
  BufferPage *getPrevPage(const BufferPage *pagePtr);

  // set (nextPagePtr, 16, 8)
  void setNextPage(BufferPage *pagePtr, BufferPage *nextPagePtr);

  // get (nextPagePtr, 16, 8)
  BufferPage *getNextPage(const BufferPage *pagePtr);

  // set (hotRowsNum, 24, 2)
  void setHotRowsNumPage(BufferPage *pagePtr, uint16_t hotRowsNum);

  // set (hotRowsNum, 24, 2)
  uint16_t getHotRowsNumPage(const BufferPage *pagePtr);

  void setReservedHeader(BufferPage *pagePtr);

  void clearPageBitMap(BufferPage *pagePtr, uint32_t occuBitmapSize,
                       uint32_t maxRowCnt);

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
  char *getKVNodeAddrRow(RowAddr rAddr) {
    return ((RowHeader *)rAddr)->kvNodeAddr;
  }
  void setKVNodeAddrRow(RowAddr rAddr, char *kvNodeAddr) {
    ((RowHeader *)rAddr)->kvNodeAddr = kvNodeAddr;
  }

  // 2. Occupancy Bitmap functions.

  // a bit for a row, page size = 64KB, row size = 128B, there are at most 512
  // rows, so 512 bits=64 Bytes is sufficient
  void setRowBitMapPage(BufferPage *pagePtr, RowOffset rowOffset);

  void clearRowBitMapPage(BufferPage *pagePtr, RowOffset rowOffset);
  void clearRowBitMap(BufferPage *pagePtr, RowOffset rowOffset);
  inline bool isBitmapSet(BufferPage *pagePtr, RowOffset rowOffset);

  // 3. Operations.
  // 3.1 Initialize a schema.

  void initializePage(BufferPage *pagePtr, BufferListBySchema &bmd);

  // create a pageList for a SKV table according to schemaID
  BufferPage *createCacheForSchema(SchemaId schemaId, SchemaVer schemaVer = 0);

  BufferPage *AllocNewPageForSchema(SchemaId schemaId);

  BufferPage *AllocNewPageForSchema(SchemaId schemaId, BufferPage *pagePtr);
};

}  // namespace NKV
