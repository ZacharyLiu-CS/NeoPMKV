//
//  pbrb.h
//  PROJECT pbrb
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <bitset>
#include <cassert>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <new>
#include <tuple>
#include <vector>

// header of this project
#include "buffer_page.h"
#include "logging.h"
#include "pmem_engine.h"
#include "profiler.h"
#include "schema.h"
#include "timestamp.h"

namespace NKV {

using RowOffset = uint32_t;
using RowAddr = void *;
using CRC32 = uint32_t;

const uint32_t rowCRC32Offset = 0;
const uint32_t rowTSOffset = sizeof(CRC32);
const uint32_t rowPlogAddrOffset = sizeof(CRC32) + sizeof(TimeStamp);

inline bool isValid(uint32_t testVal) { return !(testVal & ERRMASK); }

using IndexerT = std::map<Key, Value *>;

class PBRB {
  struct RowHeader {
    char CRC[4];
    TimeStamp timestamp;
    PmemAddress pmemAddr;
    char *kvNodeAddr;
  } __attribute__((packed));

 private:
  uint32_t _maxPageNumber;
  uint32_t _pageSize = pageSize;
  uint32_t _pageHeaderSize = 64;
  uint32_t _rowHeaderSize = sizeof(RowHeader);

  uint32_t _maxPageSearchingNum;
  std::string _fcrpOutputFileName;
  // A list to store allocated free pages
  BufferPage *_bufferPoolPtr = nullptr;
  std::list<BufferPage *> _freePageList;

  // A Map to store pages used by different SKV table, and one SKV table
  // corresponds to a list
  std::map<int, std::list<BufferPage *>> _usedPageMap;

  IndexerT *_indexer;
  uint32_t _splitCnt = 0;
  uint32_t _evictCnt = 0;

  SchemaAllocator _schemaAllocator;
  SchemaUMap _schemaUMap;

  struct BufferListBySchema {
    Schema *ownSchema = nullptr;
    uint32_t occuBitmapSize;
    uint32_t nullableBitmapSize;
    uint32_t maxRowCnt;
    std::vector<FieldMetaData> fieldsInfo;
    uint32_t rowSize;

    uint32_t curPageNum = 0;
    uint32_t curRowNum = 0;

    PBRB *bePBRBPtr = nullptr;
    // manage the buffer list
    BufferPage *headPage = nullptr;
    BufferPage *tailPage = nullptr;

    BufferListBySchema() {}

    BufferListBySchema(SchemaId schemaId, uint32_t pageSize,
                       uint32_t pageHeaderSize, uint32_t rowHeaderSize,
                       PBRB *pbrbPtr, BufferPage *headPagePtr) {
      bePBRBPtr = pbrbPtr;
      headPage = headPagePtr;
      tailPage = headPagePtr;
    }
    void setNullBitmapSize(uint32_t fieldNumber) {
      nullableBitmapSize = (fieldNumber - 1) / 8 + 1;
    }

    void setOccuBitmapSize(uint32_t pageSize) {
      occuBitmapSize = (pageSize / rowSize - 1) / 8 + 1;
    }
    void setInfo(SchemaId schemaId, uint32_t pageSize, uint32_t pageHeaderSize,
                 uint32_t rowHeaderSize) {
      // read from Schema
      Schema* res = bePBRBPtr->_schemaUMap.find(schemaId);
      if (res == nullptr) return;

      ownSchema = res;
      setNullBitmapSize(ownSchema->fields.size());

      uint32_t currRowOffset = rowHeaderSize + nullableBitmapSize;

      // Set Metadata
      for (size_t i = 0; i < ownSchema->fields.size(); i++) {
        FieldType currFT = ownSchema->fields[i].type;
        FieldMetaData fieldObj;

        fieldObj.fieldSize = FTSize[(uint8_t)(currFT)];
        fieldObj.fieldOffset = currRowOffset;
        fieldObj.isNullable = false;
        fieldObj.isVariable = false;
        fieldsInfo.push_back(fieldObj);
        // Go to next field.
        currRowOffset += fieldObj.fieldSize;
      }

      // set rowSize
      // rowSize = currRowOffset;
      rowSize = currRowOffset + sizeof(size_t);
      setOccuBitmapSize(pageSize);
      maxRowCnt = (pageSize - pageHeaderSize - occuBitmapSize) / rowSize;
    }
  };  // end of struct BufferListBySchema

  friend struct BufferListBySchema;
  std::map<SchemaId, BufferListBySchema> _bufferMap;
  TimeStamp _watermark;

  // constructor
  PBRB(int maxPageNumer, TimeStamp *wm, IndexerT *indexer,
       uint32_t maxPageSearchNum);
  ~PBRB();

  BufferPage *getPageAddr(void *rowAddr);

  // 1. Header 'set' and 'get' functions.
  struct PageHeader {
    char magic[2];
    SchemaId schemaId;
    SchemaVer schemaVer;
    BufferPage *prevPagePtr;
    BufferPage *nextpagePtr;
    uint16_t howRowNum;
    char reserved[38];
  } __attribute__((packed));

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
  TimeStamp getTimestampRow(RowAddr rAddr);
  void setTimestampRow(RowAddr rAddr, TimeStamp &ts);

  // PlogAddr: (RowAddr + 12, 8)
  void *getPlogAddrRow(RowAddr rAddr);
  void setPlogAddrRow(RowAddr rAddr, void *PlogAddr);

  // KVNodeAddr: (RowAddr + 20, 8)
  void *getKVNodeAddrRow(RowAddr rAddr);
  void setKVNodeAddrRow(RowAddr rAddr, void *KVNodeAddr);

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

  std::list<BufferPage *> getFreePageList() { return _freePageList; }

  uint32_t getMaxPageNumber() { return _maxPageNumber; }

  float totalPageUsage() {
    return 1 - ((float)_freePageList.size() / (float)_maxPageNumber);
  }

  // move cold row in pAddress to PBRB and insert hot address into KVNode
  void *cacheColdRow(PmemAddress pAddress, Key key);

  // Copy memory from plog to (pagePtr, rowOffset)
  void *cacheRowFromPlog(BufferPage *pagePtr, RowOffset rowOffset,
                         PmemAddress pAddress);

  // Copy the header of row from DataRecord of query to (pagePtr, rowOffset)
  void *cacheRowHeaderFrom(uint32_t schemaId, BufferPage *pagePtr,
                           RowOffset rowOffset, ValuePtr *vPtr, void *nodePtr);

  // Copy the field of row from DataRecord of query to (pagePtr, rowOffset)
  void *cacheRowFieldFromDataRecord(uint32_t schemaId, BufferPage *pagePtr,
                                    RowOffset rowOffset, void *field,
                                    size_t strSize, uint32_t fieldID,
                                    bool isStr);

  // Count the space utiliuzation of string fields for each schema
  void countStringFeildUtilization(BufferPage *pagePtr, RowOffset rowOffset,
                                   long &totalFieldsSize, long &totalStoreSize,
                                   long &heapFieldNum, bool isPayloadRow);

  // Copy the payload of row from DataRecord of query to (pagePtr, rowOffset)
  void *cacheRowPayloadFromDataRecord(uint32_t schemaId, BufferPage *pagePtr,
                                      RowOffset rowOffset, Value *value);

  // find an empty slot between the beginOffset and endOffset in the page
  RowOffset findEmptySlotInPage(uint32_t schemaID, BufferPage *pagePtr,
                                RowOffset beginOffset, RowOffset endOffset);

  // find an empty slot in the page
  RowOffset findEmptySlotInPage(uint32_t schemaID, BufferPage *pagePtr);

  std::pair<BufferPage *, RowOffset> findCacheRowPosition(uint32_t schemaID);
  // record the search status
  struct FCRPSlowCaseStatus {
    bool isFound = true;
    int searchPageNum = 0;
  };
  std::pair<BufferPage *, RowOffset> findCacheRowPosition(
      uint32_t schemaID, FCRPSlowCaseStatus &stat);

  // Find the page pointer and row offset to cache cold row
  std::pair<BufferPage *, RowOffset> findCacheRowPosition(uint32_t schemaID,
                                                          Key key);

  // Find Cache Row Position From pagePtr to end
  std::pair<BufferPage *, RowOffset> findCacheRowPosition(
      uint32_t schemaID, BufferPage *pagePtr, FCRPSlowCaseStatus &stat);

  // store hot row in the empty row
  // void cacheHotRow(uint32_t schemaID, SKVRecord hotRecord);

  // return pagePtr and rowOffset.
  std::pair<BufferPage *, RowOffset> findRowByAddr(void *rowAddr);

  // evict row and return cold addr.
  PmemAddress evictRow(void *rowAddr);

  // mark the row as unoccupied when evicting a hot row
  void removeHotRow(BufferPage *pagePtr, RowOffset offset);

  // release the heap space owned by this hot row when using fixed field row
  void releaseHeapSpace(BufferPage *pagePtr, void *rowAddr);

  // release the heap space owned by this hot row when using payload row
  void releasePayloadHeapSpace(BufferPage *pagePtr, void *rowAddr);

  // split a full page into two pages
  bool splitPage(BufferPage *pagePtr);

  // merge pagePtr2 into pagePtr1, reclaim pagePtr2
  bool mergePage(BufferPage *pagePtr1, BufferPage *pagePtr2);

  // allocate a free page from the freePageList to store hot rows
  BufferPage *allocateFreePage();

  // after merging two pages, reclaim a page and insert it to freePageList
  void reclaimPage(BufferPage *pagePtr);

  // copy a row from (srcPagePtr, srcOffset) to (dstPagePtr, dstOffset)
  void *copyRowInPages(BufferPage *srcPagePtr, RowOffset srcOffset,
                       BufferPage *dstPagePtr, RowOffset dstOffset);

  float getAveragePageListUsage(float &maxPageListUsage, uint32_t *schemaID,
                                std::string *schemaNameArray,
                                float *pageListUsageArray, int &schemaCount);

  float getCurPageListUsage(uint32_t schemaID);

  void *getAddrByPageAndOffset(uint32_t schemaId, BufferPage *pagePtr,
                               RowOffset offset);

  inline void prefetcht2Row(RowAddr rowAddr, size_t size) {
    size_t clsize = sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
    for (size_t off = 0; off < size; off += clsize) {
      __builtin_prefetch((uint8_t *)rowAddr + off, 0, 1);
    }
    return;
  }
};

}  // namespace NKV
