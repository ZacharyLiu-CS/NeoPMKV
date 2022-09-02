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
#include <utility>
#include <vector>

// header of this project
#include "buffer_list.h"
#include "buffer_page.h"
#include "logging.h"
#include "pmem_engine.h"
#include "profiler.h"
#include "schema.h"
#include "timestamp.h"

namespace NKV {

const uint32_t rowCRC32Offset = 0;
const uint32_t rowTSOffset = sizeof(CRC32);
const uint32_t rowPlogAddrOffset = sizeof(CRC32) + sizeof(TimeStamp);

inline bool isValid(uint32_t testVal) { return !(testVal & ERRMASK); }

using IndexerT = std::map<Key, ValuePtr *>;
using IndexerIterator = IndexerT::iterator;

class PBRB {
 private:
  uint32_t _maxPageNumber;
  uint32_t _pageSize = pageSize;
  uint32_t _pageHeaderSize = PAGE_HEADER_SIZE;
  uint32_t _rowHeaderSize = ROW_HEADER_SIZE;

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

  friend class BufferListBySchema;
  std::map<SchemaId, BufferListBySchema> _bufferMap;
  TimeStamp _watermark;

  // constructor
  PBRB(int maxPageNumer, TimeStamp *wm, IndexerT *indexer,
       uint32_t maxPageSearchNum);
  ~PBRB();

  bool read(const RowAddr addr,SchemaId schemaid, std::string &value);
  bool write(SchemaId schemaid, const std::string &value,IndexerIterator iter);

  BufferPage *getPageAddr(void *rowAddr);

  std::list<BufferPage *> getFreePageList() { return _freePageList; }

  uint32_t getMaxPageNumber() { return _maxPageNumber; }

  // create a pageList for a SKV table according to schemaID
  BufferPage *createCacheForSchema(SchemaId schemaId, SchemaVer schemaVer = 0);

  BufferPage *AllocNewPageForSchema(SchemaId schemaId);

  BufferPage *AllocNewPageForSchema(SchemaId schemaId, BufferPage *pagePtr);

  float totalPageUsage() {
    return 1 - ((float)_freePageList.size() / (float)_maxPageNumber);
  }

  // move cold row in pAddress to PBRB and insert hot address into KVNode
  void *cacheColdRow(PmemAddress pAddress, Key key);


  // Copy the header of row from DataRecord of query to (pagePtr, rowOffset)
  void *cacheRowHeaderFrom(uint32_t schemaId, BufferPage *pagePtr,
                           RowOffset rowOffset, ValuePtr *vPtr, void *nodePtr);

  // find an empty slot between the beginOffset and endOffset in the page
  inline RowOffset findEmptySlotInPage(BufferListBySchema& blbs, BufferPage *pagePtr,
                                RowOffset beginOffset = 0,
                                RowOffset endOffset = UINT32_MAX);

  // find an empty slot in the page

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
                                                          IndexerIterator iter);

  // Traverse cache list to find empty row from pagePtr 
  std::pair<BufferPage *, RowOffset> traverseFindEmptyRow(
      uint32_t schemaID, BufferPage *pagePtr = nullptr,
      uint32_t maxPageSearchingNum = UINT32_MAX);

  // return pagePtr and rowOffset.
  std::pair<BufferPage *, RowOffset> findPageAndRowByAddr(void *rowAddr);

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
