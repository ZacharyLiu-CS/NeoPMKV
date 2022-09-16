//
//  pbrb.h
//  PROJECT pbrb
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <algorithm>
#include <bitset>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <new>
#include <numeric>
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

using IndexerT = std::map<decltype(Key::primaryKey), ValuePtr>;
using IndexerList = std::unordered_map<SchemaId, std::shared_ptr<IndexerT>>;
using IndexerIterator = IndexerT::iterator;

class PBRB {
 public:
  // constructor
  PBRB(int maxPageNumber, TimeStamp *wm, IndexerList *indexerListPtr,
       SchemaUMap *umap, uint64_t retentionWindowSecs = 60,
       uint32_t maxPageSearchNum = 5, double targetOccupancyRatio = 0.7);
  // dtor
  ~PBRB();

#ifdef ENABLE_BREAKDOWN
  void analyzePerf() {
    auto outputVector = [](std::vector<double> &vec, std::string &&name) {
      double total = std::accumulate<std::vector<double>::iterator, double>(
          vec.begin(), vec.end(), 0);
      NKV_LOG_I(std::cout,
                "Number of {}: {}, total time cost: {:.2f} s, average time "
                "cost: {:.2f} ns",
                name, vec.size(), total / 1000000000, total / vec.size());
    };
    outputVector(findSlotNss, std::string("findSlotNss"));
    outputVector(fcrpNss, std::string("fcrpNss"));
    outputVector(searchingIdxNss, std::string("searchingIdxNss"));
  }
  std::vector<double> findSlotNss;
  std::vector<double> fcrpNss;
  std::vector<double> searchingIdxNss;
#endif

 private:
  uint32_t _maxPageNumber;
  uint32_t _pageSize = pageSize;
  uint32_t _pageHeaderSize = PAGE_HEADER_SIZE;
  uint32_t _rowHeaderSize = ROW_HEADER_SIZE;

  uint32_t _maxPageSearchingNum;

  // A list to store allocated free pages
  BufferPage *_bufferPoolPtr = nullptr;
  std::list<BufferPage *> _freePageList;

  // A Map to store pages used by different SKV table, and one SKV table
  // corresponds to a list
  std::map<SchemaId, std::list<BufferPage *>> _usedPageMap;

  IndexerList *_indexListPtr;

  uint32_t _splitCnt = 0;
  uint32_t _evictCnt = 0;
  SchemaUMap *_schemaUMap;

  friend class BufferListBySchema;
  std::map<SchemaId, BufferListBySchema> _bufferMap;
  TimeStamp _watermark;

  int32_t GCFailedTimes = 0;
  uint64_t _retentionWindowSecs = 60;  // 1 minute
  double targetOccupancyRatio = 0.7;

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
  inline RowOffset findEmptySlotInPage(BufferListBySchema &blbs,
                                       BufferPage *pagePtr,
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
  RowAddr getAddrByPageAndRow(BufferPage *pagePtr, RowOffset rowOff);
  // evict row and return cold addr.
  bool evictRow(IndexerIterator &iter);

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

 private:
  // Staticstics:
  // Hit Ratio
  struct AccessStatistics {
    uint64_t accessCount = 0;
    uint64_t hitCount = 0;
    uint64_t lastHitCount = 0;
    std::vector<uint64_t> hitVec{0};
    uint64_t interval = 200000;

    inline bool updateVec() {
      if (accessCount % interval == 0) {
        hitVec.emplace_back(hitCount - lastHitCount);
        lastHitCount = hitCount;
        return true;
      }
      return false;
    }
    inline bool hit() {
      accessCount++;
      hitCount++;
      return updateVec();
    }
    inline bool miss() {
      accessCount++;
      return updateVec();
    }
    inline double getHitRatio() {
      if (accessCount > 0)
        return (double)hitCount / accessCount;
      else
        return 2;
    }
  };

  std::unordered_map<SchemaId, AccessStatistics> _AccStatBySchema;

 public:
  bool schemaHit(SchemaId sid) {
    auto &accStat = _AccStatBySchema[sid];
    if (accStat.hit()) traverseIdxGC();
    return true;
  }
  bool schemaMiss(SchemaId sid) {
    auto &accStat = _AccStatBySchema[sid];
    if (accStat.miss()) traverseIdxGC();
    return true;
  }
  double getHitRatio(SchemaId sid) {
    auto it = _AccStatBySchema.find(sid);
    if (it == _AccStatBySchema.end()) return -1;
    return (double)(it->second.hitCount) / it->second.accessCount;
  }

  void outputHitRatios() {
    for (auto &it : _AccStatBySchema) {
      auto &hitVec = it.second.hitVec;
      NKV_LOG_I(std::cout, "(SchemaId: {}): Hit Ratio: {} / {} = {:.2f}",
                it.first, it.second.hitCount, it.second.accessCount,
                (double)it.second.hitCount / it.second.accessCount);
      std::string hitStr, ratioStr;
      std::for_each(hitVec.begin(), hitVec.end(), [&](uint64_t x) {
        fmt::format_to(std::back_inserter(hitStr), "{:8d} ", x);
        fmt::format_to(std::back_inserter(ratioStr), "{:8.3f} ",
                       (double)x / it.second.interval);
      });

      NKV_LOG_I(std::cout, "(SchemaId: {}): Hit Ratios per {} accesses: {}",
                it.first, it.second.interval, hitStr);
      NKV_LOG_I(std::cout, "(SchemaId: {}): Hit Ratios per {} accesses: {}",
                it.first, it.second.interval, ratioStr);
    }
  }

 private:
  std::mutex writeLock_;

  // GC
 private:
  bool _traverseIdxGCBySchema(SchemaId schemaid);
  bool _reclaimEmptyPages(SchemaId schemaid);
  inline bool _checkOccupancyRatio();
  inline double _getOccupancyRatio() {
    return 1 - ((double)_freePageList.size() / _maxPageNumber);
  }

 public:
  bool traverseIdxGC();

 public:
  bool read(TimeStamp oldTS, TimeStamp newTS, const RowAddr addr,
            SchemaId schemaid, Value &value, ValuePtr *vPtr);
  bool syncwrite(TimeStamp oldTS, TimeStamp newTS, SchemaId schemaid,
                 const Value &value, IndexerIterator iter);
  bool asyncwrite(TimeStamp oldTS, TimeStamp newTS, SchemaId schemaid,
                  const Value &value, IndexerIterator iter);
  bool dropRow(RowAddr rAddr);
  // GTEST

  friend class BufferListBySchema;
  FRIEND_TEST(PBRBTest, Test01);
};

}  // namespace NKV
