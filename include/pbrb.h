//
//  pbrb.h
//  PROJECT pbrb
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <oneapi/tbb/concurrent_map.h>
#include <oneapi/tbb/concurrent_vector.h>
#include <algorithm>
#include <atomic>
#include <bitset>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <future>
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
const uint32_t pbrbAsyncQueueSize = 16;

inline bool isValid(uint32_t testVal) { return !(testVal & ERRMASK); }

using IndexerT =
    oneapi::tbb::concurrent_map<decltype(Key::primaryKey), ValuePtr>;
using IndexerList = std::unordered_map<SchemaId, std::shared_ptr<IndexerT>>;
using IndexerIterator = IndexerT::iterator;

struct AsyncBufferEntry {
  uint32_t _entry_size = 0;
  TimeStamp _oldTS;
  TimeStamp _newTS;
  IndexerIterator _iter;
  Value _entry_content;

  AsyncBufferEntry(uint32_t entry_size) : _entry_size(entry_size) {
    _entry_content.resize(_entry_size);
  }
  inline void copyContent(TimeStamp oldTS, TimeStamp newTS,
                          IndexerIterator iter, const Value &src) {
    assert(_entry_size == src.size());
    _oldTS = oldTS;
    _newTS = newTS;
    _iter = iter;
    memcpy((char *)_entry_content.data(), src.c_str(), _entry_size);
  }
};

class AsyncBufferQueue {
 private:
  SchemaId _schema_id = 0;
  uint32_t _schema_size = 0;
  uint32_t _queue_size = 0;

  std::vector<std::shared_ptr<AsyncBufferEntry>> _queue_contents;
  // control the queue push and pop function
  std::atomic_uint32_t _enqueue_head{0};
  std::atomic_uint32_t _dequeue_tail{0};

 public:
  AsyncBufferQueue(uint32_t schema_id, uint32_t schema_size,
                   uint32_t queue_size)
      : _schema_id(schema_id),
        _schema_size(schema_size),
        _queue_size(queue_size) {
    _queue_contents.resize(queue_size);
    for (auto i = 0; i < queue_size; i++) {
      _queue_contents[i] = std::make_shared<AsyncBufferEntry>(schema_size);
    }
  }
  SchemaId getSchemaId() { return _schema_id; }

  bool EnqueueOneEntry(TimeStamp oldTS, TimeStamp newTS, IndexerIterator iter,
                       const Value &value) {
    uint32_t allocated_offset =
        _enqueue_head.fetch_add(1, std::memory_order_relaxed);
    if (allocated_offset <
        _dequeue_tail.load(std::memory_order_relaxed) + _queue_size) {
      _queue_contents[allocated_offset % _queue_size]->copyContent(oldTS, newTS,
                                                                   iter, value);

      // NKV_LOG_I(std::cout, "Enqueue entry");
      return true;
    }
    _enqueue_head.fetch_sub(1, std::memory_order_relaxed);
    return false;
  }

  std::shared_ptr<AsyncBufferEntry> DequeueOneEntry() {
    uint32_t accessing_offset =
        _dequeue_tail.fetch_add(1, std::memory_order_relaxed);

    if (accessing_offset < _enqueue_head.load(std::memory_order_relaxed)) {
      // NKV_LOG_I(std::cout, "Dequeue entry");
      return _queue_contents[accessing_offset % _queue_size];
    }
    _dequeue_tail.fetch_sub(1, std::memory_order_relaxed);
    return nullptr;
  }
  bool Empty() {
    return _enqueue_head.load(std::memory_order_relaxed) <=
           _dequeue_tail.load(std::memory_order_relaxed);
  }
};

class PBRB {
 public:
  // constructor
  PBRB(int maxPageNumber, TimeStamp *wm, IndexerList *indexerListPtr,
       SchemaUMap *umap, uint64_t retentionWindowSecs = 60,
       uint32_t maxPageSearchNum = 5, bool async_pbrb = false,
       bool enable_async_gc = false, double targetOccupancyRatio = 0.7,
       uint64_t gcIntervalus = 100000);
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
  bool _isGCRunning = false;
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
  std::map<SchemaId, std::shared_ptr<BufferListBySchema>> _bufferMap;

  bool _async_pbrb = false;
  std::map<SchemaId, std::shared_ptr<AsyncBufferQueue>> _asyncQueueMap;

  oneapi::tbb::concurrent_vector<std::shared_ptr<AsyncBufferQueue>> _asyncThreadPollList;
  

  TimeStamp _watermark;

  // GC
  bool _asyncGC = false;
  std::chrono::microseconds _gcIntervalus = std::chrono::microseconds(50000);
  uint64_t GCFailedTimes = 0;
  uint64_t _retentionWindowSecs = 60;  // 1 minute
  double targetOccupancyRatio = 0.7;
  double startGCOccupancyRatio = 0.75;

 private:
  BufferPage *getPageAddr(void *rowAddr);

  std::list<BufferPage *> &getFreePageList() { return _freePageList; }

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
  inline RowOffset findEmptySlotInPage(
      std::shared_ptr<BufferListBySchema> &blbs, BufferPage *pagePtr,
      RowOffset beginOffset = 0, RowOffset endOffset = UINT32_MAX);

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
    if (accStat.hit())
      ;  // traverseIdxGC();
    return true;
  }
  bool schemaMiss(SchemaId sid) {
    auto &accStat = _AccStatBySchema[sid];
    if (accStat.miss())
      ;  // traverseIdxGC();
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
  std::future<bool> _GCResult;
  std::mutex writeLock_;
  std::mutex traverseIdxGCLock_;
  // GC
 private:
  bool _traverseIdxGCBySchema(SchemaId schemaid);
  bool _reclaimEmptyPages(SchemaId schemaid);
  inline bool _checkOccupancyRatio(double ratio);
  inline double _getOccupancyRatio() {
    return 1 - ((double)_freePageList.size() / _maxPageNumber);
  }

  void _stopGC();
  bool _asyncTraverseIdxGC();
  bool syncWrite(TimeStamp oldTS, TimeStamp newTS, SchemaId schemaId,
                 const Value &value, IndexerIterator iter);
  bool asyncWrite(TimeStamp oldTS, TimeStamp newTS, SchemaId schemaId,
                  const Value &value, IndexerIterator iter);
  void asyncWriteHandler(decltype(&_asyncThreadPollList));

 public:
  bool traverseIdxGC();

 public:
  bool read(TimeStamp oldTS, TimeStamp newTS, const RowAddr addr,
            SchemaId schemaId, Value &value, ValuePtr *vPtr);
  bool write(TimeStamp oldTS, TimeStamp newTS, SchemaId schemaId,
             const Value &value, IndexerIterator iter);

  bool dropRow(RowAddr rAddr);

  bool evictRow(IndexerIterator &iter);
  // GTEST

  friend class BufferListBySchema;
  FRIEND_TEST(PBRBTest, Test01);
};

}  // namespace NKV
