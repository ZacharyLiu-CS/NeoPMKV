//
//  pbrb.cc
//  PROJECT pbrb
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "pbrb.h"
#include <atomic>
#include <thread>
#include "logging.h"

namespace NKV {
PBRB::PBRB(int maxPageNumber, TimeStamp *wm, IndexerList *indexerListPtr,
           SchemaUMap *umap, uint64_t retentionWindowSecs,
           uint32_t maxPageSearchNum, bool async_pbrb, bool enable_async_gc,
           double targetOccupancyRatio, uint64_t gcIntervalus) {
  static_assert(PAGE_HEADER_SIZE == 64, "PAGE_HEADER_SIZE != 64");
  // initialization

  _watermark = *wm;
  _schemaUMap = umap;
  _maxPageNumber = maxPageNumber;
  _indexListPtr = indexerListPtr;
  _maxPageSearchingNum = maxPageSearchNum;
  _retentionWindowSecs = retentionWindowSecs;
  _async_pbrb = async_pbrb;
  _gcIntervalus = std::chrono::microseconds(gcIntervalus);
  _asyncGC = enable_async_gc;
  // allocate bufferpage
  auto aligned_val = std::align_val_t{_pageSize};
  _bufferPoolPtr = static_cast<BufferPage *>(operator new(
      maxPageNumber * sizeof(BufferPage), aligned_val));

  for (int idx = 0; idx < maxPageNumber; idx++) {
    _freePageList.push_back(_bufferPoolPtr + idx);
  }
  if (_async_pbrb == true) {
    _asyncThread =
        std::thread(&PBRB::asyncWriteHandler, this, &_asyncThreadPollList);
    _asyncThread.detach();
  }

  if (_asyncGC == true) {
    _isGCRunning.store(true, std::memory_order_relaxed);
    _GCResult =
        std::async(std::launch::async, &PBRB::_asyncTraverseIdxGC, this);
  }
}

PBRB::~PBRB() {
  _isAsyncRunning.store(false, std::memory_order_relaxed);
  if (_asyncGC) _stopGC();
  if (_bufferPoolPtr != nullptr) {
    delete _bufferPoolPtr;
  }
  outputHitRatios();
}

BufferPage *PBRB::getPageAddr(void *rowAddr) {
  return (BufferPage *)((uint64_t)rowAddr & ~mask);
}

BufferPage *PBRB::createCacheForSchema(SchemaId schemaId, SchemaVer schemaVer) {
  if (_freePageList.empty()) {
    NKV_LOG_E(std::cerr, "Cannot create cache for schema: {}! (no free page)",
              schemaId);
    return nullptr;
  }

  if (_async_pbrb == true) {
    uint32_t schemaSize = _schemaUMap->find(schemaId)->getSize();
    auto bufferQueue = std::make_shared<AsyncBufferQueue>(schemaId, schemaSize,
                                                          pbrbAsyncQueueSize);
    _asyncQueueMap.insert({schemaId, bufferQueue});
    _asyncThreadPollList.push_back(bufferQueue);
    _isAsyncRunning.store(true, std::memory_order_relaxed);
    // NKV_LOG_I(std::cout, "async Queue size {}", _asyncQueue.size());
  }
  // Get a page and set schemaMetadata.
  BufferPage *pagePtr = _freePageList.front();

  std::shared_ptr<BufferListBySchema> blbsPtr =
      std::make_shared<BufferListBySchema>(schemaId, _pageSize, _pageHeaderSize,
                                           _rowHeaderSize, _schemaUMap,
                                           pagePtr);
  _bufferMap.insert_or_assign(schemaId, blbsPtr);
  auto &blbs = _bufferMap[schemaId];
  NKV_LOG_I(
      std::cout,
      "createCacheForSchema, schemaId: {}, pagePtr empty:{}, _freePageList "
      "size:{}, pageSize: {}, blbs->rowSize: {}, _bufferMap[{}].rowSize: {}, "
      "maxRowCnt: {}",
      schemaId, pagePtr == nullptr, _freePageList.size(), sizeof(BufferPage),
      blbs->rowSize, schemaId, blbs->rowSize, blbs->maxRowCnt);

  // Initialize Page.
  // memset(pagePtr, 0, sizeof(BufferPage));

  pagePtr->initializePage();
  pagePtr->setSchemaIDPage(schemaId);

  blbs->curPageNum++;
  _freePageList.pop_front();
  _AccStatBySchema.insert({schemaId, AccessStatistics()});

  return pagePtr;
}

BufferPage *PBRB::AllocNewPageForSchema(SchemaId schemaId) {
  // al1
  PointProfiler pointTimer;
  pointTimer.start();

  if (_freePageList.empty()) {
    NKV_LOG_E(std::cerr, "No free page!");
    return nullptr;
  }

  if (_bufferMap.find(schemaId) == _bufferMap.end()) {
    NKV_LOG_E(std::cerr,
              "Didn't find sid: {} in _bufferMap when alloc new page.",
              schemaId);
    return nullptr;
  }

  auto &blbs = _bufferMap[schemaId];
  BufferPage *pagePtr = blbs->headPage;
  blbs->curPageNum++;

  auto al1ns = pointTimer.end();

  if (pagePtr == nullptr)
    return createCacheForSchema(schemaId);
  else {
    // al2
    pointTimer.start();
    assert(_freePageList.size() > 0);
    BufferPage *newPage = _freePageList.front();

    // Initialize Page.
    newPage->initializePage();
    newPage->setSchemaIDPage(schemaId);

    // optimize: using tailPage.
    BufferPage *tail = blbs->tailPage;
    // set nextpage
    newPage->setPrevPage(tail);
    newPage->setNextPage(nullptr);
    tail->setNextPage(newPage);
    blbs->tailPage = newPage;

    auto al2ns = pointTimer.end();

    // al3

    pointTimer.start();
    _freePageList.pop_front();

    auto al3ns = pointTimer.end();
    // K2LOG_I(log::pbrb, "Alloc new page type 1: sid: {}, sname: {},
    // currPageNum: {}", schemaId, _bufferMap[schemaId].schema->name,
    // _bufferMap[schemaId].curPageNum);
    NKV_LOG_D(std::cout, "Remaining _freePageList size:{}",
              _freePageList.size());
    NKV_LOG_D(std::cout,
              "Allocated page for schema: {}, page count: {}, time al1: {}, "
              "al2: {}, al3: {}, total: {}",
              blbs->ownSchema->name, (blbs->curPageNum)->load, al1ns, al2ns,
              al3ns, al1ns + al2ns + al3ns);

    return newPage;
  }
}

BufferPage *PBRB::AllocNewPageForSchema(SchemaId schemaId,
                                        BufferPage *pagePtr) {
  // TODO: validation
  if (_freePageList.empty()) {
    NKV_LOG_E(std::cout, "No Free Page Now!");
    return nullptr;
  }

  assert(_freePageList.size() > 0);
  if (_bufferMap.find(schemaId) == _bufferMap.end()) {
    NKV_LOG_E(std::cerr,
              "Didn't find sid: {} in _bufferMap when alloc new page.",
              schemaId);
    return nullptr;
  }

  if (pagePtr == nullptr) {
    NKV_LOG_E(std::cerr, "specified nullptr as start pointer!");
    return nullptr;
  }

  auto &blbs = _bufferMap[schemaId];
  BufferPage *newPage = _freePageList.front();
  blbs->curPageNum++;

  // Initialize Page.
  // memset(newPage, 0, sizeof(BufferPage));
  newPage->initializePage();
  newPage->setSchemaIDPage(schemaId);

  // insert behind pagePtr
  // pagePtr -> newPage -> nextPage;

  BufferPage *nextPage = pagePtr->getNextPage();
  // nextPagePtr != tail
  if (nextPage != nullptr) {
    newPage->setNextPage(nextPage);
    newPage->setPrevPage(pagePtr);
    pagePtr->setNextPage(newPage);
    nextPage->setPrevPage(newPage);
  } else {
    newPage->setNextPage(nullptr);
    newPage->setPrevPage(pagePtr);
    pagePtr->setNextPage(newPage);
    blbs->tailPage = newPage;
  }

  _freePageList.pop_front();
  // K2LOG_I(log::pbrb, "Alloc new page type 2: sid: {}, sname: {},
  // currPageNum: {}, fromPagePtr: {}", schemaId,
  // _bufferMap[schemaId].schema->name, _bufferMap[schemaId].curPageNum, (void
  // *)pagePtr);
  NKV_LOG_D(std::cout, "Remaining _freePageList size:{}", _freePageList.size());

  return newPage;
}

// return pagePtr and rowOffset.
std::pair<BufferPage *, RowOffset> PBRB::findPageAndRowByAddr(void *rowAddr) {
  BufferPage *pagePtr = getPageAddr(rowAddr);
  uint32_t offset = (uint64_t)rowAddr & mask;
  SchemaId sid = pagePtr->getSchemaIDPage();
  auto &blbs = _bufferMap[sid];
  RowOffset rowOff =
      (offset - blbs->firstRowOffset - _pageHeaderSize) / blbs->rowSize;
  if ((offset - blbs->firstRowOffset - _pageHeaderSize) % blbs->rowSize != 0)
    NKV_LOG_I(std::cout, "WARN: offset maybe wrong!");
  return std::make_pair(pagePtr, rowOff);
}

RowAddr PBRB::getAddrByPageAndRow(BufferPage *pagePtr, RowOffset rowOff) {
  SchemaId sid = pagePtr->getSchemaIDPage();
  auto &blbs = _bufferMap[sid];
  uint32_t offset =
      _pageHeaderSize + blbs->firstRowOffset + rowOff * blbs->rowSize;
  return (uint8_t *)pagePtr + offset;
}

// Find position functions.

//

// @brief Find first empty slot in BufferPage pageptr
// @return rowOffset (UINT32_MAX for not found).
inline RowOffset PBRB::findEmptySlotInPage(
    std::shared_ptr<BufferListBySchema> &blbs, BufferPage *pagePtr,
    RowOffset beginOffset, RowOffset endOffset) {
  if (pagePtr->getHotRowsNumPage() >= blbs->maxRowCnt) {
    // NKV_LOG_I(std::cout, "BufferPage Full, skipped.");
    return UINT32_MAX;
  }
#ifdef ENABLE_BREAKDOWN
  PointProfiler timer;
  timer.start();
#endif
  uint32_t result = pagePtr->getFirstZeroBit(blbs->maxRowCnt);

#ifdef ENABLE_BREAKDOWN
  timer.end();
  findSlotNss.emplace_back(timer.duration());
#endif

  return result;
}

// Find first empty slot in linked list start with pagePtr.
std::pair<BufferPage *, RowOffset> PBRB::traverseFindEmptyRow(
    uint32_t schemaID, BufferPage *pagePtr, uint32_t maxPageSearchingNum) {
  if (maxPageSearchingNum == UINT32_MAX)
    maxPageSearchingNum = _maxPageSearchingNum;

  if (maxPageSearchingNum == 0) {
    NKV_LOG_E(std::cerr, "maxPageSearchingNum must > 0, adjusted to 1");
  }

  // Default: Traverse from headPage.
  if (pagePtr == nullptr) {
    auto &blbs = _bufferMap[schemaID];
    pagePtr = blbs->headPage;
    if (pagePtr == nullptr) {
      NKV_LOG_E(std::cerr,
                "[Schema: {}, sid: {}:] blbs->headPage == nullptr, Aborted "
                "traversing.",
                blbs->ownSchema->name, blbs->ownSchema->schemaId);
    }
  }

  BufferPage *travPagePtr = pagePtr;
  auto &blbs = _bufferMap[schemaID];
  uint32_t visitedPageNum = 1;
  while (visitedPageNum < maxPageSearchingNum && travPagePtr != nullptr) {
    RowOffset rowOff = findEmptySlotInPage(blbs, travPagePtr);
    if (rowOff != UINT32_MAX) {
      return std::make_pair(travPagePtr, rowOff);
    }
    visitedPageNum++;
    travPagePtr = travPagePtr->getNextPage();
  }

  // Didn't find en empty slot: need to allocate a new page.
  BufferPage *newPage = AllocNewPageForSchema(schemaID, pagePtr);

  // Current Stragegy: return the first slot of new page.
  return std::make_pair(newPage, 0);
}

//
std::pair<BufferPage *, RowOffset> PBRB::findCacheRowPosition(
    uint32_t schemaID, IndexerIterator iter) {
#ifdef ENABLE_BREAKDOWN
  PointProfiler fcrpTimer;
  fcrpTimer.start();
#endif
  if (iter == _indexListPtr->at(schemaID)->end()) {
    NKV_LOG_E(std::cerr, "iter == _indexer->end()");
    return std::make_pair(nullptr, 0);
  }

  auto bmIter = _bufferMap.find(schemaID);

  if (_bufferMap.find(schemaID) == _bufferMap.end()) {
    NKV_LOG_E(std::cerr, "Cannot find buffer list info with schemaID: {}",
              schemaID);
    return std::make_pair(nullptr, 0);
  }
#ifdef ENABLE_BREAKDOWN
  PointProfiler idxTimer;
  idxTimer.start();
#endif

  auto &blbs = bmIter->second;
  // Search in neighboring keys
  uint32_t maxIdxSearchNum = 3;
  IndexerIterator nextIter = iter;

  BufferPage *nextPagePtr = nullptr;
  RowOffset nextOff = UINT32_MAX;
  for (int i = 0;
       i < maxIdxSearchNum && nextIter != _indexListPtr->at(schemaID)->end();
       i++) {
    nextIter++;
    if (nextIter == _indexListPtr->at(schemaID)->end()) break;
    auto valuePtr = &nextIter->second;
    if (valuePtr->isHot()) {
      RowAddr rowAddr = valuePtr->getPBRBAddr();
      auto retVal = findPageAndRowByAddr(rowAddr);
      nextPagePtr = retVal.first;
      nextOff = retVal.second;
      break;
    }
  }

#ifdef ENABLE_BREAKDOWN
  idxTimer.end();
  searchingIdxNss.emplace_back(idxTimer.duration());
#endif
  std::pair<BufferPage *, RowOffset> result = std::make_pair(nullptr, 0);
  // Case 1: key -> nullptr
  if (nextPagePtr == nullptr) {
    result = traverseFindEmptyRow(schemaID);
  }
  // Case 2: key -> nextPagePtr findEmptySlotInPage(maxRowCnt,
  // nextPagePtr, 0, endOffset);

  else if (nextPagePtr != nullptr) {
    RowOffset rowOff = findEmptySlotInPage(blbs, nextPagePtr, nextOff);
    if (rowOff == UINT32_MAX)
      result = traverseFindEmptyRow(schemaID, nextPagePtr->getNextPage());
    else
      result = std::make_pair(nextPagePtr, rowOff);
  }

  NKV_LOG_D(std::cout, "Return a slot: pagePtr: {}, offset: {}",
            (void *)result.first, result.second);

#ifdef ENABLE_BREAKDOWN
  fcrpTimer.end();
  fcrpNss.emplace_back(fcrpTimer.duration());
#endif

  return result;
}

// Extern interfaces:

bool PBRB::read(TimeStamp oldTS, TimeStamp newTS, const RowAddr addr,
                SchemaId schemaid, Value &value, ValuePtr *vPtr) {
  BufferPage *pagePtr = getPageAddr(addr);
  auto &blbs = _bufferMap[schemaid];
  value = pagePtr->getValueRow(addr, blbs->valueSize);
  // Validation:
  if (pagePtr->getTimestampRow(addr).ne(oldTS)) {
    // Expired: Somebody updated the row.
    // TODO: Handle this case
    NKV_LOG_E(std::cerr, "Expired Timestamp, Aborted.");
    NKV_LOG_E(std::cerr,
              "Detail: [oldTS: {}, vPtr->timestamp: {}, rowTS: {}, readNewTS: "
              "{}, value: {}]",
              oldTS, vPtr->getTimestamp(), pagePtr->getTimestampRow(addr),
              newTS, value);
    return false;
  } else {
    auto &blbs = _bufferMap[schemaid];
    value = pagePtr->getValueRow(addr, blbs->valueSize);
    pagePtr->setTimestampRow(addr, newTS);
    vPtr->updateTS(newTS);
    NKV_LOG_D(
        std::cout,
        "PBRB: Successfully read row [ts: {}, value: {}, value.size(): {}]",
        oldTS, value, value.size());
    return true;
  }
}
bool PBRB::write(TimeStamp oldTS, TimeStamp newTS, SchemaId schemaId,
                 const Value &value, IndexerIterator iter) {
  if (_bufferMap.find(schemaId) == _bufferMap.end()) {
    NKV_LOG_I(
        std::cout,
        "A new schema (sid: {}) will be inserted into _bufferMap:", schemaId);
    createCacheForSchema(schemaId);
  }
  if (_async_pbrb == true) {
    return asyncWrite(oldTS, newTS, schemaId, value, iter);
  }
  return syncWrite(oldTS, newTS, schemaId, value, iter);
}

bool PBRB::syncWrite(TimeStamp oldTS, TimeStamp newTS, SchemaId schemaid,
                     const Value &value, IndexerIterator iter) {
  auto valuePtr = &iter->second;

  // Check value size:
  auto &blbs = _bufferMap[schemaid];
  if (blbs->valueSize != value.size()) {
    NKV_LOG_E(std::cerr, "Value size: {} != blbs->valueSize: {}, Aborted",
              value.size(), blbs->valueSize);
    return false;
  }

  //  2. Find a position.
  auto retVal = findCacheRowPosition(schemaid, iter);
  BufferPage *pagePtr = retVal.first;
  RowOffset rowOffset = retVal.second;
  if (pagePtr == nullptr) {
    NKV_LOG_E(std::cout, "Warning: Cannot find empty slot!");
    return false;
  }
  RowAddr rowAddr = getAddrByPageAndRow(pagePtr, rowOffset);

  // 3. copy row.
  // copy header:
  {
    std::lock_guard<std::mutex> lockGuard(writeLock_);
    pagePtr->setTimestampRow(rowAddr, newTS);
    pagePtr->setPlogAddrRow(rowAddr, valuePtr->getPmemAddr());
    pagePtr->setKVNodeAddrRow(rowAddr, valuePtr);
    // copy row content:
    pagePtr->setValueRow(rowAddr, value, blbs->valueSize);
    pagePtr->setRowBitMapPage(rowOffset);
    blbs->curRowNum++;
  }

  // 4. Check consistency
  if (valuePtr->getTimestamp().ne(oldTS)) {
    // Rollback
    pagePtr->clearRowBitMapPage(rowOffset);
    blbs->curRowNum--;
    NKV_LOG_I(
        std::cout,
        "PBRB: Write [{}] operation timestamp conflict with hot {}. Rollback",
        value, valuePtr->isHot());
    return false;
  }

  // 5. Update ValuePtr
  valuePtr->updatePBRBAddr(rowAddr, newTS);
  NKV_LOG_D(
      std::cout,
      "PBRB: Successfully sync write row [ts: {}, value: {}, value.size(): {}]",
      oldTS, value, value.size());
  return true;
}

bool PBRB::asyncWrite(TimeStamp oldTS, TimeStamp newTS, SchemaId schemaId,
                      const Value &value, IndexerIterator iter) {
  auto res =
      _asyncQueueMap[schemaId]->EnqueueOneEntry(oldTS, newTS, iter, value);
  if (res == false) {
    NKV_LOG_I(std::cout, "Fail to insert {} to buffer queue", value);
  }
  return res;
}

void PBRB::asyncWriteHandler(decltype(&_asyncThreadPollList) pollList) {
  while(true){
    if (_isAsyncRunning.load(std::memory_order_relaxed))
    break;
  }
  while (_isAsyncRunning.load(std::memory_order_relaxed)) {
    if (pollList->empty() == true) {
      std::this_thread::yield();
      continue;
    }
    for (auto asyncBuffer : *pollList) {
      if (!asyncBuffer->Empty()) {
        auto bufferEntry = asyncBuffer->DequeueOneEntry();
        if (bufferEntry != nullptr) {
          syncWrite(bufferEntry->_oldTS, bufferEntry->_newTS,
                    asyncBuffer->getSchemaId(), bufferEntry->_entry_content,
                    bufferEntry->_iter);
        }
      }
    }
    // std::this_thread::yield();
  }
}
bool PBRB::dropRow(RowAddr rAddr) {
  auto [pagePtr, rowOffset] = findPageAndRowByAddr(rAddr);
  bool result = pagePtr->clearRowBitMapPage(rowOffset);
  if (result == true) {
    auto &blbs = _bufferMap.at(pagePtr->getSchemaIDPage());
    blbs->curRowNum--;
    if (pagePtr->getHotRowsNumPage() == 0)
      blbs->reclaimPage(_freePageList, pagePtr);
    return true;
  }
  return false;
}

bool PBRB::evictRow(IndexerIterator &iter) {
  RowAddr rAddr = iter->second.getPBRBAddr();
  iter->second.setIsHot(false);
  if (dropRow(rAddr) == false) return false;
  _evictCnt++;
  return true;
}

bool PBRB::traverseIdxGC() {
  std::vector<SchemaId> GCSchemaVec;
  double occupancyRatio = _getOccupancyRatio();

  NKV_LOG_I(std::cout,
            "Current number of free pages / max page number : ({} / {}), "
            "OccupancyRatio = {:.3f}",
            _freePageList.size(), getMaxPageNumber(), occupancyRatio);
  if (occupancyRatio < targetOccupancyRatio) {
    return true;
  }

  for (auto &it : *_schemaUMap) {
    auto bMapIter = _bufferMap.find(it.first);
    if (bMapIter != _bufferMap.end() && bMapIter->second->curRowNum.load() != 0)
      GCSchemaVec.emplace_back(it.first);
  }

  std::sort(GCSchemaVec.begin(), GCSchemaVec.end(),
            [&](const SchemaId &a, const SchemaId &b) -> bool {
              return this->_AccStatBySchema[a].getHitRatio() <
                     this->_AccStatBySchema[b].getHitRatio();
            });

  bool achieved = true;
  for (auto &schemaId : GCSchemaVec) {
    if (_traverseIdxGCBySchema(schemaId)) {
      achieved = true;
      break;
    }
  }
  NKV_LOG_I(std::cout, "After GC: Occupancy Ratio: {}", _getOccupancyRatio());
  return true;
}

bool PBRB::_checkOccupancyRatio(double ratio) {
  return _getOccupancyRatio() < ratio;
}

bool PBRB::_traverseIdxGCBySchema(SchemaId schemaid) {
  TimeStamp startTS;
  startTS.getNow();
  // Adjust retention window size
  TimeStamp watermark = startTS;
  double occupancyRatio = _bufferMap.at(schemaid)->getOccupancyRatio();
  if (occupancyRatio == 0) return false;
  uint64_t moveNanoSecs = (double)_retentionWindowSecs * NANOSEC_BASE *
                          exp2(-GCFailedTimes) * (1 - occupancyRatio) /
                          (1 - targetOccupancyRatio);
  watermark.moveBackward(getTicksByNanosecs(moveNanoSecs));
  NKV_LOG_I(std::cout,
            "Start to GC schema id: {}, startTS: {}, watermark: {}, "
            "_retentionWindowSecs: {}s",
            schemaid, startTS, watermark, _retentionWindowSecs);
  NKV_LOG_I(std::cout, "Occupancy Ratio: {:.4f}, real moved nanoSecs:{:12d}",
            occupancyRatio, moveNanoSecs);

  // Traversal
  uint64_t evictCnt = 0;
  auto &idx = _indexListPtr->at(schemaid);
  bool achieveTarget = false;
  for (auto iter = idx->begin(); iter != idx->end(); iter++) {
    // Compare with watermark
    ValuePtr &valuePtr = iter->second;
    if (valuePtr.isHot() == false || valuePtr.getTimestamp().gt(watermark))
      continue;
    if (evictRow(iter)) {
      evictCnt++;
      if (_checkOccupancyRatio(targetOccupancyRatio)) {
        achieveTarget = true;
        break;
      }
    }
  }

  // GC Failed Times
  if (achieveTarget) {
    GCFailedTimes = 0;
  } else
    GCFailedTimes++;
  // auto reclaimed = _bufferMap.at(schemaid).reclaimEmptyPages(_freePageList);
  NKV_LOG_I(
      std::cout,
      "End GC for schema id: {}, evicted: {} rows, occupancy ratio: {:.6f}",
      schemaid, evictCnt, _bufferMap.at(schemaid)->getOccupancyRatio());
  return achieveTarget;
}

bool PBRB::_asyncTraverseIdxGC() {
  while (_isGCRunning.load(std::memory_order_relaxed)) {
    if (_checkOccupancyRatio(startGCOccupancyRatio)) {
      std::this_thread::sleep_for(_gcIntervalus);
      // std::this_thread::yield();
    } else {
      std::lock_guard<std::mutex> guard(traverseIdxGCLock_);
      traverseIdxGC();
    }
  };
  return true;
}

void PBRB::_stopGC() {
  _isGCRunning.store(false, std::memory_order_relaxed);
  _GCResult.wait();
  if (_GCResult.get() == true)
    NKV_LOG_I(std::cout, "Successfully stopped _asyncGC.");
}
}  // namespace NKV
