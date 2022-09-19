//
//  pbrb.cc
//  PROJECT pbrb
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "pbrb.h"

namespace NKV {
PBRB::PBRB(int maxPageNumber, TimeStamp *wm, IndexerList *indexerListPtr,
           SchemaUMap *umap, uint64_t retentionWindowSecs,
           uint32_t maxPageSearchNum, bool async_pbrb) {
  // check headerSizes
  static_assert(PAGE_HEADER_SIZE == 64, "PAGE_HEADER_SIZE != 64");
  static_assert(ROW_HEADER_SIZE == 28, "ROW_HEADER_SIZE != 28");
  // initialization

  _watermark = *wm;
  _schemaUMap = umap;
  _maxPageNumber = maxPageNumber;
  _indexListPtr = indexerListPtr;
  _maxPageSearchingNum = maxPageSearchNum;
  _retentionWindowSecs = retentionWindowSecs;
  _async_pbrb = async_pbrb;

  // allocate bufferpage
  auto aligned_val = std::align_val_t{_pageSize};
  _bufferPoolPtr = static_cast<BufferPage *>(operator new(
      maxPageNumber * sizeof(BufferPage), aligned_val));

  for (int idx = 0; idx < maxPageNumber; idx++) {
    _freePageList.push_back(_bufferPoolPtr + idx);
  }
}

PBRB::~PBRB() {
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
    _asyncQueue.insert_or_assign(
        schemaId, std::make_shared<AsyncBufferQueue>(schemaId, schemaSize,
                                                     pbrbAsyncQueueSize));
  }
  // Get a page and set schemaMetadata.
  BufferPage *pagePtr = _freePageList.front();

  _bufferMap.insert_or_assign(
      schemaId, BufferListBySchema(schemaId, _pageSize, _pageHeaderSize,
                                   _rowHeaderSize, _schemaUMap, pagePtr));
  BufferListBySchema &blbs = _bufferMap[schemaId];
  NKV_LOG_I(
      std::cout,
      "createCacheForSchema, schemaId: {}, pagePtr empty:{}, _freePageList "
      "size:{}, pageSize: {}, blbs.rowSize: {}, _bufferMap[{}].rowSize: {}, "
      "maxRowCnt: {}",
      schemaId, pagePtr == nullptr, _freePageList.size(), sizeof(BufferPage),
      blbs.rowSize, schemaId, _bufferMap[schemaId].rowSize, blbs.maxRowCnt);

  // Initialize Page.
  // memset(pagePtr, 0, sizeof(BufferPage));

  pagePtr->initializePage(blbs.occuBitmapSize);
  pagePtr->setSchemaIDPage(schemaId);
  pagePtr->setSchemaVerPage(schemaVer);

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

  BufferListBySchema &blbs = _bufferMap[schemaId];
  BufferPage *pagePtr = blbs.headPage;
  blbs.curPageNum++;

  auto al1ns = pointTimer.end();

  if (pagePtr == nullptr)
    return createCacheForSchema(schemaId);
  else {
    // al2
    pointTimer.start();
    assert(_freePageList.size() > 0);
    BufferPage *newPage = _freePageList.front();

    // Initialize Page.
    newPage->initializePage(blbs.occuBitmapSize);
    newPage->setSchemaIDPage(schemaId);
    newPage->setSchemaVerPage(blbs.ownSchema->version);

    // optimize: using tailPage.
    BufferPage *tail = blbs.tailPage;
    // set nextpage
    newPage->setPrevPage(tail);
    newPage->setNextPage(nullptr);
    tail->setNextPage(newPage);
    blbs.tailPage = newPage;

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
              blbs.ownSchema->name, blbs.curPageNum, al1ns, al2ns, al3ns,
              al1ns + al2ns + al3ns);

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

  BufferListBySchema &blbs = _bufferMap[schemaId];
  BufferPage *newPage = _freePageList.front();
  blbs.curPageNum++;

  // Initialize Page.
  // memset(newPage, 0, sizeof(BufferPage));
  newPage->initializePage(blbs.occuBitmapSize);
  newPage->setSchemaIDPage(schemaId);
  newPage->setSchemaVerPage(blbs.ownSchema->version);

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
    blbs.tailPage = newPage;
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
  BufferListBySchema &blbs = _bufferMap[sid];
  RowOffset rowOff =
      (offset - blbs.firstRowOffset - _pageHeaderSize) / blbs.rowSize;
  if ((offset - blbs.firstRowOffset - _pageHeaderSize) % blbs.rowSize != 0)
    NKV_LOG_I(std::cout, "WARN: offset maybe wrong!");
  return std::make_pair(pagePtr, rowOff);
}

RowAddr PBRB::getAddrByPageAndRow(BufferPage *pagePtr, RowOffset rowOff) {
  SchemaId sid = pagePtr->getSchemaIDPage();
  BufferListBySchema &blbs = _bufferMap[sid];
  uint32_t offset =
      _pageHeaderSize + blbs.firstRowOffset + rowOff * blbs.rowSize;
  return (uint8_t *)pagePtr + offset;
}

// Find position functions.

//

// @brief Find first empty slot in BufferPage pageptr
// @return rowOffset (UINT32_MAX for not found).
inline RowOffset PBRB::findEmptySlotInPage(BufferListBySchema &blbs,
                                           BufferPage *pagePtr,
                                           RowOffset beginOffset,
                                           RowOffset endOffset) {
  if (pagePtr->getHotRowsNumPage() >= blbs.maxRowCnt) {
    // NKV_LOG_I(std::cout, "BufferPage Full, skipped.");
    return UINT32_MAX;
  }
#ifdef ENABLE_BREAKDOWN
  PointProfiler timer;
  timer.start();
#endif

  uint32_t result = pagePtr->getFirstZeroBit(blbs.maxRowCnt);

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
    BufferListBySchema &blbs = _bufferMap[schemaID];
    pagePtr = blbs.headPage;
    if (pagePtr == nullptr) {
      NKV_LOG_E(std::cerr,
                "[Schema: {}, sid: {}:] blbs.headPage == nullptr, Aborted "
                "traversing.",
                blbs.ownSchema->name, blbs.ownSchema->schemaId);
    }
  }

  BufferPage *travPagePtr = pagePtr;
  BufferListBySchema &blbs = _bufferMap[schemaID];
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
  BufferListBySchema &blbs = _bufferMap[schemaid];
  value = pagePtr->getValueRow(addr, blbs.valueSize);
  // Validation:
  if (pagePtr->getTimestampRow(addr).gt(oldTS)) {
    // Expired: Somebody updated the row.
    // TODO: Handle this case
    NKV_LOG_E(std::cerr, "Expired Timestamp, Aborted.");
    return false;
  } else {
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
  BufferListBySchema &blbs = _bufferMap[schemaid];
  if (blbs.valueSize != value.size()) {
    NKV_LOG_E(std::cerr, "Value size: {} != blbs.valueSize: {}, Aborted",
              value.size(), blbs.valueSize);
    return false;
  }

  //  2. Find a position.
  auto retVal = findCacheRowPosition(schemaid, iter);
  BufferPage *pagePtr = retVal.first;
  RowOffset rowOffset = retVal.second;
  if (pagePtr == nullptr) {
    NKV_LOG_I(std::cout, "Warning: Cannot find empty slot!");
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
    pagePtr->setValueRow(rowAddr, value, blbs.valueSize);
    pagePtr->setRowBitMapPage(rowOffset);
    blbs.curRowNum++;
  }

  // 4. Check consistency
  if (valuePtr->getTimestamp().ne(oldTS)) {
    // Rollback
    pagePtr->clearRowBitMapPage(rowOffset);
    blbs.curRowNum--;
    NKV_LOG_D(std::cout, "PBRB: Write operation timestamp conflict. Rollback");
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
  return _asyncQueue[schemaId]->EnqueueOneEntry(oldTS, newTS, iter, value);
}

void PBRB::asyncWriteHandler() {}
bool PBRB::dropRow(RowAddr rAddr) {
  auto [pagePtr, rowOffset] = findPageAndRowByAddr(rAddr);
  bool result = pagePtr->clearRowBitMapPage(rowOffset);
  if (result == true) {
    BufferListBySchema &blbs = _bufferMap.at(pagePtr->getSchemaIDPage());
    blbs.curRowNum--;
    return true;
  }
  return false;
}

bool PBRB::evictRow(IndexerIterator &iter) {
  RowAddr rAddr = iter->second.getPBRBAddr();
  if (dropRow(rAddr) == false) return false;
  iter->second.setIsHot(false);
  _evictCnt++;
  return true;
}

bool PBRB::traverseIdxGC() {
  // TODO: Select GC Schema
  std::vector<SchemaId> GCSchemaVec;
  for (auto &it : *_schemaUMap) {
    GCSchemaVec.emplace_back(it.first);
  }

  for (auto &schemaId : GCSchemaVec) {
    _traverseIdxGCBySchema(schemaId);
  }

  return true;
}

bool PBRB::_traverseIdxGCBySchema(SchemaId schemaid) {
  TimeStamp startTS;
  startTS.getNow();
  // TODO: Adjust retention window size
  TimeStamp watermark = startTS;
  watermark.moveBackward(
      getTicksByNanosecs(_retentionWindowSecs * 1000000000ull));
  NKV_LOG_I(std::cout,
            "Start to GC schema id: {}, startTS: {}, watermark: {}, "
            "_retentionWindowSecs: {}s",
            schemaid, startTS, watermark, _retentionWindowSecs);

  auto &idx = _indexListPtr->at(schemaid);
  for (auto iter = idx->begin(); iter != idx->end(); iter++) {
    // Compare with watermark
    ValuePtr &valuePtr = iter->second;
    if (valuePtr.isHot() == false || valuePtr.getTimestamp().gt(watermark))
      continue;
    evictRow(iter);
  }

  return true;
}

}  // namespace NKV
