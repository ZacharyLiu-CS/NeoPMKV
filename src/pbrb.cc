//
//  pbrb.cc
//  PROJECT pbrb
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "pbrb.h"

namespace NKV {
PBRB::PBRB(int maxPageNumber, TimeStamp *wm, IndexerT *indexer,
           SchemaUMap *umap, uint32_t maxPageSearchNum) {
  // check headerSizes
  static_assert(PAGE_HEADER_SIZE == 64, "PAGE_HEADER_SIZE != 64");
  static_assert(ROW_HEADER_SIZE == 28, "ROW_HEADER_SIZE != 28");
  // initialization

  _watermark = *wm;
  _schemaUMap = umap;
  _maxPageNumber = maxPageNumber;
  _indexer = indexer;
  _maxPageSearchingNum = maxPageSearchNum;

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
  // Get a page and set schemaMetadata.
  BufferPage *pagePtr = _freePageList.front();
  BufferListBySchema blbs(schemaId, _pageSize, _pageHeaderSize, _rowHeaderSize,
                          _schemaUMap, pagePtr);
  _bufferMap.insert_or_assign(schemaId, blbs);

  NKV_LOG_I(
      std::cout,
      "createCacheForSchema, schemaId: {}, pagePtr empty:{}, _freePageList "
      "size:{}, pageSize: {}, smd.rowSize: {}, _bufferMap[0].rowSize: {}",
      schemaId, pagePtr == nullptr, _freePageList.size(), sizeof(BufferPage),
      blbs.rowSize, _bufferMap[schemaId].rowSize);

  // Initialize Page.
  // memset(pagePtr, 0, sizeof(BufferPage));

  pagePtr->initializePage(blbs.occuBitmapSize);
  pagePtr->setSchemaIDPage(schemaId);
  pagePtr->setSchemaVerPage(schemaVer);

  _freePageList.pop_front();

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
      (offset - blbs.fieldsInfo[0].fieldOffset - _pageHeaderSize) /
      blbs.rowSize;
  if ((offset - blbs.fieldsInfo[0].fieldOffset - _pageHeaderSize) %
          blbs.rowSize !=
      0)
    NKV_LOG_I(std::cout, "WARN: offset maybe wrong!");
  return std::make_pair(pagePtr, rowOff);
}

RowAddr PBRB::getAddrByPageAndRow(BufferPage *pagePtr, RowOffset rowOff) {
  SchemaId sid = pagePtr->getSchemaIDPage();
  BufferListBySchema &blbs = _bufferMap[sid];
  uint32_t offset =
      _pageHeaderSize + blbs.fieldsInfo[0].fieldOffset + rowOff * blbs.rowSize;
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
  return pagePtr->getFirstZeroBit(blbs.maxRowCnt);
}

// Find first empty slot in linked list start with pagePtr.
std::pair<BufferPage *, RowOffset> PBRB::traverseFindEmptyRow(
    uint32_t schemaID, BufferPage *pagePtr, uint32_t maxPageSearchingNum) {
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
  if (iter == _indexer->end()) {
    NKV_LOG_E(std::cerr, "iter == _indexer->end()");
    return std::make_pair(nullptr, 0);
  }

  auto bmIter = _bufferMap.find(schemaID);

  if (_bufferMap.find(schemaID) == _bufferMap.end()) {
    NKV_LOG_E(std::cerr, "Cannot find buffer list info with schemaID: {}",
              schemaID);
    return std::make_pair(nullptr, 0);
  }

  auto &blbs = bmIter->second;
  // Search in neighboring keys
  uint32_t maxIdxSearchNum = 5;
  IndexerIterator prevIter = iter;
  IndexerIterator nextIter = iter;

  BufferPage *prevPagePtr = nullptr;
  RowOffset prevOff = UINT32_MAX;
  for (int i = 0; i < maxIdxSearchNum; i++) {
    if (prevIter != _indexer->begin()) {
      prevIter--;
      auto valuePtr = &prevIter->second;
      if (valuePtr->isHot) {
        RowAddr rowAddr = valuePtr->addr.pbrbAddr;
        auto retVal = findPageAndRowByAddr(rowAddr);
        prevPagePtr = retVal.first;
        prevOff = retVal.second;
      }
    } else
      break;
  }

  BufferPage *nextPagePtr = nullptr;
  RowOffset nextOff = UINT32_MAX;
  for (int i = 0; i < maxIdxSearchNum && nextIter != _indexer->end(); i++) {
    nextIter++;
    if (nextIter == _indexer->end()) break;
    auto valuePtr = &nextIter->second;
    if (valuePtr->isHot) {
      RowAddr rowAddr = valuePtr->addr.pbrbAddr;
      auto retVal = findPageAndRowByAddr(rowAddr);
      nextPagePtr = retVal.first;
      nextOff = retVal.second;
    }
  }

  std::pair<BufferPage *, RowOffset> result = std::make_pair(nullptr, 0);
  // Case 1: nullptr <- key -> nullptr
  if (prevPagePtr == nullptr && nextPagePtr == nullptr) {
    result = traverseFindEmptyRow(schemaID);
  }
  // Case 2.1: prevPagePtr <- key -> nullptr findEmptySlotInPage(maxRowCnt,
  // prevPagePtr, beginOffset, UINT32_MAX);
  else if (prevPagePtr != nullptr && nextPagePtr == nullptr) {
    RowOffset rowOff = findEmptySlotInPage(blbs, prevPagePtr, prevOff);
    if (rowOff == UINT32_MAX)
      result = traverseFindEmptyRow(schemaID, prevPagePtr->getNextPage());
    else
      result = std::make_pair(prevPagePtr, rowOff);
  }
  // Case 2.2: nullptr <- key -> nextPagePtr findEmptySlotInPage(maxRowCnt,
  // nextPagePtr, 0, endOffset);

  else if (prevPagePtr == nullptr && nextPagePtr != nullptr) {
    RowOffset rowOff = findEmptySlotInPage(blbs, nextPagePtr, prevOff);
    if (rowOff == UINT32_MAX)
      result = traverseFindEmptyRow(schemaID, nextPagePtr->getNextPage());
    else
      result = std::make_pair(nextPagePtr, rowOff);
  }

  // Case 3: prevPagePtr <- key -> nextPagePtr
  else {
    // Case 3.1: Same Page:  findEmptySlotInPage(maxRowCnt, prevPagePtr,
    // beginOffset, endOffset);
    if (prevPagePtr == nextPagePtr) {
      RowOffset beginOff = 0, endOff = UINT32_MAX;
      if (prevOff < nextOff) {
        beginOff = prevOff;
        endOff = nextOff;
      }

      else {
        beginOff = nextOff;
        endOff = prevOff;
      }
      RowOffset rowOff =
          findEmptySlotInPage(blbs, prevPagePtr, beginOff, endOff);
      if (rowOff == UINT32_MAX)
        result = traverseFindEmptyRow(schemaID, prevPagePtr->getNextPage());
      else
        result = std::make_pair(prevPagePtr, rowOff);
    }
    // Case 3.2: Diff Page:  findEmptySlotInPage(maxRowCnt,
    // lowerPtr);
    else {
      uint32_t prevHotNum = prevPagePtr->getHotRowsNumPage();
      uint32_t nextHotNum = nextPagePtr->getHotRowsNumPage();

      // Case 3.2.1
      if (prevHotNum <= nextHotNum) {
        RowOffset rowOff = findEmptySlotInPage(blbs, prevPagePtr);
        if (rowOff == UINT32_MAX)
          result = traverseFindEmptyRow(schemaID, prevPagePtr->getNextPage());
        else
          result = std::make_pair(prevPagePtr, rowOff);
      }

      // Case 3.2.2
      else {
        RowOffset rowOff = findEmptySlotInPage(blbs, nextPagePtr);
        if (rowOff == UINT32_MAX)
          result = traverseFindEmptyRow(schemaID, nextPagePtr->getNextPage());
        else
          result = std::make_pair(nextPagePtr, rowOff);
      }
    }
  }

  NKV_LOG_D(std::cout, "Return a slot: pagePtr: {}, offset: {}",
            (void *)result.first, result.second);
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
    vPtr->timestamp = newTS;
    NKV_LOG_D(
        std::cout,
        "PBRB: Successfully read row [ts: {}, value: {}, value.size(): {}]",
        oldTS, value, value.size());
    return true;
  }
}

bool PBRB::syncwrite(TimeStamp oldTS, TimeStamp newTS, SchemaId schemaid,
                 const Value &value, IndexerIterator iter) {
  auto valuePtr = &iter->second;

  if (_bufferMap.find(schemaid) == _bufferMap.end()) {
    NKV_LOG_I(
        std::cout,
        "A new schema (sid: {}) will be inserted into _bufferMap:", schemaid);
    createCacheForSchema(schemaid);
  }
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
    pagePtr->setTimestampRow(rowAddr, oldTS);
    pagePtr->setPlogAddrRow(rowAddr, valuePtr->addr.pmemAddr);
    pagePtr->setKVNodeAddrRow(rowAddr, valuePtr);
    // copy row content:
    pagePtr->setValueRow(rowAddr, value, blbs.valueSize);
    pagePtr->setRowBitMapPage(rowOffset);
    blbs.curRowNum++;
  }

  // 4. Check consistency
  if (valuePtr->timestamp.ne(oldTS)) {
    // Rollback
    pagePtr->clearRowBitMapPage(rowOffset);
    blbs.curRowNum--;
    NKV_LOG_D(std::cout, "PBRB: Write operation timestamp conflict. Rollback");
    return false;
  }

  // 5. Update ValuePtr
  _updateValuePtr(newTS, valuePtr, rowAddr, true);
  NKV_LOG_D(
      std::cout,
      "PBRB: Successfully write row [ts: {}, value: {}, value.size(): {}]",
      oldTS, value, value.size());
  return true;
}
bool PBRB::dropRow(RowAddr rAddr) {
  auto [pagePtr, rowOffset] = findPageAndRowByAddr(rAddr);
  return pagePtr->clearRowBitMapPage(rowOffset);
}
}  // namespace NKV
