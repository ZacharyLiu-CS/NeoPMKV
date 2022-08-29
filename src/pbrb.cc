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
           uint32_t maxPageSearchNum) {
  // check headerSizes
  static_assert(PAGE_HEADER_SIZE == 64, "PAGE_HEADER_SIZE != 64");
  static_assert(ROW_HEADER_SIZE == 36, "ROW_HEADER_SIZE != 32");
  // initialization
  _watermark = *wm;
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

RowOffset PBRB::findEmptySlotInPage(uint32_t maxRowCnt, BufferPage *pagePtr,
                                    RowOffset beginOffset,
                                    RowOffset endOffset) {
  return pagePtr->getFirstZeroBit(maxRowCnt);
}

std::pair<BufferPage *, RowOffset> PBRB::traverseFindEmptyRow(
    uint32_t schemaID, BufferPage *pagePtr, uint32_t maxPageSearchingNum) {
  if (maxPageSearchingNum == 0) {
    NKV_LOG_E(std::cerr, "maxPageSearchingNum must > 0, adjusted to 1");
  }

  BufferPage *travPagePtr = pagePtr;
  BufferListBySchema &blbs = _bufferMap[schemaID];
  uint32_t visitedPageNum = 1;
  while (visitedPageNum < maxPageSearchingNum && travPagePtr != nullptr) {
    RowOffset rowOff = findEmptySlotInPage(blbs.maxRowCnt, travPagePtr);
    if (rowOff != UINT32_MAX) {
      return std::make_pair(travPagePtr, rowOff);
    }
    visitedPageNum++;
    travPagePtr = travPagePtr->getNextPage();
  }
  return std::make_pair(nullptr, 0);
}


BufferPage *PBRB::createCacheForSchema(SchemaId schemaId, SchemaVer schemaVer) {
  if (_freePageList.empty()) return nullptr;

  // Get a page and set schemaMetadata.
  BufferPage *pagePtr = _freePageList.front();
  BufferListBySchema bmd(schemaId, _pageSize, _pageHeaderSize, _rowHeaderSize,
                         this, pagePtr);
  _bufferMap.insert_or_assign(schemaId, bmd);

  // Initialize Page.
  // memset(pagePtr, 0, sizeof(BufferPage));

  pagePtr->initializePage(bmd.occuBitmapSize);
  pagePtr->setSchemaIDPage(schemaId);
  pagePtr->setSchemaVerPage(schemaVer);

  NKV_LOG_D(
      std::cout,
      "createCacheForSchema, schemaId: {}, pagePtr empty:{}, _freePageList "
      "size:{}, pageSize: {}, smd.rowSize: {}, _bufferMap[0].rowSize: {}",
      schemaId, pagePtr == nullptr, _freePageList.size(), sizeof(BufferPage),
      bmd.rowSize, _bufferMap[0].rowSize);

  _freePageList.pop_front();

  return pagePtr;
}

BufferPage *PBRB::AllocNewPageForSchema(SchemaId schemaId) {
  // al1
  PointProfiler pointTimer;
  pointTimer.start();

  if (_freePageList.empty()) return nullptr;

  BufferPage *pagePtr = _bufferMap[schemaId].headPage;
  _bufferMap[schemaId].curPageNum++;

  auto al1ns = pointTimer.end();

  if (pagePtr == nullptr)
    return createCacheForSchema(schemaId);
  else {
    // al2
    pointTimer.start();
    assert(_freePageList.size() > 0);
    BufferPage *newPage = _freePageList.front();

    // Initialize Page.
    newPage->initializePage(_bufferMap[schemaId].occuBitmapSize);
    newPage->setSchemaIDPage(schemaId);
    newPage->setSchemaVerPage(_bufferMap[schemaId].ownSchema->version);

    // optimize: using tailPage.
    BufferPage *tail = _bufferMap[schemaId].tailPage;
    // set nextpage
    newPage->setPrevPage(tail);
    newPage->setNextPage(nullptr);
    tail->setNextPage(newPage);
    _bufferMap[schemaId].tailPage = newPage;

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
              _bufferMap[schemaId].ownSchema->name,
              _bufferMap[schemaId].curPageNum, al1ns, al2ns, al3ns,
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
  BufferPage *newPage = _freePageList.front();
  _bufferMap[schemaId].curPageNum++;

  // Initialize Page.
  // memset(newPage, 0, sizeof(BufferPage));
  newPage->initializePage(_bufferMap[schemaId].occuBitmapSize);
  newPage->setSchemaIDPage(schemaId);
  newPage->setSchemaVerPage(_bufferMap[schemaId].ownSchema->version);

  // insert behind pagePtr
  // pagePtr -> newPage -> nextPage;

  BufferPage *nextPage = pagePtr->getNextPage();
  // nextPtr != tail
  if (nextPage != nullptr) {
    newPage->setNextPage(nextPage);
    newPage->setPrevPage(pagePtr);
    pagePtr->setNextPage(newPage);
    nextPage->setPrevPage(newPage);
  } else {
    newPage->setNextPage(nullptr);
    newPage->setPrevPage(pagePtr);
    pagePtr->setNextPage(newPage);
    _bufferMap[schemaId].tailPage = newPage;
  }

  _freePageList.pop_front();
  // K2LOG_I(log::pbrb, "Alloc new page type 2: sid: {}, sname: {},
  // currPageNum: {}, fromPagePtr: {}", schemaId,
  // _bufferMap[schemaId].schema->name, _bufferMap[schemaId].curPageNum, (void
  // *)pagePtr);
  NKV_LOG_D(std::cout, "Remaining _freePageList size:{}", _freePageList.size());

  return newPage;
}
  
}  // namespace NKV
