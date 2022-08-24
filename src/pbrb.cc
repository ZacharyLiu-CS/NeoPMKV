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

// 1. Header 'set' and 'get' functions.

// set (magic, 0, 2)
void PBRB::setMagicPage(BufferPage *pagePtr, uint16_t magic) {
  // magic is 2 bytes;
  pagePtr->writeToPage<uint16_t>(0, &magic, 2);
}

// get (magic, 0, 2)
uint16_t PBRB::getMagicPage(const BufferPage *pagePtr) {
  return pagePtr->readFromPage<uint16_t>(0, 2);
}

// set (schemaID, 2, 4)
void PBRB::setSchemaIDPage(BufferPage *pagePtr, uint32_t schemaID) {
  pagePtr->writeToPage<SchemaId>(2, &schemaID, 4);
}

// get (schemaID, 2, 4)
SchemaId PBRB::getSchemaIDPage(const BufferPage *pagePtr) {
  return pagePtr->readFromPage<uint32_t>(2, 4);
}

// set (schemaVer, 6, 2)
void PBRB::setSchemaVerPage(BufferPage *pagePtr, uint16_t schemaVer) {
  pagePtr->writeToPage<uint16_t>(6, &schemaVer, 2);
}

// get (schemaVer, 6, 2)
uint16_t PBRB::getSchemaVerPage(
    const BufferPage *pagePtr) {  // schemaVer is 2 bytes
  return pagePtr->readFromPage<uint16_t>(6, 2);
}

// get (prevPagePtr, 8, 8)
void PBRB::setPrevPage(BufferPage *pagePtr, BufferPage *prevPagePtr) {
  pagePtr->writeToPage<BufferPage *>(8, &prevPagePtr, 8);
}

// set (prevPagePtr, 8, 8)
BufferPage *PBRB::getPrevPage(const BufferPage *pagePtr) {
  return pagePtr->readFromPage<BufferPage *>(8, 8);
}

// set (nextPagePtr, 16, 8)
void PBRB::setNextPage(BufferPage *pagePtr,
                       BufferPage *nextPagePtr) {  // page pointer is 8 bytes
  pagePtr->writeToPage<BufferPage *>(16, &nextPagePtr, 8);
}

// get (nextPagePtr, 16, 8)
BufferPage *PBRB::getNextPage(const BufferPage *pagePtr) {
  return pagePtr->readFromPage<BufferPage *>(16, 8);
}

// set (hotRowsNum, 24, 2)
void PBRB::setHotRowsNumPage(BufferPage *pagePtr, uint16_t hotRowsNum) {
  pagePtr->writeToPage<uint16_t>(24, &hotRowsNum, 2);
}

// set (hotRowsNum, 24, 2)
uint16_t PBRB::getHotRowsNumPage(const BufferPage *pagePtr) {
  return pagePtr->readFromPage<uint16_t>(24, 2);
}

void PBRB::setReservedHeader(BufferPage *pagePtr) {  // reserved is 38 bytes
  memset(pagePtr->content + 26, 0, _pageHeaderSize - 26);
}

void PBRB::clearPageBitMap(BufferPage *pagePtr, uint32_t occuBitmapSize,
                           uint32_t maxRowCnt) {
  memset(pagePtr->content + _pageHeaderSize, 0, occuBitmapSize);
  for (uint32_t rowOffset = 0; rowOffset < maxRowCnt; rowOffset++) {
    uint32_t byteIndex = rowOffset / 8;
    uint32_t offset = rowOffset % 8;
    pagePtr->content[_pageHeaderSize + byteIndex] &= ~(0x1 << offset);
  }
}

void PBRB::setRowBitMapPage(BufferPage *pagePtr, RowOffset rowOffset) {
  auto &bmd = _bufferMap[getSchemaIDPage(pagePtr)];
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  pagePtr->content[_pageHeaderSize + byteIndex] |= 0x1 << offset;
  setHotRowsNumPage(pagePtr, getHotRowsNumPage(pagePtr) + 1);
  bmd.curRowNum++;
}

void PBRB::clearRowBitMapPage(BufferPage *pagePtr, RowOffset rowOffset) {
  auto &bmd = _bufferMap[getSchemaIDPage(pagePtr)];
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  pagePtr->content[_pageHeaderSize + byteIndex] &= ~(0x1 << offset);
  setHotRowsNumPage(pagePtr, getHotRowsNumPage(pagePtr) - 1);
  bmd.curRowNum--;
}

void PBRB::clearRowBitMap(BufferPage *pagePtr, RowOffset rowOffset) {
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  pagePtr->content[_pageHeaderSize + byteIndex] &= ~(0x1 << offset);
}

inline bool PBRB::isBitmapSet(BufferPage *pagePtr, RowOffset rowOffset) {
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  uint8_t bit = (pagePtr->content[_pageHeaderSize + byteIndex] >> offset) & 1;
  if (bit)
    return true;
  else
    return false;
}
void PBRB::initializePage(BufferPage *pagePtr, BufferListBySchema &bmd) {
  // Memset May Cause Performance Problems.
  // Optimized to just clear the header and occuBitMap
  memset(pagePtr, 0, _pageHeaderSize + bmd.occuBitmapSize);
  // memset(pagePtr, 0x00, sizeof(BufferPage));
  setMagicPage(pagePtr, 0x1010);
  setSchemaIDPage(pagePtr, -1);
  setSchemaVerPage(pagePtr, 0);
  setPrevPage(pagePtr, nullptr);
  setNextPage(pagePtr, nullptr);
  setHotRowsNumPage(pagePtr, 0);
  setReservedHeader(pagePtr);
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

  initializePage(pagePtr, bmd);
  setSchemaIDPage(pagePtr, schemaId);
  setSchemaVerPage(pagePtr, schemaVer);

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
    initializePage(newPage, _bufferMap[schemaId]);
    setSchemaIDPage(newPage, schemaId);
    setSchemaVerPage(newPage, _bufferMap[schemaId].ownSchema->version);

    // optimize: using tailPage.
    BufferPage *tail = _bufferMap[schemaId].tailPage;
    // set nextpage
    setPrevPage(newPage, tail);
    setNextPage(newPage, nullptr);
    setNextPage(tail, newPage);
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
  initializePage(newPage, _bufferMap[schemaId]);
  setSchemaIDPage(newPage, schemaId);
  setSchemaVerPage(newPage, _bufferMap[schemaId].ownSchema->version);

  // insert behind pagePtr
  // pagePtr -> newPage -> nextPage;

  BufferPage *nextPage = getNextPage(pagePtr);
  // nextPtr != tail
  if (nextPage != nullptr) {
    setNextPage(newPage, nextPage);
    setPrevPage(newPage, pagePtr);
    setNextPage(pagePtr, newPage);
    setPrevPage(nextPage, newPage);
  } else {
    setNextPage(newPage, nullptr);
    setPrevPage(newPage, pagePtr);
    setNextPage(pagePtr, newPage);
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
