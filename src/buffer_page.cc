//
//  buffer_page.h
//  PROJECT buffer_page
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "buffer_page.h"
#include "gtest/gtest_prod.h"
#include "logging.h"
namespace NKV {

// 1. Header 'set' and 'get' functions.

// set (magic, 0, 2)
void BufferPage::setMagicPage(uint16_t magic) {
  // magic is 2 bytes;
  writeToPage<uint16_t>(0, &magic, 2);
}

// get (magic, 0, 2)
uint16_t BufferPage::getMagicPage() { return readFromPage<uint16_t>(0, 2); }

// set (schemaID, 2, 4)
void BufferPage::setSchemaIDPage(uint32_t schemaID) {
  writeToPage<SchemaId>(2, &schemaID, 4);
}

// get (schemaID, 2, 4)
SchemaId BufferPage::getSchemaIDPage() { return readFromPage<uint32_t>(2, 4); }

// set (schemaVer, 6, 2)
void BufferPage::setSchemaVerPage(uint16_t schemaVer) {
  writeToPage<uint16_t>(6, &schemaVer, 2);
}

// get (schemaVer, 6, 2)
uint16_t BufferPage::getSchemaVerPage() {  // schemaVer is 2 bytes
  return readFromPage<uint16_t>(6, 2);
}

// get (prevPagePtr, 8, 8)
void BufferPage::setPrevPage(BufferPage *prevPagePtr) {
  writeToPage<BufferPage *>(8, &prevPagePtr, 8);
}

// set (prevPagePtr, 8, 8)
BufferPage *BufferPage::getPrevPage() {
  return readFromPage<BufferPage *>(8, 8);
}

// set (nextPagePtr, 16, 8)
void BufferPage::setNextPage(
    BufferPage *nextPagePtr) {  // page pointer is 8 bytes
  writeToPage<BufferPage *>(16, &nextPagePtr, 8);
}

// get (nextPagePtr, 16, 8)
BufferPage *BufferPage::getNextPage() {
  return readFromPage<BufferPage *>(16, 8);
}

// set (hotRowsNum, 24, 2)
void BufferPage::setHotRowsNumPage(uint16_t hotRowsNum) {
  writeToPage<uint16_t>(24, &hotRowsNum, 2);
}

// set (hotRowsNum, 24, 2)
uint16_t BufferPage::getHotRowsNumPage() {
  return readFromPage<uint16_t>(24, 2);
}

void BufferPage::setReservedHeader() {  // reserved is 38 bytes
  memset(content + 26, 0, PAGE_HEADER_SIZE - 26);
}

void BufferPage::clearPageBitMap(uint32_t occuBitmapSize) {
  memset(content + PAGE_HEADER_SIZE, 0, occuBitmapSize);
}

bool BufferPage::setRowBitMapPage(RowOffset rowOffset) {
  if (isBitmapSet(rowOffset)) return false;
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  content[PAGE_HEADER_SIZE + byteIndex] |= 0x1 << offset;
  setHotRowsNumPage(getHotRowsNumPage() + 1);
  return true;
}

bool BufferPage::clearRowBitMapPage(RowOffset rowOffset) {
  if (!isBitmapSet(rowOffset)) return false;
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  content[PAGE_HEADER_SIZE + byteIndex] &= ~(0x1 << offset);
  setHotRowsNumPage(getHotRowsNumPage() - 1);
  return true;
}

bool BufferPage::isBitmapSet(RowOffset rowOffset) {
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  uint8_t bit = (content[PAGE_HEADER_SIZE + byteIndex] >> offset) & 1;
  if (bit)
    return true;
  else
    return false;
}
void BufferPage::initializePage(uint32_t occuBitmapSize) {
  // Memset May Cause Performance Problems.
  // Optimized to just clear the header and occuBitMap
  memset(content, 0, PAGE_HEADER_SIZE + occuBitmapSize);
  // memset(pagePtr, 0x00, sizeof(BufferPage));
  setMagicPage(0x1010);
  setSchemaIDPage(0);
  setSchemaVerPage(0);
  setPrevPage(nullptr);
  setNextPage(nullptr);
  setHotRowsNumPage(0);
  setReservedHeader();
}

RowOffset BufferPage::getFirstZeroBit(uint32_t maxRowNumOfPage,
                                      uint32_t beginOffset,
                                      uint32_t endOffset) {
  if (maxRowNumOfPage == getHotRowsNumPage()) return UINT32_MAX;
  if (endOffset > maxRowNumOfPage) endOffset = maxRowNumOfPage;
  if (beginOffset > endOffset)
    NKV_LOG_E(std::cerr, "beginOffset: {} > endOffset: {}", beginOffset,
              endOffset);

  // No __builtin implementation:
  uint32_t beginByte = beginOffset / 8;
  uint32_t endByte = endOffset / 8;

  for (uint32_t byteIdx = beginByte; byteIdx <= endByte; byteIdx++) {
    uint8_t beginBit = 0, endBit = 8;
    // First Byte
    if (byteIdx == beginByte) beginBit = beginOffset % 8;
    // Last Byte
    if (byteIdx == endByte) endBit = endOffset % 8;

    uint8_t byteContent = content[PAGE_HEADER_SIZE + byteIdx];
    // Fix Byte
    uint8_t byteMask = ((1 << beginBit) - 1) | ~((1 << endBit) - 1);
    byteContent |= byteMask;
    NKV_LOG_D(std::cout, "ByteIdx: {}, byteMask: {}, byteContent: {}", byteIdx,
              byteMask, byteContent);
    if (byteContent != UINT8_MAX)
      for (uint8_t bitIdx = beginBit; bitIdx < endBit; bitIdx++)
        if (((byteContent >> bitIdx) & (uint8_t)1) == 0)
          return byteIdx * 8 + bitIdx;
  }
  
  return UINT32_MAX;
}

}  // end of namespace NKV