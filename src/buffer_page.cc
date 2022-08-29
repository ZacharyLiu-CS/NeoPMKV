//
//  buffer_page.h
//  PROJECT buffer_page
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include "buffer_page.h"
#include "logging.h"
namespace NKV {

// 1. Header 'set' and 'get' functions.

// set (magic, 0, 2)
void BufferPage::setMagicPage(uint16_t magic) {
  // magic is 2 bytes;
  writeToPage<uint16_t>(0, &magic, 2);
}

// get (magic, 0, 2)
uint16_t BufferPage::getMagicPage() {
  return readFromPage<uint16_t>(0, 2);
}

// set (schemaID, 2, 4)
void BufferPage::setSchemaIDPage(uint32_t schemaID) {
  writeToPage<SchemaId>(2, &schemaID, 4);
}

// get (schemaID, 2, 4)
SchemaId BufferPage::getSchemaIDPage() {
  return readFromPage<uint32_t>(2, 4);
}

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

void BufferPage::clearPageBitMap(uint32_t occuBitmapSize,
                           uint32_t maxRowCnt) {
  memset(content + PAGE_HEADER_SIZE, 0, occuBitmapSize);
}

void BufferPage::setRowBitMapPage(RowOffset rowOffset) {
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  content[PAGE_HEADER_SIZE + byteIndex] |= 0x1 << offset;
  setHotRowsNumPage(getHotRowsNumPage() + 1);
}

void BufferPage::clearRowBitMapPage(RowOffset rowOffset) {
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  content[PAGE_HEADER_SIZE + byteIndex] &= ~(0x1 << offset);
  setHotRowsNumPage(getHotRowsNumPage() - 1);
}

void BufferPage::clearRowBitMap(RowOffset rowOffset) {
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  content[PAGE_HEADER_SIZE + byteIndex] &= ~(0x1 << offset);
}

inline bool BufferPage::isBitmapSet(RowOffset rowOffset) {
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
  setSchemaIDPage(-1);
  setSchemaVerPage(0);
  setPrevPage(nullptr);
  setNextPage(nullptr);
  setHotRowsNumPage(0);
  setReservedHeader();
}

} // end of namespace NKV