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

bool BufferPage::setRowBitMapPage(RowOffset rowOffset) {
  if (isBitmapSet(rowOffset)) return false;
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  ((PageHeader *)content)->occupancyBitmap.bitmap[byteIndex] |= 0x1 << offset;
  setHotRowsNumPage(getHotRowsNumPage() + 1);
  return true;
}

bool BufferPage::clearRowBitMapPage(RowOffset rowOffset) {
  if (!isBitmapSet(rowOffset)) return false;
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  ((PageHeader *)content)->occupancyBitmap.bitmap[byteIndex] &=
      ~(0x1 << offset);
  setHotRowsNumPage(getHotRowsNumPage() - 1);
  return true;
}

bool BufferPage::isBitmapSet(RowOffset rowOffset) {
  uint32_t byteIndex = rowOffset / 8;
  uint32_t offset = rowOffset % 8;
  uint8_t bit =
      (((PageHeader *)content)->occupancyBitmap.bitmap[byteIndex] >> offset) &
      1;
  if (bit)
    return true;
  else
    return false;
}
void BufferPage::initializePage() {
  // Memset May Cause Performance Problems.
  // Optimized to just clear the header and occuBitMap
  memset(content, 0, PBRB_PAGE_HEADER_SIZE);
  // memset(pagePtr, 0x00, sizeof(BufferPage));
  setMagicPage(0x1010);
  setSchemaIDPage(0);
  setHotRowsNumPage(0);
  setPrevPage(nullptr);
  setNextPage(nullptr);
  setReservedHeader();
}

RowOffset BufferPage::getFirstZeroBit(uint32_t maxRowNumOfPage,
                                      uint32_t beginOffset,
                                      uint32_t endOffset) {
  if (maxRowNumOfPage == getHotRowsNumPage()) return UINT32_MAX;
  if (endOffset > maxRowNumOfPage) endOffset = maxRowNumOfPage;
  if (beginOffset > endOffset) {
    NKV_LOG_E(std::cerr, "beginOffset: {} > endOffset: {}", beginOffset,
              endOffset);
    return UINT32_MAX;
  }

  // No __builtin implementation:
  uint32_t beginByte = beginOffset / 8;
  uint32_t endByte = endOffset / 8;

  for (uint32_t byteIdx = beginByte; byteIdx <= endByte; byteIdx++) {
    uint8_t beginBit = 0, endBit = 8;
    // First Byte
    if (byteIdx == beginByte) beginBit = beginOffset % 8;
    // Last Byte
    if (byteIdx == endByte) endBit = endOffset % 8;

    uint8_t byteContent = ((PageHeader *)content)->occupancyBitmap.bitmap[byteIdx];
    // Fix Byte
    uint8_t byteMask = ((1 << beginBit) - 1) | ~((1 << endBit) - 1);
    byteContent |= byteMask;
    NKV_LOG_D(std::cout, "ByteIdx: {}, byteMask: {}, byteContent: {}", byteIdx,
              byteMask, byteContent);
    if (byteContent != UINT8_MAX)
      for (uint8_t bitIdx = beginBit; bitIdx < endBit; bitIdx++)
        if (((byteContent >> bitIdx) & (uint8_t)1) == 0) {
          return byteIdx * 8 + bitIdx;
        }
  }

  return UINT32_MAX;
}

}  // end of namespace NKV