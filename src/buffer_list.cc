
//
//  buffer_list.cc
//  PROJECT buffer_list
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "buffer_list.h"
#include "buffer_page.h"
#include "pbrb.h"

namespace NKV {

void BufferListBySchema::setOccuBitmapSize(uint32_t pageSize) {
  occuBitmapSize = sizeof(OccupancyBitmap);
  // occuBitmapSize = (pageSize / rowSize - 1) / 8 + 1;
}
void BufferListBySchema::setInfo(SchemaId schemaId, uint32_t pageSize,
                                 uint32_t pageHeaderSize,
                                 uint32_t rowHeaderSize) {
  // read from Schema
  Schema *res = sUMap->find(schemaId);
  if (res == nullptr) return;

  ownSchema = res;
  setOccuBitmapSize(pageSize);

  firstRowOffset = 0;
  valueSize = ownSchema->getSize();
  // set rowSize
  // rowSize = currRowOffset (align to 8 bytes)
  rowSize = ((valueSize + rowHeaderSize - 1) / 8 + 1) * 8;

  NKV_LOG_D(std::cout,
            "PageHeaderSize: {}, OccupancyBitmapSize: {}, RowSize: {}",
            pageHeaderSize, occuBitmapSize, rowSize);
  setOccuBitmapSize(pageSize);
  maxRowCnt = (pageSize - pageHeaderSize) / rowSize;
}

bool BufferListBySchema::reclaimPage(std::list<BufferPage *> &freePageList,
                                     BufferPage *pagePtr) {
  // Case 1: head page
  if (pagePtr == headPage) {
    curPageNum--;
    headPage = pagePtr->getNextPage();

    // special: only 1 page now
    if (headPage == nullptr) {
      assert(curPageNum.load() == 0);
      tailPage = nullptr;
      freePageList.emplace_back(pagePtr);
      return true;
    }
    headPage->setPrevPage(nullptr);
  } else if (pagePtr == tailPage) {
    curPageNum--;
    tailPage = pagePtr->getPrevPage();
    tailPage->setNextPage(nullptr);
  } else {
    curPageNum--;
    BufferPage *prevPtr = pagePtr->getPrevPage();
    BufferPage *nextPtr = pagePtr->getNextPage();
    prevPtr->setNextPage(nextPtr);
    nextPtr->setPrevPage(prevPtr);
  }
  freePageList.emplace_back(pagePtr);
  return true;
}

uint64_t BufferListBySchema::reclaimEmptyPages(
    std::list<BufferPage *> &freePageList) {
  if (curPageNum.load() == 0) return 0;
  uint64_t reclaimedPageNum = 0;

  // Traverse linked list
  BufferPage *nextPage = nullptr;
  for (BufferPage *pagePtr = headPage; pagePtr != nullptr; pagePtr = nextPage) {
    nextPage = pagePtr->getNextPage();
    if (pagePtr->getHotRowsNumPage() == 0) continue;
    if (reclaimPage(freePageList, pagePtr)) reclaimedPageNum++;
  }
  return reclaimedPageNum;
}
}  // end of namespace NKV