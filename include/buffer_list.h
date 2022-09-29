//
//  buffer_list.h
//  PROJECT buffer_list
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <atomic>
#include <list>
#include "buffer_page.h"
#include "schema.h"

namespace NKV {

class BufferPage;
class PBRB;

class BufferListBySchema {
 private:
  Schema *ownSchema = nullptr;
  uint32_t occuBitmapSize;
  uint32_t maxRowCnt;
  uint32_t valueSize = 0;
  uint32_t rowSize = 0;

  uint32_t firstRowOffset = 0;
  std::atomic<uint32_t> curPageNum;
  std::atomic<uint32_t> curRowNum;

  SchemaUMap *sUMap = nullptr;
  // manage the buffer list
  BufferPage *headPage = nullptr;
  BufferPage *tailPage = nullptr;

 public:
  // return occupancy Ratio
  double getOccupancyRatio() {
    uint32_t rNum, pNum;
    rNum = curRowNum.load();
    pNum = curPageNum.load();
    if (rNum != 0) {
      NKV_LOG_I(std::cout, "curRowNum: {}, maxRowNum: {}", rNum,
                pNum * maxRowCnt);
      return ((double)rNum) / (pNum * maxRowCnt);
    } else
      return 0;
  }

  BufferPage *getHeadPage() { return headPage; }

  BufferListBySchema() {}

  ~BufferListBySchema() {}

  BufferListBySchema(SchemaId schemaId, uint32_t pageSize,
                     uint32_t pageHeaderSize, uint32_t rowHeaderSize,
                     SchemaUMap *sUMapPtr, BufferPage *headPagePtr) {
    sUMap = sUMapPtr;
    headPage = headPagePtr;
    tailPage = headPagePtr;
    curRowNum = 0;
    curPageNum = 0;
    setInfo(schemaId, pageSize, pageHeaderSize, rowHeaderSize);
  }
  void setOccuBitmapSize(uint32_t pageSize);
  void setInfo(SchemaId schemaId, uint32_t pageSize, uint32_t pageHeaderSize,
               uint32_t rowHeaderSize);
  bool reclaimPage(std::list<BufferPage *> &freePageList, BufferPage *pagePtr);
  uint64_t reclaimEmptyPages(std::list<BufferPage *> &freePageList);
  friend class PBRB;
};  // end of struct BufferListBySchema
}  // end of namespace NKV