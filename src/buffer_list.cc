
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

  firstRowOffset = rowHeaderSize;
  valueSize = ownSchema->getSize();
  // set rowSize
  // rowSize = currRowOffset (align to 8 bytes)
  rowSize = ((valueSize + firstRowOffset - 1) / 8 + 1) * 8;

  NKV_LOG_D(std::cout,
            "PageHeaderSize: {}, OccupancyBitmapSize: {}, RowSize: {}",
            pageHeaderSize, occuBitmapSize, rowSize);
  setOccuBitmapSize(pageSize);
  maxRowCnt = (pageSize - pageHeaderSize - occuBitmapSize) / rowSize;
}
}  // end of namespace NKV