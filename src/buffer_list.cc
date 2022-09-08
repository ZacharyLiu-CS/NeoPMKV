
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

void BufferListBySchema::setNullBitmapSize(uint32_t fieldNumber) {
  nullableBitmapSize = 0;
  // nullableBitmapSize = (fieldNumber - 1) / 8 + 1;
}

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
  setNullBitmapSize(ownSchema->fields.size());

  uint32_t currRowOffset = rowHeaderSize + nullableBitmapSize;
  valueSize = 0;
  uint32_t fieldHeadSize = sizeof(uint32_t);
  // Set Metadata
  for (size_t i = 0; i < ownSchema->fields.size(); i++) {
    FieldMetaData fieldObj;
    fieldObj.fieldSize = ownSchema->fields[i].size;
    valueSize += (fieldObj.fieldSize + fieldHeadSize);

    currRowOffset += fieldHeadSize;
    fieldObj.fieldOffset = currRowOffset;
    fieldObj.isNullable = false;
    fieldObj.isVariable = false;
    fieldsInfo.push_back(fieldObj);
    // Go to next field.
    currRowOffset += fieldObj.fieldSize;
  }

  // set rowSize
  // rowSize = currRowOffset (align to 8 bytes)
  rowSize = ((currRowOffset - 1) / 8 + 1) * 8;

  NKV_LOG_D(std::cout,
            "PageHeaderSize: {}, OccupancyBitmapSize: {}, RowSize: {}",
            pageHeaderSize, occuBitmapSize, rowSize);
  setOccuBitmapSize(pageSize);
  maxRowCnt = (pageSize - pageHeaderSize - occuBitmapSize) / rowSize;
}
}  // end of namespace NKV