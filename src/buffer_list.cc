
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
  Schema *res = bePBRBPtr->_schemaUMap.find(schemaId);
  if (res == nullptr) return;

  ownSchema = res;
  setNullBitmapSize(ownSchema->fields.size());

  uint32_t currRowOffset = rowHeaderSize + nullableBitmapSize;
  valueSize = 0;
  // Set Metadata
  for (size_t i = 0; i < ownSchema->fields.size(); i++) {
    FieldType currFT = ownSchema->fields[i].type;
    FieldMetaData fieldObj;

    fieldObj.fieldSize = FTSize[(uint8_t)(currFT)];
    valueSize += fieldObj.fieldSize;
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