//
//  buffer_list.h
//  PROJECT buffer_list
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include "buffer_page.h"
#include "schema.h"

namespace NKV {

class BufferPage;
class PBRB;

class BufferListBySchema {
 private:
  Schema *ownSchema = nullptr;
  uint32_t occuBitmapSize;
  uint32_t nullableBitmapSize;
  uint32_t maxRowCnt;
  std::vector<FieldMetaData> fieldsInfo;
  uint32_t valueSize = 0;
  uint32_t rowSize;

  uint32_t curPageNum = 0;
  uint32_t curRowNum = 0;

  PBRB *bePBRBPtr = nullptr;
  // manage the buffer list
  BufferPage *headPage = nullptr;
  BufferPage *tailPage = nullptr;

 public:
  BufferPage *getHeadPage() { return headPage; }

  BufferListBySchema() {}

  BufferListBySchema(SchemaId schemaId, uint32_t pageSize,
                     uint32_t pageHeaderSize, uint32_t rowHeaderSize,
                     PBRB *pbrbPtr, BufferPage *headPagePtr) {
    bePBRBPtr = pbrbPtr;
    headPage = headPagePtr;
    tailPage = headPagePtr;
    setInfo(schemaId, pageSize, pageHeaderSize, rowHeaderSize);
  }
  void setNullBitmapSize(uint32_t fieldNumber);
  void setOccuBitmapSize(uint32_t pageSize);
  void setInfo(SchemaId schemaId, uint32_t pageSize, uint32_t pageHeaderSize,
               uint32_t rowHeaderSize);

  friend class PBRB;
};  // end of struct BufferListBySchema
}  // end of namespace NKV