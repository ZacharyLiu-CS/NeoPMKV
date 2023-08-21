//
//  schema.cc
//
//  Created by zhenliu on 17/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "schema.h"
#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include "field_type.h"
#include "mempool.h"
#include "profiler.h"
#include "timestamp.h"

namespace NKV {

void RowMetaHead::setMeta(uint16_t rSize, RowType rType, SchemaId sId,
                          SchemaVer sVersion) {
  this->rowSize = rSize;
  this->rowType = rType;
  this->schemaId = sId;
  this->schemaVersion = sVersion;
}
void PartialRowMeta::setMeta(PmemAddress pRow, uint8_t mSize,
                             vector<uint32_t> &fArr) {
  this->prevRow = pRow;
  this->metaSize = mSize;
  this->fieldCount = fArr.size();
  for (uint32_t i = 0; i < this->fieldCount; i++) {
    this->fieldArr[i] = (uint8_t)fArr[i];
  }
}

uint32_t PartialRowMeta::CalculateSize(uint32_t fieldCount) {
  return sizeof(PartialRowMeta) + fieldCount;
}

PartialSchema::PartialSchema(Schema *fullSchemaPtr, uint8_t *fields,
                             uint8_t fieldCount) {
  allFieldSize = 0;
  for (uint8_t i = 0; i < fieldCount; i++) {
    fMap.insert({fields[i], allFieldSize});
    allFieldSize += fullSchemaPtr->getSize(fields[i]);
  }
}
// Schema function part
Schema::Schema(std::string name, uint32_t schemaId, uint32_t primaryKeyField,
               std::vector<SchemaField> &fields)
    : name(name),
      version(0),
      schemaId(schemaId),
      primaryKeyField(primaryKeyField),
      fields(fields) {
  size += ROW_META_HEAD_SIZE;
  for (auto i : fields) {
    fieldsMeta.push_back(FieldMetaData());
    auto &field_meta = fieldsMeta.back();
    field_meta.fieldSize = i.size;
    field_meta.fieldOffset = size;
    field_meta.isNullable = false;
    field_meta.isVariable = false;
    size += i.size;
    allFieldSize += i.size;
    if (i.type == FieldType::VARSTR) {
      hasVariableField = true;
      field_meta.isVariable = true;
    }
  }
}
uint32_t Schema::getFieldId(const std::string &fieldName) {
  uint32_t fieldId = 0;
  for (auto &i : fields) {
    if (i.name == fieldName) break;
    fieldId += 1;
  }
  return fieldId;
}
}  // end of namespace NKV
