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
#include <utility>
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
      latestVersion(0),
      schemaId(schemaId),
      primaryKeyField(primaryKeyField),
      fields(fields) {
  size += ROW_META_HEAD_SIZE;
  for (auto &field : fields) {
    bool isVariableField = field.type == FieldType::VARSTR;
    FieldMetaData fMeta = FieldMetaData{.fieldSize = field.size,
                                        .fieldOffset = this->size,
                                        .isDeleted = false,
                                        .isNullable = false,
                                        .isVariable = isVariableField};
    if (isVariableField == true) {
      hasVariableField = true;
    }
    this->fieldsMeta.push_back(fMeta);
    this->size += field.size;
    allFieldSize += field.size;
    if (field.type == FieldType::VARSTR) {
      hasVariableField = true;
    }
  }
  uint32_t fieldsCount = fields.size();
  for (uint32_t sid = 0; sid < fieldsCount; sid++) {
    nameMap.insert({fields[sid].name, sid});
  }
}

Schema::Schema(const Schema &obj) {
  name = obj.name;
  latestVersion = obj.latestVersion;
  schemaId = obj.schemaId;
  primaryKeyField = obj.primaryKeyField;
  size = obj.size;
  allFieldSize = obj.allFieldSize;
  hasVariableField = obj.hasVariableField;
  fields = obj.fields;
  fieldsMeta = obj.fieldsMeta;
  nameMap = obj.nameMap;
  deletedField = obj.deletedField;
  addedField = obj.addedField;
}

bool Schema::addFieldImpl(SchemaField &&field) {
  std::lock_guard<std::mutex> _addField(schemaMutex);
  fields.push_back(field);
  bool isVariableField = field.type == FieldType::VARSTR;
  FieldMetaData fMeta = FieldMetaData{.fieldSize = field.size,
                                      .fieldOffset = this->size,
                                      .isDeleted = false,
                                      .isNullable = false,
                                      .isVariable = isVariableField};
  if (isVariableField == true) {
    hasVariableField = true;
  }
  fieldsMeta.push_back(fMeta);
  return true;
}

bool Schema::dropFieldImpl(SchemaId sid) {
  std::lock_guard<std::mutex> _dropField(schemaMutex);
  deletedField.insert(sid);
  fieldsMeta[sid].isDeleted = true;
  return true;
}
bool Schema::addField(const SchemaField &field) { return true; }

bool Schema::dropField(SchemaId sid) { return true; }
uint32_t Schema::getFieldId(const std::string &fieldName) {
  return nameMap[fieldName];
}
}  // end of namespace NKV
