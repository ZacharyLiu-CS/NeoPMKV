//
//  schema_parser.cc
//
//  Created by zhenliu on 10/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "schema_parser.h"
#include <cstring>
#include "field_type.h"
#include "kv_type.h"
#include "mempool.h"
#include "pmem_engine.h"
#include "profiler.h"
#include "schema.h"
#include "schema_parser.h"

namespace NKV {

std::string SchemaParser::ParseFromUserWriteToSeq(
    Schema *schemaPtr, std::vector<Value> &fieldValues) {
  std::string result;
  uint32_t contentSize = schemaPtr->getAllFieldSize();  // record the row size
  uint32_t seqRowSize = schemaPtr->getSize();
  uint32_t userWriteCount = fieldValues.size();
  for (uint32_t i = 0; i < userWriteCount; i++) {
    // add the fixed part
    if (schemaPtr->fieldsMeta[i].isVariable == false) {
      continue;
    }
    uint32_t varFieldSize = fieldValues[i].size();
    if (varFieldSize > sizeof(VarFieldContent::contentData)) {
      seqRowSize += varFieldSize;
      contentSize += varFieldSize;
    }
    // add the variable part
  }
  result.resize(seqRowSize);

  // do the movement
  char *destPtr = result.data();
  RowMetaPtr(destPtr)->setMeta(contentSize, RowType::FULL_DATA,
                               schemaPtr->schemaId, schemaPtr->version);

  destPtr = skipRowMeta(destPtr);
  char *startPtr = destPtr;
  uint32_t total_size = 0;
  char *varDestPtr = startPtr + schemaPtr->getAllFieldSize();

  for (uint32_t i = 0; i < userWriteCount; i++) {
    uint32_t fieldSize = schemaPtr->fieldsMeta[i].fieldSize;
    const char *src_ptr = fieldValues[i].c_str();
    // not variable field
    if (schemaPtr->fieldsMeta[i].isVariable == false) {
      uint32_t fSize = fieldValues[i].size();
      fSize = fSize > fieldSize ? fieldSize : fSize;
      memcpy(destPtr, src_ptr, fSize);
      destPtr += fieldSize;
      continue;
    }
    // is variable field, only need to construct the fiexed part
    if (fieldValues[i].size() <= 8) {
      EncodeToVarFieldFullData(destPtr, fieldValues[i].data(),
                               fieldValues[i].size());
      destPtr += schemaPtr->fieldsMeta[i].fieldSize;
    }
    // is variable field whose size is > 8, need to put them to row tail
    if (fieldValues[i].size() > 8) {
      memcpy(varDestPtr, fieldValues[i].data(), fieldValues[i].size());
      EncodeToVarFieldOffset(destPtr, varDestPtr - destPtr,
                             fieldValues[i].size());
      varDestPtr += fieldValues[i].size();
      destPtr += schemaPtr->fieldsMeta[i].fieldSize;
    }
  }
  return result;
}
string SchemaParser::ParseFromPartialUpdateToRow(Schema *schemaPtr,
                                                 PmemAddress pmemAddr,
                                                 vector<Value> &fieldValues,
                                                 vector<uint32_t> &fields) {
  std::string value;
  uint32_t fieldsCount = fieldValues.size();
  // if the fields is all the shema has
  if (fieldsCount == schemaPtr->fields.size())
    return ParseFromUserWriteToSeq(schemaPtr, fieldValues);

  uint32_t fieldsSize = 0;
  uint32_t varContentSize = 0;
  for (uint32_t i = 0; i < fieldsCount; i++) {
    uint32_t fid = fields[i];
    fieldsSize += schemaPtr->getSize(fid);
    if (schemaPtr->fieldsMeta[fid].isVariable) {
      uint32_t fieldContentSize = fieldValues[i].size();
      varContentSize += fieldContentSize > 8 ? fieldContentSize : 0;
    }
  }
  uint32_t partialMetaSize = PartialRowMeta::CalculateSize(fieldsCount);
  value.resize(ROW_META_HEAD_SIZE + partialMetaSize + fieldsSize +
               varContentSize);
  RowMetaPtr(value.data())
      ->setMeta(partialMetaSize + fieldsSize + varContentSize,
                RowType::PARTIAL_FIELD, schemaPtr->schemaId,
                schemaPtr->version);

  char *pMetaPtr = skipRowMeta(value.data());
  PartialRowMetaPtr(pMetaPtr)->setMeta(pmemAddr, partialMetaSize, fields);
  char *fieldPtr = skipPartialRowMeta(pMetaPtr);
  char *varContentPtr = fieldPtr + fieldsSize;
  for (uint32_t i = 0; i < fieldsCount; i++) {
    uint32_t fid = fields[i];
    uint32_t fieldSize = schemaPtr->fieldsMeta[fid].fieldSize;
    if (schemaPtr->fieldsMeta[fid].isVariable == false) {
      uint32_t copySize = fieldValues[i].size();
      copySize = copySize > fieldSize ? fieldSize : copySize;
      memcpy(fieldPtr, fieldValues[i].c_str(), copySize);
      fieldPtr += fieldSize;
      continue;
    }
    // the var content part
    if (fieldValues[i].size() <= 8) {
      EncodeToVarFieldFullData(fieldPtr, fieldValues[i].data(),
                               fieldValues[i].size());
      fieldPtr += fieldSize;
    }
    // is variable field whose size is > 8, need to put them to row tail
    if (fieldValues[i].size() > 8) {
      memcpy(varContentPtr, fieldValues[i].data(), fieldValues[i].size());
      EncodeToVarFieldOffset(fieldPtr, varContentPtr - fieldPtr,
                             fieldValues[i].size());
      varContentPtr += fieldValues[i].size();
      fieldPtr += fieldSize;
    }
  }

  return value;
}

bool SchemaParser::ParseFromSeqToTwoPart(Schema *schemaPtr,
                                         std::string &seqValue,
                                         bool loadVarPartToCache) {
  if (RowMetaPtr(seqValue.data())->getType() != RowType::FULL_DATA)
    return false;

  uint32_t rowFixedPartSize = schemaPtr->getSize();
  uint32_t rowVarPartSize = seqValue.size() - rowFixedPartSize;
  RowMetaPtr(seqValue.data())
      ->setMeta(schemaPtr->getAllFieldSize() + rowVarPartSize,
                RowType::FULL_FIELD, schemaPtr->schemaId, schemaPtr->version);

  char *fieldPtr = skipRowMeta(seqValue.data());
  if (schemaPtr->hasVariableField == false) {
    seqValue.resize(rowFixedPartSize);
    return true;
  }
  if (loadVarPartToCache == false) {
    for (auto &i : schemaPtr->fieldsMeta) {
      if (i.isVariable == true) EncodeToVarFieldNotCache(fieldPtr);
      fieldPtr += i.fieldSize;
    }
    return true;
  }

  // copy the varbaile field content
  bool noSpace = false;
  char *rowVarPartPtr = seqValue.data() + rowFixedPartSize;
  for (auto &i : schemaPtr->fieldsMeta) {
    // not variable field, do nothing
    if (i.isVariable == false) {
      fieldPtr += i.fieldSize;
      continue;
    }
    // meet variable field
    uint32_t varFieldSize = GetVarFieldSize(fieldPtr);
    // data is in the field, no need to move
    if (varFieldSize <= 8) {
      fieldPtr += i.fieldSize;
      continue;
    }
    // there is no space for movement
    if (noSpace == true) {
      EncodeToVarFieldNotCache(fieldPtr);
      fieldPtr += i.fieldOffset;
      continue;
    }

    // let's start to move
    try {
      void *varSpacePtr = _globalPool->Malloc(varFieldSize);
      memcpy(varSpacePtr, rowVarPartPtr, varFieldSize);
      EncodeToVarFieldOnlyPointer(fieldPtr, varSpacePtr, varFieldSize);
    } catch (std::bad_alloc &ba) {
      noSpace = true;
      EncodeToVarFieldNotCache(fieldPtr);
    }
    fieldPtr += i.fieldSize;
  }
  seqValue.resize(rowFixedPartSize);
  if (noSpace == true) return false;
  return true;
}

bool SchemaParser::FreeTwoPartRow(Schema *schemaPtr, char *value) {
  if (RowMetaPtr(value)->getType() != RowType::FULL_FIELD) return false;

  if (schemaPtr->hasVariableField == false) return true;
  uint32_t rowFixedPartSize = schemaPtr->getSize();
  char *fieldPtr = skipRowMeta(value);
  for (auto &i : schemaPtr->fieldsMeta) {
    if (i.isVariable == true) {
      VarFieldContent *varFieldPtr = VarFieldPtr(fieldPtr);
      if (varFieldPtr->contentType == VarFieldType::ONLY_POINTER) {
        _globalPool->Free(varFieldPtr->contentPtr);
      }
    }
    fieldPtr += i.fieldSize;
  }
  return true;
}
bool SchemaParser::ParseFromTwoPartToSeq(Schema *schemaPtr, string &newValue,
                                         char *oldPtr) {
  if (oldPtr == nullptr) return false;
  if (RowMetaPtr(oldPtr)->getType() == RowType::PARTIAL_FIELD) return false;
  uint32_t rowFixedPartSize = schemaPtr->getSize();

  uint32_t rowVarPartSize =
      RowMetaPtr(oldPtr)->getSize() - schemaPtr->getAllFieldSize();

  newValue.resize(rowFixedPartSize + rowVarPartSize);
  // copy the fixed size part
  memcpy(newValue.data(), oldPtr, rowFixedPartSize);

  if (rowVarPartSize == 0) {
    return true;
  }
  char *rowVarPart = newValue.data() + rowFixedPartSize;

  char *newFieldPtr = skipRowMeta(newValue.data());

  for (auto &i : schemaPtr->fieldsMeta) {
    if (i.isVariable == false) {
      newFieldPtr += i.fieldSize;
      continue;
    }
    VarFieldContent *varFieldPtr = VarFieldPtr(newFieldPtr);
    if (varFieldPtr->contentType == VarFieldType::ONLY_POINTER) {
      // copy the variable field into the row
      memcpy(rowVarPart, varFieldPtr->contentPtr, varFieldPtr->contentSize);
      varFieldPtr->contentOffset = rowVarPart - newFieldPtr;
      varFieldPtr->contentType = VarFieldType::ROW_OFFSET;
      rowVarPart += varFieldPtr->contentSize;
    }
    if (varFieldPtr->contentType == VarFieldType::NOT_CACHE) {
      return false;
    }
    newFieldPtr += i.fieldSize;
  }

  return true;
}

bool ValueReader::ExtractFieldFromPmemRow(PmemAddress rowPtr,
                                          PmemEngine *enginePtr,
                                          uint32_t fieldId, Value &value) {
  const Status s = enginePtr->read(rowPtr, value, _schemaPtr, fieldId);
  return s.is2xxOK();
}

bool ValueReader::ExtractFieldFromFullRow(char *rowPtr, uint32_t fieldId,
                                          Value &value) {
  // the full_field and full_data type can do this
  if (RowMetaPtr(rowPtr)->getType() == RowType::PARTIAL_FIELD) return false;
  // decide if it is variable
  bool isVariable = _schemaPtr->fieldsMeta[fieldId].isVariable;
  if (isVariable == false) {
    uint32_t fieldSize = _schemaPtr->getSize(fieldId);
    value.resize(fieldSize);
    memcpy(value.data(), rowPtr + _schemaPtr->getPBRBOffset(fieldId),
           fieldSize);
    return true;
  }
  // variable field and is smaller than 8B
  VarFieldContent *varFieldPtr =
      VarFieldPtr(rowPtr + _schemaPtr->getPBRBOffset(fieldId));
  uint32_t fieldSize = varFieldPtr->contentSize;
  VarFieldType fieldType = varFieldPtr->contentType;

  if (fieldType == VarFieldType::FULL_DATA) {
    value.resize(fieldSize);
    memcpy(value.data(), varFieldPtr->contentData, fieldSize);
    return true;
  }
  // store the value in seq row
  if (fieldType == VarFieldType::ROW_OFFSET) {
    value.resize(fieldSize, 0);
    memcpy(value.data(), (char *)varFieldPtr + varFieldPtr->contentOffset,
           fieldSize);
    return true;
  }
  // store the value in the heap space
  if (fieldType == VarFieldType::ONLY_POINTER) {
    value.resize(fieldSize);
    memcpy(value.data(), varFieldPtr->contentPtr, fieldSize);
    return true;
  }
  if (fieldType == VarFieldType::NULL_T) {
    return true;
  }
  // NOT in Cache
  return false;
}

bool ValueReader::ExtractFieldFromPartialRow(char *rowPtr, uint32_t fieldId,
                                             Value &value) {
  if (RowMetaPtr(rowPtr)->getType() != RowType::PARTIAL_FIELD) return false;
  PartialRowMeta *pMetaPtr = PartialRowMetaPtr(skipRowMeta(rowPtr));
  ParitalSchema pSchema(_schemaPtr, pMetaPtr->fieldArr, pMetaPtr->fieldCount);
  if (pSchema.checkExisted(fieldId) == false) {
    return false;
  }
  char *fieldPtr =
      skipPartialRowMeta((char *)pMetaPtr) + pSchema.getOffset(fieldId);
  // not variable field
  if (_schemaPtr->fieldsMeta[fieldId].isVariable == false) {
    uint32_t fSize = _schemaPtr->getSize(fieldId);
    value.resize(fSize);
    memcpy(value.data(), fieldPtr, fSize);
    return true;
  }

  // is variable field, must be OFFSET type
  VarFieldContent *vPtr = VarFieldPtr(fieldPtr);
  if (vPtr->contentType == VarFieldType::FULL_DATA) {
    uint32_t fSize = vPtr->contentSize;
    value.resize(fSize, 0);
    memcpy(value.data(), vPtr->contentPtr, fSize);
    return true;
  }
  if (vPtr->contentType == VarFieldType::ROW_OFFSET) {
    uint32_t fSize = vPtr->contentSize;
    value.resize(fSize, 0);
    memcpy(value.data(), (char *)vPtr + vPtr->contentOffset, fSize);
    return true;
  }

  return false;
}

bool ValueReader::ExtractMultiFieldFromPartialRow(char *rowPtr,
                                                  vector<uint32_t> &fieldsId,
                                                  vector<Value> &values) {
  if (RowMetaPtr(rowPtr)->getType() != RowType::PARTIAL_FIELD) return false;
  PartialRowMeta *pMetaPtr = PartialRowMetaPtr(skipRowMeta(rowPtr));

  ParitalSchema pSchema(_schemaPtr, pMetaPtr->fieldArr, pMetaPtr->fieldCount);
  values.resize(fieldsId.size());
  // iterate to extract the value
  for (uint32_t i = 0; i < fieldsId.size(); i++) {
    uint32_t fieldId = fieldsId[i];
    if (pSchema.checkExisted(fieldId) == false) {
      continue;
    }
    auto &value = values[i];
    char *fieldPtr =
        skipPartialRowMeta((char *)pMetaPtr + pSchema.getOffset(fieldId));
    // not variable field
    if (_schemaPtr->fieldsMeta[fieldId].isVariable == false) {
      uint32_t fSize = _schemaPtr->getSize(fieldId);
      value.resize(fSize);
      memcpy(value.data(), fieldPtr, fSize);
      continue;
    }

    // is variable field, must be OFFSET type
    VarFieldContent *vPtr = VarFieldPtr(fieldPtr);
    if (vPtr->contentType == VarFieldType::FULL_DATA) {
      uint32_t fSize = vPtr->contentSize;
      value.resize(fSize, 0);
      memcpy(value.data(), vPtr->contentPtr, fSize);
      continue;
    }
    if (vPtr->contentType == VarFieldType::ROW_OFFSET) {
      uint32_t fSize = vPtr->contentSize;
      value.resize(fSize, 0);
      memcpy(value.data(), (char *)vPtr->contentPtr + vPtr->contentOffset,
             fSize);
      continue;
    }
  }

  return true;
}
}  // namespace NKV
