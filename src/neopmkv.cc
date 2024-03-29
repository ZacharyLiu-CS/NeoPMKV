//
//  neopmkv.cc
//  PROJECT neopmkv
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "neopmkv.h"
#include <cstdint>
#include <mutex>
#include "buffer_page.h"
#include "field_type.h"
#include "kv_type.h"
#include "logging.h"
#include "pbrb.h"
#include "profiler.h"
#include "schema.h"
#include "schema_parser.h"
#include "timestamp.h"

namespace NKV {

SchemaId NeoPMKV::CreateSchema(vector<SchemaField> fields,
                               uint32_t primarykey_id, string name) {
  Schema newSchema = _schemaAllocator.CreateSchema(name, primarykey_id, fields);
  _sMap.addSchema(newSchema);
  _sParser.insert({newSchema.getSchemaId(), new SchemaParser(_memPoolPtr)});
  _indexerList.insert({newSchema.getSchemaId(), std::make_shared<IndexerT>()});
  if (_enable_pbrb == true) {
    _pbrb->createCacheForSchema(newSchema.getSchemaId());
  }
  return newSchema.getSchemaId();
}
// DML (data manipulation language)
Schema *NeoPMKV::QuerySchema(SchemaId sid) { return _sMap.find(sid); }

SchemaVer NeoPMKV::AddField(SchemaId sid, SchemaField &sField) {
  Schema *schemaPtr = _sMap.find(sid);
  bool s = schemaPtr->addField(sField);
  return schemaPtr->getVersion();
}
SchemaVer NeoPMKV::DropField(SchemaId sid, SchemaId fieldId) {
  Schema *schemaPtr = _sMap.find(sid);
  bool s = schemaPtr->dropField(fieldId);
  return schemaPtr->getVersion();
}

bool NeoPMKV::getValueHelper(IndexerIterator &idxIter,
                             shared_ptr<IndexerT> indexer, SchemaId schemaid,
                             Value &value, uint32_t fieldId) {
  if (idxIter == indexer->end()) {
    return false;
  }
  ValuePtr &vPtr = idxIter->second;
  auto [hotStatus, oldTS] = vPtr.getHotStatus();

  // read from pbrb
  if (hotStatus == true) {
    NKV_LOG_D(std::cout, "Read value from PBRB");
    _pbrb->schemaHit(schemaid);
    // Read PBRB
    TimeStamp newTS;
    newTS.getNow();

    POINT_PROFILE_START(_timer);

    bool status = _pbrb->read(oldTS, newTS, vPtr.getPBRBAddr(), schemaid, value,
                              &vPtr, fieldId);
    POINT_PROFILE_END(_timer);
    PROFILER_ATMOIC_ADD(_durationStat.pbrbReadCount, 1);
    PROFILER_ATMOIC_ADD(_durationStat.pbrbReadTimeNanoSecs, _timer.duration());
    if (status == true) {
      return true;
    }
  }
  // TODO: add the merge logic
  // Read PLog get a value
  Schema *schemaPtr = _sMap.find(schemaid);
  ValueReader valueReader(schemaPtr);
  POINT_PROFILE_START(pmem_timer);
  // read the full value
  if (fieldId == UINT32_MAX) {
    Value v;
    Status s = _engine_ptr->read(vPtr.getPmemAddr(), v);
    if (vPtr.getPrevItemCount() != 0) {
      vector<Value> allValues;
      while (valueReader.ExtractRowTypeFromRow(v.data()) ==
             RowType::PARTIAL_FIELD) {
        allValues.push_back(v);
        s = _engine_ptr->read(
            valueReader.ExtractPrevRowFromPartialRow(v.data()), v);
      }
      allValues.push_back(v);
      SchemaParser::MergePartialUpdateToFullRow(schemaPtr, value, allValues);
    } else {
      value.assign(v);
    }
    assert(s.is2xxOK());
  }
  // read the partial field
  if (fieldId != UINT32_MAX) {
    Status s = _engine_ptr->read(vPtr.getPmemAddr(), value, schemaPtr, fieldId);
    assert(s.is2xxOK());
  }
  POINT_PROFILE_END(pmem_timer);
  PROFILER_ATMOIC_ADD(_durationStat.pmemReadCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.pmemReadTimeNanoSecs,
                      pmem_timer.duration());
  // disable pbrb
  if (_enable_pbrb == false) {
    return true;
  }
  // only partial value
  if (fieldId != UINT32_MAX) {
    return true;
  }
  TimeStamp newTs;
  newTs.getNow();

  _pbrb->schemaMiss(schemaid);

  POINT_PROFILE_START(pbrb_timer);
  if (schemaPtr->hasVarField() == true) {
    std::string fixedValue = value;
    auto i = _sParser[schemaid]->ParseFromSeqToTwoPart(schemaPtr, fixedValue);
    bool status = _pbrb->write(oldTS, newTs, schemaid, fixedValue, idxIter);
  } else {
    bool status = _pbrb->write(oldTS, newTs, schemaid, value, idxIter);
  }

  POINT_PROFILE_END(pbrb_timer);
  PROFILER_ATMOIC_ADD(_durationStat.pbrbWriteCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.pbrbWriteTimeNanoSecs,
                      pbrb_timer.duration());
  return true;
}

bool NeoPMKV::getValueHelper(IndexerIterator &idxIter,
                             shared_ptr<IndexerT> indexer, SchemaId schemaid,
                             vector<Value> &values, vector<uint32_t> &fields) {
  if (idxIter == indexer->end()) {
    return false;
  }
  ValuePtr &vPtr = idxIter->second;
  auto [hotStatus, oldTS] = vPtr.getHotStatus();
  values.resize(fields.size());
  // read from pbrb
  if (hotStatus == true) {
    NKV_LOG_D(std::cout, "Read value from PBRB");
    _pbrb->schemaHit(schemaid);
    // Read PBRB
    TimeStamp newTS;
    newTS.getNow();

    POINT_PROFILE_START(_timer);

    bool status = _pbrb->read(oldTS, newTS, vPtr.getPBRBAddr(), schemaid,
                              values, &vPtr, fields);
    POINT_PROFILE_END(_timer);
    PROFILER_ATMOIC_ADD(_durationStat.pbrbReadCount, 1);
    PROFILER_ATMOIC_ADD(_durationStat.pbrbReadTimeNanoSecs, _timer.duration());

    return true;
  }
  Schema *schemaPtr = _sMap.find(schemaid);
  ValueReader valueReader(schemaPtr);
  // Read PLog get a value
  POINT_PROFILE_START(pmem_timer);
  Value allValue;
  Status s = _engine_ptr->read(vPtr.getPmemAddr(), allValue);
  if (vPtr.getPrevItemCount() != 0) {
    vector<Value> allValues;
    while (valueReader.ExtractRowTypeFromRow(allValue.data()) ==
           RowType::PARTIAL_FIELD) {
      allValues.push_back(allValue);
      s = _engine_ptr->read(
          valueReader.ExtractPrevRowFromPartialRow(allValue.data()), allValue);
    }
    allValues.push_back(allValue);
    SchemaParser::MergePartialUpdateToFullRow(schemaPtr, allValue, allValues);
  }
  POINT_PROFILE_END(pmem_timer);
  PROFILER_ATMOIC_ADD(_durationStat.pmemReadCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.pmemReadTimeNanoSecs,
                      pmem_timer.duration());
  for (uint32_t i = 0; i < fields.size(); i++)
    valueReader.ExtractFieldFromFullRow(allValue.data(), fields[i], values[i]);

  if (_enable_pbrb == false) {
    return true;
  }
  TimeStamp newTs;
  newTs.getNow();

  _pbrb->schemaMiss(schemaid);

  POINT_PROFILE_START(pbrb_timer);
  if (schemaPtr->hasVarField() == true) {
    _sParser[schemaid]->ParseFromSeqToTwoPart(schemaPtr, allValue);
    bool status = _pbrb->write(oldTS, newTs, schemaid, allValue, idxIter);
  } else {
    bool status = _pbrb->write(oldTS, newTs, schemaid, allValue, idxIter);
  }
  POINT_PROFILE_END(pbrb_timer);
  PROFILER_ATMOIC_ADD(_durationStat.pbrbWriteCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.pbrbWriteTimeNanoSecs,
                      pbrb_timer.duration());
  return true;
}
bool NeoPMKV::updateFullValue(IndexerIterator &idxIter,
                              shared_ptr<IndexerT> indexer, const Key &key,
                              Value &newPartialValue) {
  ValuePtr &vPtr = idxIter->second;
  auto [hotStatus, oldTS] = vPtr.getHotStatus();

  Schema *schemaPtr = _sMap.find(key.getSchemaId());
  Value newFullValue;
  vector<Value> oldFullValues(2);
  oldFullValues[0] = newPartialValue;

  // read from pbrb
  if (hotStatus == true) {
    NKV_LOG_D(std::cout, "Read value from PBRB");
    _pbrb->schemaHit(key.getSchemaId());
    // Read PBRB
    TimeStamp newTS;
    newTS.getNow();

    POINT_PROFILE_START(_timer);

    bool status = _pbrb->read(oldTS, newTS, vPtr.getPBRBAddr(),
                              key.getSchemaId(), oldFullValues.back(), &vPtr);
    POINT_PROFILE_END(_timer);
    PROFILER_ATMOIC_ADD(_durationStat.pbrbReadCount, 1);
    PROFILER_ATMOIC_ADD(_durationStat.pbrbReadTimeNanoSecs, _timer.duration());
    SchemaParser::MergePartialUpdateToFullRow(schemaPtr, newFullValue,
                                              oldFullValues);
    return putExistedValue(idxIter, &vPtr, key, newFullValue, false);
  }
  ValueReader valueReader(schemaPtr);
  // Read PLog get a value
  POINT_PROFILE_START(pmem_timer);
  Value allValue;
  Status s = _engine_ptr->read(vPtr.getPmemAddr(), allValue);
  if (vPtr.getPrevItemCount() != 0) {
    vector<Value> allValues;
    allValues.push_back(newPartialValue);
    while (valueReader.ExtractRowTypeFromRow(allValue.data()) ==
           RowType::PARTIAL_FIELD) {
      allValues.push_back(allValue);
      s = _engine_ptr->read(
          valueReader.ExtractPrevRowFromPartialRow(allValue.data()), allValue);
    }
    allValues.push_back(allValue);
    SchemaParser::MergePartialUpdateToFullRow(schemaPtr, allValue, allValues);
  }
  POINT_PROFILE_END(pmem_timer);
  PROFILER_ATMOIC_ADD(_durationStat.pmemReadCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.pmemReadTimeNanoSecs,
                      pmem_timer.duration());
  return putExistedValue(idxIter, &vPtr, key, allValue, false);
}

bool NeoPMKV::Get(Key &key, Value &value) {
  POINT_PROFILE_START(overall_timer);
  auto indexer = _indexerList[key.getSchemaId()];

  POINT_PROFILE_START(index_timer);

  IndexerIterator idxIter = indexer->find(key.primaryKey);

  POINT_PROFILE_END(index_timer);
  PROFILER_ATMOIC_ADD(_durationStat.indexQueryCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.indexQueryTimeNanoSecs,
                      index_timer.duration());

  // NKV_LOG_I(std::cout, "key: {} value: {} valuePtr: {}", key, value,
  //           idxIter->second);
  POINT_PROFILE_START(get_timer);
  bool status = getValueHelper(idxIter, indexer, key.getSchemaId(), value);

  POINT_PROFILE_END(get_timer);
  PROFILER_ATMOIC_ADD(_durationStat.GetValueFromIteratorCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.GetValueFromIteratorTimeNanoSecs,
                      get_timer.duration());

  POINT_PROFILE_END(overall_timer);
  PROFILER_ATMOIC_ADD(_durationStat.GetInterfaceCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.GetInterfaceTimeNanoSecs,
                      overall_timer.duration());
  return status;
}

bool NeoPMKV::PartialGet(Key &key, Value &value, uint32_t field) {
  POINT_PROFILE_START(overall_timer);
  auto indexer = _indexerList[key.getSchemaId()];

  POINT_PROFILE_START(index_timer);

  IndexerIterator idxIter = indexer->find(key.primaryKey);

  POINT_PROFILE_END(index_timer);
  PROFILER_ATMOIC_ADD(_durationStat.indexQueryCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.indexQueryTimeNanoSecs,
                      index_timer.duration());

  // NKV_LOG_I(std::cout, "key: {} value: {} valuePtr: {}", key, value,
  //           idxIter->second);
  POINT_PROFILE_START(get_timer);
  bool status =
      getValueHelper(idxIter, indexer, key.getSchemaId(), value, field);

  POINT_PROFILE_END(get_timer);
  PROFILER_ATMOIC_ADD(_durationStat.GetValueFromIteratorCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.GetValueFromIteratorTimeNanoSecs,
                      get_timer.duration());

  POINT_PROFILE_END(overall_timer);
  PROFILER_ATMOIC_ADD(_durationStat.GetInterfaceCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.GetInterfaceTimeNanoSecs,
                      overall_timer.duration());
  return status;
}

bool NeoPMKV::MultiPartialGet(Key &key, vector<string> &value,
                              vector<uint32_t> fields) {
  value.resize(fields.size());
  POINT_PROFILE_START(overall_timer);
  auto indexer = _indexerList[key.getSchemaId()];

  POINT_PROFILE_START(index_timer);

  IndexerIterator idxIter = indexer->find(key.primaryKey);

  POINT_PROFILE_END(index_timer);
  PROFILER_ATMOIC_ADD(_durationStat.indexQueryCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.indexQueryTimeNanoSecs,
                      index_timer.duration());

  // NKV_LOG_I(std::cout, "key: {} value: {} valuePtr: {}", key, value,
  //           idxIter->second);
  POINT_PROFILE_START(get_timer);
  bool status =
      getValueHelper(idxIter, indexer, key.getSchemaId(), value, fields);

  POINT_PROFILE_END(get_timer);
  PROFILER_ATMOIC_ADD(_durationStat.GetValueFromIteratorCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.GetValueFromIteratorTimeNanoSecs,
                      get_timer.duration());

  POINT_PROFILE_END(overall_timer);
  PROFILER_ATMOIC_ADD(_durationStat.GetInterfaceCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.GetInterfaceTimeNanoSecs,
                      overall_timer.duration());
  return status;
}
bool NeoPMKV::Put(const Key &key, Value &value) {
  Schema *schemaPtr = _sMap.find(key.getSchemaId());
  return putNewValue(key, value);
}
bool NeoPMKV::Put(const Key &key, vector<Value> &fieldList) {
  Schema *schemaPtr = _sMap.find(key.getSchemaId());
  std::string value = _sParser[key.getSchemaId()]->ParseFromUserWriteToSeq(
      schemaPtr, fieldList);
  return putNewValue(key, value);
}

bool NeoPMKV::putNewValue(const Key &key, const Value &value) {
  auto indexer = _indexerList[key.getSchemaId()];

  PmemAddress pmAddr;
  POINT_PROFILE_START(pmem_timer);
  Status s = _engine_ptr->append(pmAddr, value.c_str(), value.size());

  POINT_PROFILE_END(pmem_timer);
  PROFILER_ATMOIC_ADD(_durationStat.pmemWriteCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.pmemWriteTimeNanoSecs,
                      pmem_timer.duration());

  if (!s.is2xxOK()) return false;
  TimeStamp putTs;
  putTs.getNow();
  ValuePtr vPtr(pmAddr, putTs);

  // try to insert

  POINT_PROFILE_START(index_timer);
  auto [iter, status] = indexer->insert({key.primaryKey, vPtr});

  POINT_PROFILE_END(index_timer);
  PROFILER_ATMOIC_ADD(_durationStat.indexInsertCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.indexInsertTimeNanoSecs,
                      index_timer.duration());
  // NKV_LOG_I(std::cout, "key: {} value: {} valuePtr: {}", key, value, vPtr);
  // status is true means insert success, we don't have the kv before
  if (status == true) return true;
  // status is false means having the old kv
  if (_enable_pbrb == true && iter->second.isHot() == true) {
    _pbrb->dropRow(iter->second.getPBRBAddr(), _sMap.find(key.getSchemaId()));
  }
  iter->second.setFullColdPmemAddr(pmAddr, putTs);

  return true;
}

bool NeoPMKV::PartialUpdate(Key &key, Value &fieldValue, uint32_t fieldId) {
  Schema *schemaPtr = _sMap.find(key.getSchemaId());
  vector<Value> valueList = {fieldValue};
  vector<uint32_t> fieldList = {fieldId};
  auto indexer = _indexerList[key.getSchemaId()];

  IndexerIterator idxIter = indexer->find(key.primaryKey);
  if (idxIter == indexer->end()) {
    return false;
  }
  ValuePtr *vPtr = &idxIter->second;
  PmemAddress oldPmemAddr = vPtr->getPmemAddr();

  std::string pValue = _sParser[key.getSchemaId()]->ParseFromPartialUpdateToRow(
      schemaPtr, vPtr->getPmemAddr(), valueList, fieldList);
  if (vPtr->getPrevItemCount() <= 3) {
    auto s = putExistedValue(idxIter, vPtr, key, pValue, true);
    if (s == false) return s;
    if (_in_place_update_opt == false) return true;
    // now we can do the in-place-update optimization
    if (schemaPtr->getFieldType(fieldId) == FieldType::VARSTR) {
      return true;
    }
    auto iOffset = schemaPtr->getPmemOffset(fieldId);
    auto iSize = fieldValue.size();
    if (iSize > schemaPtr->getSize(fieldId))
      iSize = schemaPtr->getSize(fieldId);
    _engine_ptr->write(oldPmemAddr + iOffset, fieldValue.data(), iSize);
    vPtr->setFullColdPmemAddr(oldPmemAddr);
    return true;
  }

  return updateFullValue(idxIter, indexer, key, pValue);
}

bool NeoPMKV::MultiPartialUpdate(Key &key, vector<Value> &fieldValues,
                                 vector<uint32_t> &fields) {
  Schema *schemaPtr = _sMap.find(key.getSchemaId());
  auto indexer = _indexerList[key.getSchemaId()];

  IndexerIterator idxIter = indexer->find(key.primaryKey);
  if (idxIter == indexer->end()) {
    return false;
  }

  ValuePtr *vPtr = &idxIter->second;
  PmemAddress oldPmemAddr = vPtr->getPmemAddr();

  std::string pValue = _sParser[key.getSchemaId()]->ParseFromPartialUpdateToRow(
      schemaPtr, vPtr->getPmemAddr(), fieldValues, fields);
  if (vPtr->getPrevItemCount() <= 3) {
    bool s = putExistedValue(idxIter, vPtr, key, pValue, true);
    if (s == false) return s;
    if (_in_place_update_opt == false) return true;
    // now we can do the in-place-update optimization
    for (auto i : fields) {
      if (schemaPtr->getFieldType(i) == FieldType::VARSTR) {
        return true;
      }
    }
    for (uint32_t i = 0; i < fields.size(); i++) {
      auto iFieldId = fields[i];
      auto iOffset = schemaPtr->getPmemOffset(iFieldId);
      auto iSize = fieldValues[i].size();
      if (iSize > schemaPtr->getSize(iFieldId))
        iSize = schemaPtr->getSize(iFieldId);
      _engine_ptr->write(oldPmemAddr + iOffset, fieldValues[i].data(), iSize);
    }
    vPtr->setFullColdPmemAddr(oldPmemAddr);
    return true;
  }
  return updateFullValue(idxIter, indexer, key, pValue);
}

bool NeoPMKV::putExistedValue(IndexerIterator &idxIter, ValuePtr *vPtr,
                              const Key &key, const Value &value,
                              bool isPartial) {
  PmemAddress pmAddr;
  POINT_PROFILE_START(pmem_timer);
  Status s = _engine_ptr->append(pmAddr, value.c_str(), value.size());

  POINT_PROFILE_END(pmem_timer);
  PROFILER_ATMOIC_ADD(_durationStat.pmemUpdateCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.pmemUpdateTimeNanoSecs,
                      pmem_timer.duration());

  if (!s.is2xxOK()) return false;
  TimeStamp putTs;
  putTs.getNow();

  // NKV_LOG_I(std::cout, "key: {} value: {} valuePtr: {}", key, value, vPtr);
  // status is true means insert success, we don't have the kv before
  // status is false means having the old kv
  if (_enable_pbrb == true && idxIter->second.isHot() == true) {
    _pbrb->dropRow(idxIter->second.getPBRBAddr(),
                   _sMap.find(key.getSchemaId()));
  }
  if (isPartial == true) {
    vPtr->setPartialColdPmemAddr(pmAddr, putTs);
  } else {
    vPtr->setFullColdPmemAddr(pmAddr, putTs);
  }

  return true;
}
bool NeoPMKV::Remove(Key &key) {
  auto indexer = _indexerList[key.getSchemaId()];

  IndexerIterator idxIter = indexer->find(key.primaryKey);
  if (idxIter == indexer->end()) {
    return false;
  }
  bool isHot = idxIter->second.isHot();
  if (isHot) {
    _pbrb->dropRow(idxIter->second.getPBRBAddr(),
                   _sMap.find(key.getSchemaId()));
  }
  indexer->unsafe_erase(idxIter);
  return true;
}

bool NeoPMKV::Scan(Key &start, vector<Value> &value_list, uint32_t scan_len) {
  auto indexer = _indexerList[start.getSchemaId()];

  POINT_PROFILE_START(index_timer);

  auto iter = indexer->upper_bound(start.primaryKey);

  POINT_PROFILE_END(index_timer);

  PROFILER_ATMOIC_ADD(_durationStat.indexQueryCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.indexQueryTimeNanoSecs,
                      index_timer.duration());

  POINT_PROFILE_START(get_value);

  for (auto i = 0; i < scan_len && iter != indexer->end(); i++, iter++) {
    string tmp_value;
    getValueHelper(iter, indexer, start.getSchemaId(), tmp_value);
    value_list.push_back(tmp_value);
  }
  POINT_PROFILE_END(get_value);
  PROFILER_ATMOIC_ADD(_durationStat.GetValueFromIteratorCount, scan_len);
  PROFILER_ATMOIC_ADD(_durationStat.GetValueFromIteratorTimeNanoSecs,
                      get_value.duration());
  return true;
}

bool NeoPMKV::PartialScan(Key &start, vector<Value> &value_list,
                          uint32_t scan_len, uint32_t field) {
  auto indexer = _indexerList[start.getSchemaId()];

  POINT_PROFILE_START(index_timer);

  auto iter = indexer->upper_bound(start.primaryKey);

  POINT_PROFILE_END(index_timer);

  PROFILER_ATMOIC_ADD(_durationStat.indexQueryCount, 1);
  PROFILER_ATMOIC_ADD(_durationStat.indexQueryTimeNanoSecs,
                      index_timer.duration());

  POINT_PROFILE_START(get_value);

  for (auto i = 0; i < scan_len && iter != indexer->end(); i++, iter++) {
    string tmp_value;
    getValueHelper(iter, indexer, start.getSchemaId(), tmp_value, field);
    value_list.push_back(tmp_value);
  }
  POINT_PROFILE_END(get_value);
  PROFILER_ATMOIC_ADD(_durationStat.GetValueFromIteratorCount, scan_len);
  PROFILER_ATMOIC_ADD(_durationStat.GetValueFromIteratorTimeNanoSecs,
                      get_value.duration());
  return true;
}

void NeoPMKV::outputReadStat() {
#ifdef ENABLE_STATISTICS
  NKV_LOG_I(std::cout, " Enable pbrb: {}, async pbrb: {}", _enable_pbrb,
            _async_pbrb);
  NKV_LOG_I(
      std::cout,
      "Get Interface Count: {}, Total Time Cost: {:.2f} "
      "s, Average Time "
      "Cost: {:.2f} ns",
      _durationStat.GetInterfaceCount.load(),
      _durationStat.GetInterfaceTimeNanoSecs.load() / (double)NANOSEC_BASE,
      _durationStat.GetInterfaceTimeNanoSecs.load() /
          (double)_durationStat.GetInterfaceCount.load());
  NKV_LOG_I(std::cout,
            "GetValue from Index Iterator Count: {}, Total Time Cost: {:.2f} "
            "s, Average Time "
            "Cost: {:.2f} ns",
            _durationStat.GetValueFromIteratorCount.load(),
            _durationStat.GetValueFromIteratorTimeNanoSecs.load() /
                (double)NANOSEC_BASE,
            _durationStat.GetValueFromIteratorTimeNanoSecs.load() /
                (double)_durationStat.GetValueFromIteratorCount.load());
  NKV_LOG_I(std::cout,
            "Index: Query Count: {}, Total Time Cost: {:.2f} s, Average Time "
            "Cost: {:.2f} ns",
            _durationStat.indexQueryCount.load(),
            _durationStat.indexQueryTimeNanoSecs.load() / (double)NANOSEC_BASE,
            _durationStat.indexQueryTimeNanoSecs.load() /
                (double)_durationStat.indexQueryCount.load());
  NKV_LOG_I(std::cout,
            "Index: insert Count: {}, Total Time Cost: {:.2f} s, Average Time "
            "Cost: {:.2f} ns",
            _durationStat.indexInsertCount.load(),
            _durationStat.indexInsertTimeNanoSecs.load() / (double)NANOSEC_BASE,
            _durationStat.indexInsertTimeNanoSecs.load() /
                (double)_durationStat.indexInsertCount.load());
  NKV_LOG_I(std::cout,
            "PMem: Read Count: {}, Total Time Cost: {:.2f} s, Average Time "
            "Cost: {:.2f} ns",
            _durationStat.pmemReadCount.load(),
            _durationStat.pmemReadTimeNanoSecs.load() / (double)NANOSEC_BASE,
            _durationStat.pmemReadTimeNanoSecs.load() /
                (double)_durationStat.pmemReadCount.load());
  NKV_LOG_I(std::cout,
            "PMem: Write Count: {}, Total Time Cost: {:.2f} s, Average Time "
            "Cost: {:.2f} ns",
            _durationStat.pmemWriteCount.load(),
            _durationStat.pmemWriteTimeNanoSecs.load() / (double)NANOSEC_BASE,
            _durationStat.pmemWriteTimeNanoSecs.load() /
                (double)_durationStat.pmemWriteCount.load());
  NKV_LOG_I(std::cout,
            "PMem: Update Count: {}, Total Time Cost: {:.2f} s, Average Time "
            "Cost: {:.2f} ns",
            _durationStat.pmemUpdateCount.load(),
            _durationStat.pmemUpdateTimeNanoSecs.load() / (double)NANOSEC_BASE,
            _durationStat.pmemUpdateTimeNanoSecs.load() /
                (double)_durationStat.pmemUpdateCount.load());
  NKV_LOG_I(std::cout,
            "PBRB: Read Count: {}, Total Time Cost: {:.2f} s, Average Time "
            "Cost: {:.2f} ns",
            _durationStat.pbrbReadCount.load(),
            _durationStat.pbrbReadTimeNanoSecs.load() / (double)NANOSEC_BASE,
            _durationStat.pbrbReadTimeNanoSecs.load() /
                (double)_durationStat.pbrbReadCount.load());
  NKV_LOG_I(std::cout,
            "PBRB: Write Count: {}, Total Time Cost: {:.2f} s, Average Time "
            "Cost: {:.2f} ns",
            _durationStat.pbrbWriteCount.load(),
            _durationStat.pbrbWriteTimeNanoSecs.load() / (double)NANOSEC_BASE,
            _durationStat.pbrbWriteTimeNanoSecs.load() /
                (double)_durationStat.pbrbWriteCount.load());
#endif
}

}  // namespace NKV
