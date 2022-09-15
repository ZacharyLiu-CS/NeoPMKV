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
#include "logging.h"
#include "pbrb.h"
#include "schema.h"

namespace NKV {
bool NeoPMKV::getValueFromIndexIterator(IndexerIterator &idxIter,
                                        std::shared_ptr<IndexerT> indexer,
                                        SchemaId schemaid, Value &value) {
  if (idxIter == indexer->end()) {
    return false;
  }
  ValuePtr &vPtr = idxIter->second;

  if (vPtr.isHot()) {
    NKV_LOG_D(std::cout, "Read value from PBRB");
    _pbrb->schemaHit(schemaid);
    // Read PBRB
    TimeStamp tsRead;
    tsRead.getNow();
#ifdef ENABLE_STATISTICS
    _timer.start();
#endif
    bool status = _pbrb->read(vPtr.getTimestamp(), tsRead, vPtr.getPBRBAddr(),
                              schemaid, value, &vPtr);
#ifdef ENABLE_STATISTICS
    _timer.end();
    _durationStat.pbrbReadCount++;
    _durationStat.pbrbReadTimeSecs += _timer.duration();
#endif

  } else {
    NKV_LOG_D(std::cout, "Read value from PLOG");
    // Read PLog get a value

#ifdef ENABLE_STATISTICS
    _timer.start();
#endif
    _engine_ptr->read(vPtr.getPmemAddr(), value);
#ifdef ENABLE_STATISTICS
    _timer.end();
    _durationStat.pmemReadCount++;
    _durationStat.pmemReadTimeSecs += _timer.duration();
#endif
    if (_enable_pbrb == false) {
      return true;
    }
    TimeStamp tsInsert;
    tsInsert.getNow();
    _pbrb->schemaMiss(schemaid);
    if (_async_pbrb == false) {
      bool status = _pbrb->syncwrite(vPtr.getTimestamp(), tsInsert, schemaid,
                                     value, idxIter);
    }
  }
  return true;
}

bool NeoPMKV::get(Key &key, std::string &value) {
  if (checkSchemaIdValid(key.schemaId) == false) {
    return false;
  }
  auto indexer = _indexerList[key.schemaId];
  IndexerIterator idxIter = indexer->find(key.primaryKey);
  return getValueFromIndexIterator(idxIter, indexer, key.schemaId, value);
}

bool NeoPMKV::put(Key &key, const std::string &value) {
  if (checkSchemaIdValid(key.schemaId) == false) {
    return false;
  }
  auto indexer = _indexerList[key.schemaId];

  PmemAddress pmAddr;
  Status s = _engine_ptr->append(pmAddr, value.c_str(), value.size());
  if (!s.is2xxOK()) return false;
  ValuePtr vPtr;
  vPtr.updatePmemAddr(pmAddr);
  // try to insert
  auto [iter, status] = indexer->insert({key.primaryKey, vPtr});
  if (status == true) return true;
  if (_enable_pbrb == true && iter->second.isHot() == true) {
    _pbrb->dropRow(iter->second.getPBRBAddr());
  }
  iter->second.updatePmemAddr(pmAddr);
  return true;
}
bool NeoPMKV::update(Key &key,
                     std::vector<std::pair<std::string, std::string>> &values) {
  if (checkSchemaIdValid(key.schemaId) == false) {
    return false;
  }
  auto indexer = _indexerList[key.schemaId];
  IndexerIterator idxIter = indexer->find(key.primaryKey);
  if (idxIter == indexer->end()) return false;
  ValuePtr &vPtr = idxIter->second;
  PmemAddress pmemAddr = vPtr.getPmemAddr();

  for (const auto &[fieldName, fieldContent] : values) {
    uint32_t fieldOffset = _sUMap.find(key.schemaId)->getPmemOffset(fieldName);
    _engine_ptr->write(pmemAddr + fieldOffset, fieldContent.c_str(),
                       fieldContent.size());
  }
  vPtr.updatePmemAddr(pmemAddr);
  return true;
}
bool NeoPMKV::update(Key &key,
                     std::vector<std::pair<uint32_t, std::string>> &values) {
  if (checkSchemaIdValid(key.schemaId) == false) {
    return false;
  }
  auto indexer = _indexerList[key.schemaId];
  IndexerIterator idxIter = indexer->find(key.primaryKey);
  if (idxIter == indexer->end()) return false;
  ValuePtr &vPtr = idxIter->second;
  PmemAddress pmemAddr = vPtr.getPmemAddr();

  for (const auto &[fieldId, fieldContent] : values) {
    uint32_t fieldOffset = _sUMap.find(key.schemaId)->getPmemOffset(fieldId);
    _engine_ptr->write(pmemAddr + fieldOffset, fieldContent.c_str(),
                       fieldContent.size());
  }
  vPtr.updatePmemAddr(pmemAddr);
  return true;
}
bool NeoPMKV::remove(Key &key) {
  if (checkSchemaIdValid(key.schemaId) == false) {
    return false;
  }
  auto indexer = _indexerList[key.schemaId];

  IndexerIterator idxIter = indexer->find(key.primaryKey);
  if (idxIter == indexer->end()) {
    return false;
  }
  bool isHot = idxIter->second.isHot();
  if (isHot) {
    _pbrb->dropRow(idxIter->second.getPBRBAddr());
  }
  indexer->unsafe_erase(idxIter);
  return true;
}
bool NeoPMKV::scan(Key &start, std::vector<std::string> &value_list,
                   uint32_t scan_len) {
  if (checkSchemaIdValid(start.schemaId) == false) {
    return false;
  }
  auto indexer = _indexerList[start.schemaId];
  auto iter = indexer->upper_bound(start.primaryKey);
  for (auto i = 0; i < scan_len && iter != indexer->end(); i++, iter++) {
    std::string tmp_value;
    getValueFromIndexIterator(iter, indexer, start.schemaId, tmp_value);
    value_list.push_back(tmp_value);
  }
  return true;
}

}  // namespace NKV
