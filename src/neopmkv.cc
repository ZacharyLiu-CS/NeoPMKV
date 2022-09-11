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
                                        Value &value) {
  if (idxIter == _indexer.end()) {
    return false;
  }
  ValuePtr &vPtr = idxIter->second;

  if (vPtr.isHot()) {
    NKV_LOG_D(std::cout, "Read value from PBRB");
    _pbrb->schemaHit(idxIter->first.schemaId);
    // Read PBRB
    TimeStamp tsRead;
    tsRead.getNow();
#ifdef ENABLE_STATISTICS
    _timer.start();
#endif
    bool status = _pbrb->read(vPtr.getTimestamp(), tsRead, vPtr.getPBRBAddr(),
                              idxIter->first.schemaId, value, &vPtr);
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
    _pbrb->schemaMiss(idxIter->first.schemaId);
    if (_async_pbrb == false) {
      bool status = _pbrb->syncwrite(vPtr.getTimestamp(), tsInsert,
                                     idxIter->first.schemaId, value, idxIter);
    }
  }
  return true;
}
bool NeoPMKV::get(Key &key, std::string &value) {
  IndexerIterator idxIter = _indexer.find(key);
  return getValueFromIndexIterator(idxIter, value);
}
bool NeoPMKV::put(Key &key, const std::string &value) {
  PmemAddress pmAddr;
  Status s = _engine_ptr->append(pmAddr, value.c_str(), value.size());
  if (!s.is2xxOK()) return false;
  ValuePtr vPtr;
  vPtr.updatePmemAddr(pmAddr);
  // try to insert
  auto [iter, status] = _indexer.insert({key, vPtr});
  if (status == true) return true;
  if (_enable_pbrb == true && iter->second.isHot() == true) {
    _pbrb->dropRow(iter->second.getPBRBAddr());
  }
  iter->second.updatePmemAddr(pmAddr);
  return true;
}

bool NeoPMKV::remove(Key &key) {
  IndexerIterator idxIter = _indexer.find(key);
  if (idxIter == _indexer.end()) {
    return false;
  }
  bool isHot = idxIter->second.isHot();
  if (isHot) {
    _pbrb->dropRow(idxIter->second.getPBRBAddr());
  }
  _indexer.erase(idxIter);
  return true;
}
bool NeoPMKV::scan(Key &start, std::vector<std::string> &value_list,
                   uint32_t scan_len) {
  auto iter = _indexer.upper_bound(start);
  for (auto i = 0; i < scan_len && iter != _indexer.end(); i++, iter++) {
    std::string tmp_value;
    getValueFromIndexIterator(iter, tmp_value);
    value_list.push_back(tmp_value);
  }
  return true;
}

}  // namespace NKV
