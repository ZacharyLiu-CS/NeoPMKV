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

  if (vPtr.isHot) {
    NKV_LOG_D(std::cout, "Read value from PBRB");
    // Read PBRB
    TimeStamp tsRead;
    tsRead.getNow();
    bool status = _pbrb->read(vPtr.timestamp, tsRead, vPtr.addr.pbrbAddr,
                              idxIter->first.schemaId, value, &vPtr);
  } else {
    NKV_LOG_D(std::cout, "Read value from PLOG");
    // Read PLog get a value
    _engine_ptr->read(vPtr.addr.pmemAddr, value);
    if (_enable_pbrb == false) {
      return true;
    }
    TimeStamp tsInsert;
    tsInsert.getNow();
    if (_async_pbrb == false) {
      bool status = _pbrb->syncwrite(vPtr.timestamp, tsInsert,
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
  if (_enable_pbrb == true && iter->second.isHot == true) {
    _pbrb->dropRow(iter->second.addr.pbrbAddr);
  }
  iter->second.updatePmemAddr(pmAddr);
  return true;
}

bool NeoPMKV::remove(Key &key) {
  IndexerIterator idxIter = _indexer.find(key);
  if (idxIter == _indexer.end()) {
    return false;
  }
  bool isHot = idxIter->second.isHot;
  if (isHot) {
    _pbrb->dropRow(idxIter->second.addr.pbrbAddr);
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
