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
#include "profiler.h"
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
    PointProfiler _timer;
    _timer.start();
#endif
    bool status = _pbrb->read(vPtr.getTimestamp(), tsRead, vPtr.getPBRBAddr(),
                              schemaid, value, &vPtr);
#ifdef ENABLE_STATISTICS
    _timer.end();
    _durationStat.pbrbReadCount.fetch_add(1);
    _durationStat.pbrbReadTimeNanoSecs.fetch_add(_timer.duration());
#endif

  } else {
    NKV_LOG_D(std::cout, "Read value from PLOG");
    // Read PLog get a value

#ifdef ENABLE_STATISTICS
    PointProfiler _timer;
    _timer.start();
#endif
    _engine_ptr->read(vPtr.getPmemAddr(), value);
#ifdef ENABLE_STATISTICS
    _timer.end();
    _durationStat.pmemReadCount.fetch_add(1);
    _durationStat.pmemReadTimeNanoSecs.fetch_add(_timer.duration());
#endif
    if (_enable_pbrb == false) {
      return true;
    }
#ifdef ENABLE_STATISTICS
    _timer.start();
#endif

    TimeStamp tsInsert;
    tsInsert.getNow();
    _pbrb->schemaMiss(schemaid);
    bool status =
        _pbrb->write(vPtr.getTimestamp(), tsInsert, schemaid, value, idxIter);
#ifdef ENABLE_STATISTICS
    _timer.end();
    _durationStat.pbrbWriteCount.fetch_add(1);
    _durationStat.pbrbWriteTimeNanoSecs.fetch_add(_timer.duration());
#endif
  }
  return true;
}

bool NeoPMKV::get(Key &key, std::string &value) {
  if (checkSchemaIdValid(key.schemaId) == false) {
    return false;
  }
  auto indexer = _indexerList[key.schemaId];

#ifdef ENABLE_STATISTICS
  PointProfiler _timer;
  _timer.start();
#endif

  IndexerIterator idxIter = indexer->find(key.primaryKey);

#ifdef ENABLE_STATISTICS
  _timer.end();
  _durationStat.indexQueryCount.fetch_add(1);
  _durationStat.indexQueryTimeNanoSecs.fetch_add(_timer.duration());
#endif

  return getValueFromIndexIterator(idxIter, indexer, key.schemaId, value);
}

bool NeoPMKV::put(Key &key, const std::string &value) {
  if (checkSchemaIdValid(key.schemaId) == false) {
    return false;
  }
  auto indexer = _indexerList[key.schemaId];

  PmemAddress pmAddr;

#ifdef ENABLE_STATISTICS
  PointProfiler _timer;
  _timer.start();
#endif
  Status s = _engine_ptr->append(pmAddr, value.c_str(), value.size());

#ifdef ENABLE_STATISTICS
  _timer.end();
  _durationStat.pmemWriteCount.fetch_add(1);
  _durationStat.pmemWriteTimeNanoSecs.fetch_add(_timer.duration());
#endif

  if (!s.is2xxOK()) return false;
  ValuePtr vPtr;
  vPtr.updatePmemAddr(pmAddr);
  // try to insert
#ifdef ENABLE_STATISTICS
  _timer.start();
#endif

  auto [iter, status] = indexer->insert({key.primaryKey, vPtr});

#ifdef ENABLE_STATISTICS
  _timer.end();
  _durationStat.indexInsertCount.fetch_add(1);
  _durationStat.indexInsertTimeNanoSecs.fetch_add(_timer.duration());
#endif

  // status is true means insert success, we don't have the kv before
  if (status == true) return true;
  // status is false means having the old kv
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
#ifdef ENABLE_STATISTICS
  PointProfiler _timer;
  _timer.start();
#endif

  IndexerIterator idxIter = indexer->find(key.primaryKey);

#ifdef ENABLE_STATISTICS
  _timer.end();
  _durationStat.indexQueryCount.fetch_add(1);
  _durationStat.indexQueryTimeNanoSecs.fetch_add(_timer.duration());
#endif

  if (idxIter == indexer->end()) return false;
  ValuePtr &vPtr = idxIter->second;
  PmemAddress pmemAddr = vPtr.getPmemAddr();

#ifdef ENABLE_STATISTICS
  _timer.start();
#endif
  for (const auto &[fieldName, fieldContent] : values) {
    uint32_t fieldOffset = _sUMap.find(key.schemaId)->getPmemOffset(fieldName);
    _engine_ptr->write(pmemAddr + fieldOffset, fieldContent.c_str(),
                       fieldContent.size());
  }
#ifdef ENABLE_STATISTICS
  _timer.end();
  _durationStat.pmemUpdateCount.fetch_add(1);
  _durationStat.pmemUpdateTimeNanoSecs.fetch_add(_timer.duration());
#endif

  vPtr.updatePmemAddr(pmemAddr);
  return true;
}
bool NeoPMKV::update(Key &key,
                     std::vector<std::pair<uint32_t, std::string>> &values) {
  if (checkSchemaIdValid(key.schemaId) == false) {
    return false;
  }
  auto indexer = _indexerList[key.schemaId];

#ifdef ENABLE_STATISTICS
  PointProfiler _timer;
  _timer.start();
#endif

  IndexerIterator idxIter = indexer->find(key.primaryKey);

#ifdef ENABLE_STATISTICS
  _timer.end();
  _durationStat.indexQueryCount.fetch_add(1);
  _durationStat.indexQueryTimeNanoSecs.fetch_add(_timer.duration());
#endif

  if (idxIter == indexer->end()) return false;
  ValuePtr &vPtr = idxIter->second;
  PmemAddress pmemAddr = vPtr.getPmemAddr();

#ifdef ENABLE_STATISTICS
  _timer.start();
#endif
  for (const auto &[fieldId, fieldContent] : values) {
    uint32_t fieldOffset = _sUMap.find(key.schemaId)->getPmemOffset(fieldId);
    _engine_ptr->write(pmemAddr + fieldOffset, fieldContent.c_str(),
                       fieldContent.size());
  }

#ifdef ENABLE_STATISTICS
  _timer.end();
  _durationStat.pmemUpdateCount.fetch_add(1);
  _durationStat.pmemUpdateTimeNanoSecs.fetch_add(_timer.duration());
#endif

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

#ifdef ENABLE_STATISTICS
  PointProfiler _timer;
  _timer.start();
#endif

  auto iter = indexer->upper_bound(start.primaryKey);

#ifdef ENABLE_STATISTICS
  _timer.end();
  _durationStat.indexQueryCount.fetch_add(1);
  _durationStat.indexQueryTimeNanoSecs.fetch_add(_timer.duration());
#endif
  for (auto i = 0; i < scan_len && iter != indexer->end(); i++, iter++) {
    std::string tmp_value;
    getValueFromIndexIterator(iter, indexer, start.schemaId, tmp_value);
    value_list.push_back(tmp_value);
  }
  return true;
}
#ifdef ENABLE_STATISTICS
void NeoPMKV::outputReadStat() {
  NKV_LOG_I(std::cout, " Enable pbrb: {}, async pbrb: {}", _enable_pbrb,
            _async_pbrb);
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
}
#endif

}  // namespace NKV
