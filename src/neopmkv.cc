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
#include "timestamp.h"

namespace NKV {
bool NeoPMKV::getValueFromIndexIterator(IndexerIterator &idxIter,
                                        std::shared_ptr<IndexerT> indexer,
                                        SchemaId schemaid, Value &value,
                                        std::vector<uint32_t> &fields) {
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
#ifdef ENABLE_STATISTICS
    PointProfiler _timer;
    _timer.start();
#endif
    bool status = _pbrb->read(oldTS, newTS, vPtr.getPBRBAddr(), schemaid, value,
                              &vPtr, fields);
#ifdef ENABLE_STATISTICS
    _timer.end();
    _durationStat.pbrbReadCount.fetch_add(1);
    _durationStat.pbrbReadTimeNanoSecs.fetch_add(_timer.duration());
#endif

    return true;
  }

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
    if (fields.size() != 0) {
      auto schema = _sUMap.find(schemaid);
      auto field_offset = schema->getPBRBOffset(fields[0]);
      auto field_size = schema->getSize(fields[0]) + FieldHeadSize;
      value = value.substr(field_offset, field_size);
    }
    return true;
  }
  TimeStamp newTs;
  newTs.getNow();
  _pbrb->schemaMiss(schemaid);

#ifdef ENABLE_STATISTICS
  _timer.start();
#endif
  bool status = _pbrb->write(oldTS, newTs, schemaid, value, idxIter);
#ifdef ENABLE_STATISTICS
  _timer.end();
  _durationStat.pbrbWriteCount.fetch_add(1);
  _durationStat.pbrbWriteTimeNanoSecs.fetch_add(_timer.duration());
#endif
  if (fields.size() != 0) {
    auto schema = _sUMap.find(schemaid);
    auto field_offset = schema->getPBRBOffset(fields[0]);
    auto field_size = schema->getSize(fields[0]) + FieldHeadSize;
    value = value.substr(field_offset, field_size);
  }
  return true;
}

bool NeoPMKV::get(Key &key, std::string &value, std::vector<uint32_t> fields) {
#ifdef ENABLE_STATISTICS
  PointProfiler getTimer;
  getTimer.start();
#endif
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
  // NKV_LOG_I(std::cout, "key: {} value: {} valuePtr: {}", key, value,
  //           idxIter->second);
#ifdef ENABLE_STATISTICS
  PointProfiler _overallTimer;
  _overallTimer.start();
#endif
  bool status =
      getValueFromIndexIterator(idxIter, indexer, key.schemaId, value, fields);

#ifdef ENABLE_STATISTICS
  _overallTimer.end();
  _durationStat.GetValueFromIteratorCount.fetch_add(1);
  _durationStat.GetValueFromIteratorTimeNanoSecs.fetch_add(
      _overallTimer.duration());
#endif
#ifdef ENABLE_STATISTICS
  getTimer.end();
  _durationStat.GetInterfaceCount.fetch_add(1);
  _durationStat.GetInterfaceTimeNanoSecs.fetch_add(getTimer.duration());
#endif
  return status;
}

bool NeoPMKV::put(Key &key, const std::string &value) {

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
  TimeStamp putTs;
  putTs.getNow();
  ValuePtr vPtr(pmAddr, putTs);

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
  // NKV_LOG_I(std::cout, "key: {} value: {} valuePtr: {}", key, value, vPtr);
  // status is true means insert success, we don't have the kv before
  if (status == true) return true;
  // status is false means having the old kv
  if (_enable_pbrb == true && iter->second.isHot() == true) {
    _pbrb->dropRow(iter->second.getPBRBAddr());
  }
  iter->second.setColdPmemAddr(pmAddr, putTs);

  return true;
}
bool NeoPMKV::update(Key &key,
                     std::vector<std::pair<std::string, std::string>> &values) {

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
  TimeStamp updatetTs;
  updatetTs.getNow();

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

  vPtr.setColdPmemAddr(pmemAddr, updatetTs);
  return true;
}
bool NeoPMKV::update(Key &key,
                     std::vector<std::pair<uint32_t, std::string>> &values) {
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
  TimeStamp updatetTs;
  updatetTs.getNow();

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

  vPtr.setColdPmemAddr(pmemAddr, updatetTs);
  return true;
}
bool NeoPMKV::remove(Key &key) {
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
                   uint32_t scan_len, std::vector<uint32_t> fields) {

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

#ifdef ENABLE_STATISTICS
  PointProfiler _overallTimer;
  _overallTimer.start();
#endif

  for (auto i = 0; i < scan_len && iter != indexer->end(); i++, iter++) {
    std::string tmp_value;
    getValueFromIndexIterator(iter, indexer, start.schemaId, tmp_value, fields);
    value_list.push_back(tmp_value);
  }
#ifdef ENABLE_STATISTICS
  _overallTimer.end();
  _durationStat.GetValueFromIteratorCount.fetch_add(scan_len);
  _durationStat.GetValueFromIteratorTimeNanoSecs.fetch_add(
      _overallTimer.duration());
#endif
  return true;
}
#ifdef ENABLE_STATISTICS
void NeoPMKV::outputReadStat() {
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
}
#endif

}  // namespace NKV
