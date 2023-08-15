//
//  kv.h
//  PROJECT kv
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <string>
#include <unordered_map>
#include "buffer_page.h"
#include "logging.h"
#include "mempool.h"
#include "pbrb.h"
#include "pmem_engine.h"
#include "pmem_log.h"
#include "profiler.h"
#include "schema.h"
#include "schema_parser.h"
#include "timestamp.h"

namespace NKV {

using std::pair;
using std::string;
using std::vector;
using SchemaParserMap = std::unordered_map<SchemaId, SchemaParser>;

class NeoPMKV {
 public:
  NeoPMKV(string db_path = "/mnt/pmem0/tmp-neopmkv",
          uint64_t chunk_size = 16ULL << 20, uint64_t db_size = 16ULL << 30,
          bool enable_pbrb = false, bool async_pbrb = false,
          bool enable_async_gc = false, uint32_t max_page_num = 1ull << 18,
          uint64_t rw_mirco = 2000, double gc_threshold = 0.7,
          uint64_t gc_inteval_micro = 2000, double hit_threshold = 0.3) {
    _enable_pbrb = enable_pbrb;
    _async_pbrb = async_pbrb;

    // initialize the pmemlog
    _engine_config.chunk_size = chunk_size;
    _engine_config.engine_capacity = db_size;
    string shard_path = db_path;
    strcpy(_engine_config.engine_path, shard_path.c_str());
    NKV::PmemEngine::open(_engine_config, &_engine_ptr);
    _memPoolPtr = new MemPool(pageSize, max_page_num);
    // start to setup pbrb
    if (_enable_pbrb == true) {
      // get the timestamp now
      TimeStamp ts_start_pbrb;
      ts_start_pbrb.getNow();
      _pbrb = new PBRB(max_page_num, &ts_start_pbrb, &_indexerList, &_sMap,
                       rw_mirco, 4, async_pbrb, enable_async_gc, gc_threshold,
                       gc_inteval_micro, hit_threshold);
    }
  }
  ~NeoPMKV() {
    delete _memPoolPtr;
    delete _engine_ptr;
    if (_enable_pbrb) {
      delete _pbrb;
    }
    outputReadStat();
  }

  SchemaId createSchema(vector<SchemaField> fields, uint32_t primarykeyId,
                        string name);
  bool MultiPartialGet(Key &key, vector<Value> &value,
                       const vector<uint32_t> fields);
  bool PartialGet(Key &key, Value &value, uint32_t field);
  bool Get(Key &key, Value &value);

  bool Put(Key &key, vector<Value> fieldList);
  bool Put(Key &key, const Value &value);

  bool PartialUpdate(Key &key, pair<uint32_t, string> &values);
  bool MultiPartialUpdate(Key &key, vector<pair<uint32_t, string>> &values);

  bool Remove(Key &key);
  bool Scan(Key &start, vector<Value> &valueList, uint32_t scanLen);
  bool PartialScan(Key &start, vector<Value> &valueList, uint32_t scanLen,
                   uint32_t fieldId);

  void outputReadStat();
 private:
  bool getValueFromIndexIterator(IndexerIterator &idxIter,
                                 std::shared_ptr<IndexerT> indexer,
                                 SchemaId schemaid, vector<Value> &value,
                                 vector<uint32_t> &fields);
  // use store the key -> valueptr
  IndexerList _indexerList;
  // use to allocate schema
  SchemaAllocator _schemaAllocator;
  // store schema id -> schema (unordered map)
  SchemaUMap _sMap;
  // schema parser list
  SchemaParserMap _sParser;
  MemPool *_memPoolPtr = nullptr;

  // pbrb part
  bool _enable_pbrb = false;
  bool _async_pbrb = false;
  PBRB *_pbrb = nullptr;

  PmemEngineConfig _engine_config;
  PmemEngine *_engine_ptr = nullptr;

  friend class VariableFieldTest;

  // Statistics:
  struct StatStruct {
    std::atomic<uint64_t> GetInterfaceCount = {0};
    std::atomic<uint64_t> GetInterfaceTimeNanoSecs = {0};

    std::atomic<uint64_t> GetValueFromIteratorCount = {0};
    std::atomic<uint64_t> GetValueFromIteratorTimeNanoSecs = {0};

    std::atomic<uint64_t> indexQueryCount = {0};
    std::atomic<uint64_t> indexQueryTimeNanoSecs = {0};

    std::atomic<uint64_t> indexInsertCount = {0};
    std::atomic<uint64_t> indexInsertTimeNanoSecs = {0};

    std::atomic<uint64_t> pmemReadCount = {0};
    std::atomic<uint64_t> pmemReadTimeNanoSecs = {0};

    std::atomic<uint64_t> pmemWriteCount = {0};
    std::atomic<uint64_t> pmemWriteTimeNanoSecs = {0};

    std::atomic<uint64_t> pmemUpdateCount = {0};
    std::atomic<uint64_t> pmemUpdateTimeNanoSecs = {0};

    std::atomic<uint64_t> pbrbReadCount = {0};
    std::atomic<uint64_t> pbrbReadTimeNanoSecs = {0};

    std::atomic<uint64_t> pbrbWriteCount = {0};
    std::atomic<uint64_t> pbrbWriteTimeNanoSecs = {0};
  } _durationStat;

};

}  // namespace NKV