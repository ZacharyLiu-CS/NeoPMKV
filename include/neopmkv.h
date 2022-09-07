//
//  kv.h
//  PROJECT kv
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <string>
#include "buffer_page.h"
#include "logging.h"
#include "pbrb.h"
#include "pmem_engine.h"
#include "pmem_log.h"
#include "profiler.h"
#include "schema.h"
#include "timestamp.h"

namespace NKV {

#define SHARD_NUM 64

class NeoPMKV {
 public:
  NeoPMKV(std::string db_path = "/mnt/pmem0/tmp-neopmkv",
          uint64_t chunk_size = 16ULL << 20, uint64_t db_size = 16ULL << 30,
          bool enable_pbrb = false, bool async_pbrb = false,
          uint32_t maxPageNum = 1ull << 18) {
    _enable_pbrb = enable_pbrb;
    _async_pbrb = async_pbrb;

    // initialize the pmemlog
    _engine_config.chunk_size = chunk_size;
    _engine_config.engine_capacity = db_size;
    std::string shard_path = db_path;
    strcpy(_engine_config.engine_path, shard_path.c_str());
    NKV::PmemEngine::open(_engine_config, &_engine_ptr);

    // start to setup pbrb
    if (_enable_pbrb == true) {
      // get the timestamp now
      TimeStamp ts_start_pbrb;
      ts_start_pbrb.getNow();
      _pbrb = new PBRB(maxPageNum, &ts_start_pbrb, &_indexer, &_sUMap);
    }
  }
  ~NeoPMKV() {
    delete _engine_ptr;
    if (_enable_pbrb) {
      delete _pbrb;
    }
  }

  SchemaId createSchema(std::vector<SchemaField> fields, uint32_t primarykey_id,
                        std::string name) {
    Schema newSchema =
        _schemaAllocator.createSchema(name, primarykey_id, fields);
    _sUMap.addSchema(newSchema);
    return newSchema.schemaId;
  }

  bool get(Key &key, Value &value);
  bool put(Key &key, const Value &value);
  bool remove(Key &key);
  bool scan(Key &start, std::vector<Value> &value_list, uint32_t scan_len);

 private:
  bool getValueFromIndexIterator(IndexerIterator &idxIter, Value &value);
  // use store the key -> valueptr
  IndexerT _indexer;
  // use to allocate schema
  SchemaAllocator _schemaAllocator;
  // store schema id -> schema (unordered map)
  SchemaUMap _sUMap;

  // pbrb part
  bool _enable_pbrb = false;
  bool _async_pbrb = false;
  PBRB *_pbrb = nullptr;

  PmemEngineConfig _engine_config;
  PmemEngine *_engine_ptr = nullptr;
};

}  // namespace NKV