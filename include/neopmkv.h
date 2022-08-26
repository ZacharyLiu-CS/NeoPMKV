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

using EntryKey = uint64_t;
using EntryValue = PmemAddress;
#define SHARD_NUM 64

class NeoPMKV {
 public:
  NeoPMKV(std::string db_path = "/mnt/pmem0/tmp-neopmkv",
          uint64_t chunk_size = 16ULL << 20, uint64_t db_size = 16ULL << 30) {
    for (uint32_t i = 0; i < SHARD_NUM; i++) {
      _engine_config[i].chunk_size = chunk_size;
      _engine_config[i].engine_capacity = db_size;
      std::string shard_path = db_path + "/shard" + std::to_string(i);
      strcpy(_engine_config[i].engine_path, shard_path.c_str());
      NKV::PmemEngine::open(_engine_config[i], &_engine_ptr[i]);
    }
  }
  ~NeoPMKV() {for(uint32_t i = 0; i< SHARD_NUM ;i++) delete _engine_ptr[i]; }
  bool get(EntryKey key, std::string &value);
  bool put(EntryKey &key, const std::string &value);
  bool remove(EntryKey &key);
  bool scan(EntryKey &start, std::vector<std::string> &value_list,
            uint32_t scan_len);

 private:
  std::map<EntryKey, EntryValue> _indexer[SHARD_NUM];
  std::mutex _mutex[SHARD_NUM];
  PmemEngineConfig _engine_config[SHARD_NUM];
  PmemEngine *_engine_ptr[SHARD_NUM];
};

}  // namespace NKV