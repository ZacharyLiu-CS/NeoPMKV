//
//  kv.h
//  PROJECT kv
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

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
class NeoPMKV {
 public:
  NeoPMKV(std::string db_path = "/mnt/pmem0/tmp-neopmkv",
          uint64_t chunk_size = 1ULL << 30, uint64_t db_size = 16ULL << 30) {
    _engine_config.chunk_size = chunk_size;
    _engine_config.engine_capacity = db_size;
    strcpy(_engine_config.engine_path, db_path.c_str());
    NKV::PmemEngine::open(_engine_config, &_engine_ptr);
  }
  ~NeoPMKV() {
    if (std::filesystem::exists(_engine_config.engine_path)) {
      std::filesystem::remove_all(_engine_config.engine_path);
    }
  }
  bool get(EntryKey key, std::string &value);
  bool put(EntryKey &key, const std::string &value);
  bool remove(EntryKey &key);
  bool scan(EntryKey &start, std::vector<std::string>& value_list,
            uint32_t scan_len);

 private:
  std::map<EntryKey, EntryValue> _indexer;

  PmemEngineConfig _engine_config;
  PmemEngine *_engine_ptr = nullptr;
};

}  // namespace NKV