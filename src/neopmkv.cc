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

namespace NKV {
bool NeoPMKV::get(EntryKey key, std::string &value) {
  uint32_t idx = key % SHARD_NUM;
  std::lock_guard<std::mutex> guard(_mutex[idx]);
  if (_indexer[idx].find(key) != _indexer[idx].end()) {
    EntryValue value_ptr = _indexer[idx][key];
    if (value_ptr == UINT64_MAX) {
      {
        return false;
      }
    }
    auto s = _engine_ptr[idx]->read(value_ptr, value);
    if (s.is2xxOK())
      return true;
    else
      return false;
  }
  return false;
}
bool NeoPMKV::put(EntryKey &key, const std::string &value) {
  uint32_t idx = key % SHARD_NUM;
  std::lock_guard<std::mutex> guard(_mutex[idx]);
  EntryValue value_ptr;
  Status s =
      _engine_ptr[idx]->append(value_ptr, (char *)value.c_str(), value.size());
  if (s.is2xxOK()) {
    _indexer[idx][key] = value_ptr;
    return true;
  }
  return false;
}
bool NeoPMKV::remove(EntryKey &key) {
  uint32_t idx = key % SHARD_NUM;
  std::lock_guard<std::mutex> guard(_mutex[idx]);
  if (_indexer[idx].find(key) != _indexer[idx].end()) {
    _indexer[idx][key] = UINT64_MAX;
    return true;
  }
  return false;
}
bool NeoPMKV::scan(EntryKey &start, std::vector<std::string> &value_list,
                   uint32_t scan_len) {
  uint32_t idx = start % SHARD_NUM;
  std::lock_guard<std::mutex> guard(_mutex[idx]);
  value_list.reserve(scan_len);
  auto iter = _indexer[idx].upper_bound(start);
  for (auto i = 0; i < scan_len && iter != _indexer[idx].end(); i++) {
    std::string tmp_value;
    _engine_ptr[idx]->read(iter->second, tmp_value);
    value_list.push_back(tmp_value);
    iter++;
  }
  return true;
}

}  // namespace NKV
