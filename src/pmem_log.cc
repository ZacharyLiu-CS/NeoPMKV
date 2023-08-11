//
//  pmem_log.cc
//  PROJECT pmem_log
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "pmem_log.h"
#include <bits/stdint-uintn.h>
#include <fmt/format.h>
#include "logging.h"
#include "pmem_engine.h"
#include "schema.h"

namespace NKV {

Status PmemLog::init(PmemEngineConfig &plog_meta) {
  // create the directory if not exist
  if (!std::filesystem::exists(plog_meta.engine_path)) {
    bool res = std::filesystem::create_directories(plog_meta.engine_path);
    if (res == false) {
      return PmemStatuses::S500_Internal_Server_Error_Create_Fail;
    }
  }
  const std::filesystem::space_info si =
      std::filesystem::space(plog_meta.engine_path);
  if (si.available < plog_meta.engine_capacity) {
    return PmemStatuses::S507_Insufficient_Storage;
  }
  // get engine_path and plog_id from the input parm
  _plog_meta = plog_meta;
  std::string meta_file_name = _genMetaFile();
  // check whether the metafile exists
  std::filesystem::path meteaFilePath(meta_file_name);
  bool is_metafile_existed = std::filesystem::exists(meteaFilePath);
  // if exists, we donn't need to create, and we just map them
  Status meta_map_res;
  char *meta_file_addr = nullptr;
  if (is_metafile_existed) {
    meta_map_res = _mapExistingFile(meta_file_name, &meta_file_addr);
  }
  // if not exists, we need to create those files for them
  else {
    meta_map_res =
        _createThenMapFile(meta_file_name, sizeof(plog_meta), &meta_file_addr);
  }

  // check out the status
  if (!meta_map_res.is2xxOK()) {
    return meta_map_res;
  }
  // assign the plog metadata file name and pmem_addr
  _plog_meta_file.file_name = meta_file_name;
  _plog_meta_file.pmem_addr = meta_file_addr;

  if (is_metafile_existed) {
    // assign the plog metadata info from pmem space
    _plog_meta = *(PmemEngineConfig *)_plog_meta_file.pmem_addr;
    plog_meta = _plog_meta;
    for (uint64_t i = 0; i < _plog_meta.chunk_count; i++) {
      std::string chunk_name = _genNewChunkName();
      char *plog_addr = nullptr;
      auto chunk_status = _mapExistingFile(chunk_name, &plog_addr);
      _active_chunk_id.fetch_add(1);
      if (!chunk_status.is2xxOK()) {
        return chunk_status;
      }
      _chunk_list.push_back(
          {.file_name = std::move(chunk_name), .pmem_addr = plog_addr});
    }

    _tail_offset.store(_plog_meta.tail_offset);
    _active_chunk_id.store(_tail_offset.load() / _plog_meta.chunk_size);
  } else {
    _plog_meta = plog_meta;
    // write metadata to metaFile
    if (_is_pmem) {
      _copyToPmem(_plog_meta_file.pmem_addr, (char *)&_plog_meta,
                  sizeof(_plog_meta));
    } else {
      _copyToNonPmem(_plog_meta_file.pmem_addr, (char *)&_plog_meta,
                     sizeof(_plog_meta));
    }
    // create first chunk
    return _addNewChunk();
  }
  return PmemStatuses::S201_Created_Engine;
}

Status PmemLog::append(PmemAddress &pmemAddr, const char *value,
                       uint32_t size) {
  PmemSize append_size = size + sizeof(uint32_t);
  // checkout the is_sealed condition
  if (_plog_meta.is_sealed) {
    return PmemStatuses::S409_Conflict_Append_Sealed_engine;
  }
  // checkout the capacity
  if (_tail_offset.load() + append_size > _plog_meta.engine_capacity) {
    return PmemStatuses::S507_Insufficient_Storage_Over_Capcity;
  }
  pmemAddr = _append((char *)value, size);
  return PmemStatuses::S200_OK_Append;
}

Status PmemLog::write(PmemAddress writeAddr, const char *value, uint32_t size) {
  if (writeAddr > _tail_offset.load()) {
    return PmemStatuses::S403_Forbidden_Invalid_Offset;
  }
  _write(writeAddr, value, size);
  return PmemStatuses::S200_OK_Write;
}
Status PmemLog::read(PmemAddress readAddr, std::string &value) {
  // checkout the effectiveness of start_offset
  if (readAddr > _tail_offset.load()) {
    return PmemStatuses::S403_Forbidden_Invalid_Offset;
  }
  uint32_t read_size = 0;
  _read((char *)&read_size, readAddr, sizeof(uint32_t));
  return read(readAddr + sizeof(uint32_t), value, read_size);
}

Status PmemLog::read(PmemAddress readAddr, std::string &value, uint32_t size) {
  // checkout the effectiveness of start_offset
  if (readAddr > _tail_offset.load()) {
    return PmemStatuses::S403_Forbidden_Invalid_Offset;
  }

  // checkout the effectiveness of read size
  uint64_t chunk_remaing_space =
      _plog_meta.chunk_size - readAddr % _plog_meta.chunk_size;
  if (chunk_remaing_space < size) {
    return PmemStatuses::S403_Forbidden_Invalid_Size;
  }
  if (readAddr + size > _tail_offset.load()) {
    return PmemStatuses::S403_Forbidden_Invalid_Size;
  }
  // pass the check
  value.resize(size);
  _read((char *)value.data(), readAddr, size);

  return PmemStatuses::S200_OK_Found;
}
Status PmemLog::seal() {
  if (_plog_meta.is_sealed == false) {
    return PmemStatuses::S200_OK_Sealed;
  } else {
    return PmemStatuses::S200_OK_AlSealed;
  }
}
uint64_t PmemLog::getFreeSpace() {
  return _plog_meta.engine_capacity - _tail_offset.load();
}

uint64_t PmemLog::getUsedSpace() { return _tail_offset.load(); };

}  // namespace NKV
