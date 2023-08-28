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
#include "kv_type.h"
#include "logging.h"
#include "pmem_engine.h"
#include "schema.h"
#include "schema_parser.h"

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
Status PmemLog::append(PmemAddress &pmemAddr, const char *value, uint32_t size,
                       bool noHead) {
  if (noHead == true) {
    std::string write_value;

    write_value.resize(size + NKV::ROW_META_HEAD_SIZE);
    NKV::RowMetaPtr(const_cast<char *>(write_value.data()))
        ->setMeta(size, NKV::RowType::FULL_DATA, 0, 0);
    memcpy(NKV::skipRowMeta(const_cast<char *>(write_value.data())), value,
           size);
    return this->append(pmemAddr, write_value.c_str(),
                        size + NKV::ROW_META_HEAD_SIZE);
  }
  return this->append(pmemAddr, value, size);
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

Status PmemLog::read(PmemAddress readAddr, std::string &value, bool noHead) {
  auto s = this->read(readAddr, value);
  if (noHead == true) {
    value.assign(value.substr(NKV::ROW_META_HEAD_SIZE, -1));
    return s;
  }
  return s;
}
Status PmemLog::read(PmemAddress readAddr, std::string &value) {
  // checkout the effectiveness of start_offset
  if (readAddr > _tail_offset.load()) {
    return PmemStatuses::S403_Forbidden_Invalid_Offset;
  }
  char *valuePtr = _convertToPtr(readAddr);
  uint32_t readSize = RowMetaPtr(valuePtr)->getSize() + ROW_META_HEAD_SIZE;
  value.resize(readSize);

  _read(value.data(), readAddr, readSize);
  return PmemStatuses::S200_OK_Found;
}
Status PmemLog::read(PmemAddress readAddr, std::string &value,
                     Schema *schemaPtr, uint32_t fieldId) {
  // checkout the effectiveness of start_offset
  if (readAddr > _tail_offset.load()) {
    return PmemStatuses::S403_Forbidden_Invalid_Offset;
  }
  char *valuePtr = _convertToPtr(readAddr);
  ValueReader fieldReader(schemaPtr);
  if (fieldReader.ExtractRowTypeFromRow(valuePtr) == RowType::FULL_DATA) {
    fieldReader.ExtractFieldFromFullRow(valuePtr, fieldId, value);
    return PmemStatuses::S200_OK_Found;
  }
  // not full value , must look for the partial value or look for prev
  bool containTarget =
      fieldReader.ExtractFieldFromPartialRow(valuePtr, fieldId, value);
  if (containTarget == true) return PmemStatuses::S200_OK_Found;
  PmemAddress prevPmemAddr = fieldReader.ExtractPrevRowFromPartialRow(valuePtr);
  return this->read(prevPmemAddr, value, schemaPtr, fieldId);
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
