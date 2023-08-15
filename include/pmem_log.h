//
//  pmem_log.h
//  PROJECT pmem_log
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <libpmem.h>
#include <atomic>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>
#include "logging.h"
#include "pmem_engine.h"
#include "schema.h"

namespace NKV {

class PmemLog : public PmemEngine {
 public:
  PmemLog() {}

  ~PmemLog() {
    _plog_meta.tail_offset = _tail_offset.load();

    if (_is_pmem) {
      _copyToPmem(_plog_meta_file.pmem_addr, (char *)&_plog_meta,
                  sizeof(_plog_meta));
    } else {
      _copyToNonPmem(_plog_meta_file.pmem_addr, (char *)&_plog_meta,
                     sizeof(_plog_meta));
    }

    pmem_unmap(_plog_meta_file.pmem_addr, sizeof(PmemEngineConfig));
    for (auto &i : _chunk_list) {
      pmem_unmap(i.pmem_addr, _plog_meta.chunk_size);
    }
  }

  Status init(PmemEngineConfig &plog_meta) override;

  Status append(PmemAddress &pmemAddr, const char *value,
                uint32_t size) override;

  Status write(PmemAddress writeAddr, const char *value,
               uint32_t size) override;

  Status read(PmemAddress readAddr, std::string &value) override;

  Status read(PmemAddress readAddr, std::string &value, Schema *schemPtr,
              uint32_t fieldId) override;

  Status seal() override;

  uint64_t getFreeSpace() override;

  uint64_t getUsedSpace() override;

 private:
  // private _append function to write srcdata to plog
  inline PmemAddress _append(char *srcdata, size_t len) {
    constexpr uint32_t head_size = sizeof(uint32_t);
    uint64_t now_tail_offset = _tail_offset.fetch_add(len + head_size);
    if ((1 + _active_chunk_id.load()) * _plog_meta.chunk_size <
        now_tail_offset + len + head_size) {
      _mutex.lock();
      _addNewChunk();
      now_tail_offset = _tail_offset.fetch_add(len + head_size);
      _mutex.unlock();
    }

    char *pmem_addr =
        _chunk_list[now_tail_offset / _plog_meta.chunk_size].pmem_addr +
        now_tail_offset % _plog_meta.chunk_size;
    NKV_LOG_D(std::cout,
              "Append data: len=>{} to offset=>{}, activate chunk id=>{}", len,
              now_tail_offset, _active_chunk_id.load());

    *(uint32_t *)pmem_addr = len;
    if (_is_pmem) {
      _copyToPmem(pmem_addr + head_size, srcdata, len);
    } else {
      _copyToNonPmem(pmem_addr + head_size, srcdata, len);
    }
    // atomic modify the tail_offset variable
    return now_tail_offset;
  }

  inline char *_convertToPtr(PmemAddress src) {
    uint32_t chunk_id = src / _plog_meta.chunk_size;
    char *pmem_addr =
        _chunk_list[chunk_id].pmem_addr + src % _plog_meta.chunk_size;
    NKV_LOG_D(std::cout, "Convert from: {} => {}", src, pmem_addr);
    return pmem_addr;
  }
  // private _read function to read data from given offset and length
  inline void _read(char *dst, PmemAddress src, uint32_t len) {
    NKV_LOG_D(std::cout, "Read data: offset =>{},len=>{}", src, len);
    uint32_t chunk_id = src / _plog_meta.chunk_size;
    char *pmem_addr =
        _chunk_list[chunk_id].pmem_addr + src % _plog_meta.chunk_size;
    memcpy(dst, pmem_addr, len);
  }

  inline void _write(PmemAddress dst, const char *src, uint32_t len) {
    NKV_LOG_D(std::cout, "Write data: offset =>{},len=>{}", dst, len);
    uint32_t chunk_id = dst / _plog_meta.chunk_size;
    char *pmem_addr =
        _chunk_list[chunk_id].pmem_addr + dst % _plog_meta.chunk_size;
    uint32_t head_size = sizeof(uint32_t);
    if (_is_pmem) {
      _copyToPmem(pmem_addr + head_size, src, len);
    } else {
      _copyToNonPmem(pmem_addr + head_size, src, len);
    }
  }

  // Map an existing file
  //   case 1 : file already exists
  //            return S500_Internal_Server_Error_Map_Fail
  //   case 2 : file is created successfully
  //            return S201_Created_File
  inline Status _mapExistingFile(std::string chunk_file, char **pmem_addr) {
    size_t mapped_len;
    if ((*pmem_addr = static_cast<char *>(pmem_map_file(
             chunk_file.c_str(), 0, 0, 0666, &mapped_len, &_is_pmem))) ==
        NULL) {
      NKV_LOG_E(std::cerr, "pmem map existing file fail!");
      return PmemStatuses::S500_Internal_Server_Error_Map_Fail;
    }
    return PmemStatuses::S200_OK_Map;
  }

  // Create and Map file to userspace
  //  case 1: no enough space for creating files
  //          return S507_Insufficient_Storage
  //  case 2: targe file already existed
  //          return S409_Conflict_Created_File
  //  case 3: create file successfully
  //          return S201_Created_File
  inline Status _createThenMapFile(std::string file_name, uint64_t file_size,
                                   char **pmemAddr) {
    // check avaliable space in engine data path
    const std::filesystem::space_info si =
        std::filesystem::space(_plog_meta.engine_path);
    if (si.available < file_size) {
      return PmemStatuses::S507_Insufficient_Storage;
    }

    // create the target file
    size_t mapped_len;
    if ((*pmemAddr = static_cast<char *>(pmem_map_file(
             file_name.c_str(), file_size, PMEM_FILE_CREATE | PMEM_FILE_EXCL,
             0666, &mapped_len, &_is_pmem))) == NULL) {
      NKV_LOG_E(std::cerr, "pmem create existing file!");
      return PmemStatuses::S409_Conflict_File_Existed;
    }
    return PmemStatuses::S201_Created_File;
  }
  inline Status _addNewChunk() {
    if (_tail_offset.load() <
        (_active_chunk_id.load() + 1) * _plog_meta.chunk_size) {
      return PmemStatuses::S201_Created_File;
    }
    std::string chunk_name = _genNewChunkName();
    char *chunk_addr = nullptr;
    auto chunk_status =
        _createThenMapFile(chunk_name, _plog_meta.chunk_size, &chunk_addr);
    _plog_meta.chunk_count++;
    if (!chunk_status.is2xxOK()) {
      return chunk_status;
    }
    _chunk_list.push_back(
        {.file_name = std::move(chunk_name), .pmem_addr = chunk_addr});

    _active_chunk_id.fetch_add(1);
    _tail_offset.store(_plog_meta.chunk_size * _active_chunk_id.load());
    NKV_LOG_D(std::cout,
              "generate new chunk, now active chunk id:{}, tail offset:{}",
              _active_chunk_id.load(), _tail_offset.load());
    return PmemStatuses::S201_Created_File;
  }
  // generate the new chunk name
  inline std::string _genNewChunkName() {
    return fmt::format("{}/{}_{}.plog", _plog_meta.engine_path,
                       _plog_meta.plog_id, _active_chunk_id.load() + 1);
  }

  // generate the metadata file name
  inline std::string _genMetaFile() {
    return fmt::format("{}/{}.meta", _plog_meta.engine_path,
                       _plog_meta.plog_id);
  }

  // write data to file in nonpmem by memcpying method
  template <typename T,
            typename T2 = typename std::enable_if<std::is_pod<T>::value>::type>
  inline void _copyToNonPmem(char *dst, T *src, size_t len) {
    memcpy(dst, src, len);
    // Flush it
    pmem_msync(dst, len);
  }

  // write data to pmem file by store instruction
  template <typename T,
            typename T2 = typename std::enable_if<std::is_pod<T>::value>::type>
  inline void _copyToPmem(char *pmemAddr, T *src, size_t len) {
    pmem_memcpy_persist(pmemAddr, src, len);
  }

  struct FileInfo {
    std::string file_name;
    char *pmem_addr;
  };
  // indentify whether the target path is in pmem device
  // indicate when crating or mapping existing file
  int _is_pmem;

  // the current activate chunk id
  std::atomic<int> _active_chunk_id{-1};
  std::atomic<uint64_t> _tail_offset{0};

  // record all the information of chunks
  std::vector<FileInfo> _chunk_list;
  std::mutex _mutex;

  // define the meta file info
  // incluing file_name and pmem_addr
  FileInfo _plog_meta_file;

  // metadata of the plog
  // usually user defined
  PmemEngineConfig _plog_meta;
};

}  // namespace NKV
