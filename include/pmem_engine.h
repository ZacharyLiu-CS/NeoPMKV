//
//  pmem_engine.h
//  PROJECT pmem_engine
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include <tuple>
#include "schema.h"

namespace NKV {

// define the plog address
//


// define the pmem engine status
// Open Status
// S201_Created_Engine                     Create engine successfully
// S507_Insufficient_Storage               No enough space for pmem engine
// S403_Forbidden_Invalid_Config           Invalid pmem engine config

// Read status
// S200_OK_Found                           Found target data
// S403_Forbidden_Invalid_Offset           Invalid offset in pmem engine
// S403_Forbidden_Invalid_Size             Invalid size in pmem engine

// Append status
// S200_OK_Append                          Append data successfully
// S507_Insufficient_Storage_Over_Capcity  Data size is over the engine capcity
// S409_Conflict_Append_Sealed_engine      Append data to a sealed engine

// SealStatus
// S200_OK_Sealed                          Seal the engine successfully
// S200_OK_AlSealed                        Already sealed before sealing

// File operation
// S201_Created_File                       Create pmem engine file successfully
// S200_OK_Map                             Map file to userspace successfully
// S409_Conflict_File_Existed              File exists before creating
// S500_Internal_Server_Error_Create_Fail  Fail to create pmem engine file
// S500_Internal_Server_Error_Map_Fail     Fail to map a file to userspace

struct Status {
  int code;
  std::string message;

  // defined codes below
  Status operator()(std::string message) const
  {
    return Status { this->code, message };
  }

  // 2xx OK codes
  inline bool is2xxOK() const
  {
    return code >= 200 && code <= 299;
  }
};

struct PmemStatuses {
  // 200 OK
  // append data in pmem engine sucessfully
  static const inline Status S200_OK_Append { .code = 200, .message = "Append data to pemem engine successfully!" };

  // 200 OK
  // find the required data in pmem engine
  static const inline Status S200_OK_Found { .code = 200, .message = "Found the required data!" };

  // 200 OK
  // map file succeccfully
  static const inline Status S200_OK_Map { .code = 200, .message = "Map file successfully!" };

  // 201 OK
  // create pmem engine successfully
  static const inline Status S201_Created_Engine { .code = 201, .message = "Created pmem engine successfully!" };

  // 201 OK
  // create file successfully
  static const inline Status S201_Created_File { .code = 201, .message = "Created file successfully!" };

  // 200 OK the request is addressed rightly
  // the plog is sealed successfylly
  static const inline Status S200_OK_Sealed { .code = 200, .message = "Seal the target plog successfully!" };

  // 201 OK the request is addressed rightly
  // the plog is already sealed
  static const inline Status S200_OK_AlSealed { .code = 200, .message = "The target plog is already sealed!" };

  // 403 Forbidden
  // the offset is invalid in the existing Pmem
  static const inline Status S403_Forbidden_Invalid_Offset { .code = 403, .message = "Invalid offset in pmem!" };

  // 403 Forbidden
  // the size is invalid in the existing Pmem
  static const inline Status S403_Forbidden_Invalid_Size { .code = 403, .message = "Invalid size in pmem!" };

  // 409 Conflict
  // the file is already existed, so there is a conflict when creating file
  static const inline Status S409_Conflict_File_Existed { .code = 409, .message = "The file is already existed before existing!" };

  // 409 Conflict
  // the engine is already sealed, so it cannot serve append request
  static const inline Status S409_Conflict_Append_Sealed_engine { .code = 409, .message = "Try to append data to sealed engine!" };

  // 500 internal server error
  // fail to create file
  static const inline Status S500_Internal_Server_Error_Create_Fail { .code = 500, .message = "Create file failed!" };

  // 507 insufficient storage
  // no enough space for pemem engine to create file
  static const inline Status S507_Insufficient_Storage { .code = 507, .message = "Insufficien space for pmem engine!" };

  // 500 internal server error
  // fail to map a file
  static const inline Status S500_Internal_Server_Error_Map_Fail { .code = 500, .message = "Map a file failed!" };

  // 507 insufficient storage
  // the internal data size is over the capacity
  static const inline Status S507_Insufficient_Storage_Over_Capcity { .code = 507, .message = "Over the preset pmem engine capacity!" };

  // 403 forbidden
  // the configuration is invalid
  static const inline Status S403_Forbidden_Invalid_Config { .code = 403, .message = "Invalid pmem configuration!" };
};

//　plain old data format, satisfy the assignment with pmemcpy
// pmem storage engine config
struct PmemEngineConfig {

  // upper limit of pmem engine, default 100GB
  uint64_t engine_capacity = 100ULL << 30;

  // current offset of the plog, this value is persisted
  // when the plog is sealed or close
  uint64_t tail_offset = 0;

  // control chunk size, default 80MB
  uint64_t chunk_size = 80ULL << 20;

  // number of current chunks
  uint64_t chunk_count = 0;

  // engine_path define the path of stored files
  char engine_path[128] = "/mnt/pmem0/pmem-chogori/";

  // plogId is used to encode the chunk name
  // for example: if the plogId is userDataPlog
  // so the name of first chunk is userDataPlog_chunk0.plog
  //           the second chunk is userDataPlog_chunk1.plog
  char plog_id[128] = "userDataPlog";

  // is_sealed: true means sealed, false means activated
  bool is_sealed = false;
};

//
//  NKV pmem storage engine interface
//
class PmemEngine {
  public:
  // open function is used to open an existing plog or create a plog
  // the parameter path only refers to the directory of plogs
  // the parameter plogId refers to the name of the plog
  static Status open(PmemEngineConfig&, PmemEngine**);

  PmemEngine() { }

  PmemEngine(const PmemEngine&) = delete;

  PmemEngine& operator=(const PmemEngine&) = delete;

  virtual ~PmemEngine() { }

  virtual Status init(PmemEngineConfig& plog_meta) = 0;

  virtual Status append(PmemAddress &pmemAddr, const char *value, uint32_t size) = 0;

  virtual Status read(const PmemAddress &readAddr, std::string& value) = 0;

  virtual Status seal() = 0;

  virtual uint64_t getFreeSpace() = 0;

  virtual uint64_t getUsedSpace() = 0;
};

} // ns NKV
