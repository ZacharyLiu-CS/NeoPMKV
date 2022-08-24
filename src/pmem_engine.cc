//
//  pmem_engine.cc
//  PROJECT pmem_engine
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "pmem_engine.h"
#include "pmem_log.h"

namespace NKV {

Status PmemEngine::open(PmemEngineConfig &plog_meta, PmemEngine ** engine_ptr){
    if ( plog_meta.chunk_size > plog_meta.engine_capacity
        || plog_meta.is_sealed == true ){
        return PmemStatuses::S403_Forbidden_Invalid_Config;
    }
    PmemLog * engine = new PmemLog;
    Status s = engine->init(plog_meta);
    if (!s.is2xxOK()){
        NKV_LOG_E(std::cerr,"Create PmemLog Failed!");
        delete engine;
    }else{
        *engine_ptr = engine;
    }
    return s;
}

} // namespace NKV
