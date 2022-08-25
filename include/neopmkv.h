//
//  kv.h
//  PROJECT kv
//
//  Created by zhenliu on 25/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once

#include "buffer_page.h"
#include "pmem_engine.h"
#include "pmem_log.h"
#include "logging.h"
#include "pbrb.h"
#include "schema.h"
#include "profiler.h"
#include "timestamp.h"

namespace NKV{
class NeoPMKV{
public:
  bool get(const std::string &key, std::string &value);
  bool put(const std::string &key, const std::string &value);
  bool remove(const std::string &key);
  bool scan(const std::string &start, std::vector<std::string> value_list, uint32_t scan_len);

};

} // end of NKV