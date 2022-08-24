//
//  timestamp.h
//  PROJECT timestamp
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <cstdint>

namespace NKV {
struct TimeStamp {
  uint64_t txn_nanoseconds;
  uint64_t unused;
};
}
