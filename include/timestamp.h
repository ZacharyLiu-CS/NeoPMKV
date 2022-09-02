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
  bool eq(TimeStamp ts) {
    return this->txn_nanoseconds == ts.txn_nanoseconds;
  }
  bool ne(TimeStamp ts) {
    return this->txn_nanoseconds != ts.txn_nanoseconds;
  }
  bool gt(TimeStamp ts) {
    return this->txn_nanoseconds > ts.txn_nanoseconds;
  }
  bool ge(TimeStamp ts) {
    return this->txn_nanoseconds >= ts.txn_nanoseconds;
  }
  bool lt(TimeStamp ts) {
    return this->txn_nanoseconds < ts.txn_nanoseconds;
  }
  bool le(TimeStamp ts) {
    return this->txn_nanoseconds <= ts.txn_nanoseconds;
  }
};
}  // namespace NKV
