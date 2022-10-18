//
//  timestamp.h
//  PROJECT timestamp
//
//  Created by zhenliu on 22/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#pragma once
#include <fmt/format.h>
#include <cstdint>
#include "profiler.h"

namespace NKV {
struct TimeStamp {
  uint64_t txn_ticks = 0;

  void getNow() { txn_ticks = rte_rdtsc(); }

  void moveBackward(uint64_t ticks) { txn_ticks -= ticks; }
  bool eq(TimeStamp ts) { return this->txn_ticks == ts.txn_ticks; }
  bool ne(TimeStamp ts) { return this->txn_ticks != ts.txn_ticks; }
  bool gt(TimeStamp ts) { return this->txn_ticks > ts.txn_ticks; }
  bool ge(TimeStamp ts) { return this->txn_ticks >= ts.txn_ticks; }
  bool lt(TimeStamp ts) { return this->txn_ticks < ts.txn_ticks; }
  bool le(TimeStamp ts) { return this->txn_ticks <= ts.txn_ticks; }
};

}  // namespace NKV
template <>
struct fmt::formatter<NKV::TimeStamp> {
  // Parses format specifications of the form ['f' | 'e'].
  constexpr auto parse(format_parse_context &ctx) -> decltype(ctx.begin()) {
    return ctx.begin();
  }

  // Formats the point p using the parsed format specification (presentation)
  // stored in this formatter.
  template <typename FormatContext>
  auto format(const NKV::TimeStamp &ts, FormatContext &ctx) const
      -> decltype(ctx.out()) {
    // ctx.out() is an output iterator to write to.
    return fmt::format_to(ctx.out(), "{}", ts.txn_ticks);
  }
};