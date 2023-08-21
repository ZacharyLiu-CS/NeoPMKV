//
//  kv_type.cc
//
//  Created by zhenliu on 18/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//


#include "kv_type.h"
#include <atomic>

namespace NKV {

 // ValuePtr part
 ValuePtr::ValuePtr(PmemAddress pmAddr, TimeStamp ts) {
    _pmemAddr = pmAddr;
    _timestamp.store(ts, std::memory_order_release);
    _isHot.store(false, std::memory_order_release);
  }

  ValuePtr::ValuePtr(RowAddr rowAddr, TimeStamp ts) {
    _pbrbAddr = rowAddr;
    _timestamp.store(ts, std::memory_order_release);
    _isHot.store(true, std::memory_order_release);
  }

  ValuePtr::ValuePtr(const ValuePtr &valuePtr) {
    _pmemAddr = valuePtr._pmemAddr;
    _pbrbAddr = valuePtr._pbrbAddr;
    _timestamp.store(valuePtr._timestamp, std::memory_order_release);
    _isHot.store(valuePtr._isHot.load(std::memory_order_acquire),
                 std::memory_order_release);
  }

  std::pair<bool, TimeStamp> ValuePtr::getHotStatus() const {
    return {_isHot.load(std::memory_order_acquire),
            _timestamp.load(std::memory_order_acquire)};
  }
  bool ValuePtr::isHot() const { return _isHot.load(std::memory_order_acquire); }

  void ValuePtr::setFullColdPmemAddr(PmemAddress pmAddr, TimeStamp newTS) {
    _pmemAddr = pmAddr;
    _timestamp.store(newTS, std::memory_order_release);
    _prevItemCount.store(0, std::memory_order_release);
    _isHot.store(false, std::memory_order_release);
  }

  void ValuePtr::setPartialColdPmemAddr(PmemAddress pmAddr, TimeStamp newTS) {
    _pmemAddr = pmAddr;
    _timestamp.store(newTS, std::memory_order_release);
    _prevItemCount.fetch_add(1, std::memory_order_release);
    _isHot.store(false, std::memory_order_release);
  }


  void ValuePtr::evictToCold() { _isHot.store(false, std::memory_order_release); }

  bool ValuePtr::setHotTimeStamp(TimeStamp oldTS, TimeStamp newTS) {
    if (_timestamp.compare_exchange_weak(oldTS, newTS) == false) {
      return false;
    }
    _isHot.store(true, std::memory_order_release);
    return true;
  }
  bool ValuePtr::setHotPBRBAddr(RowAddr rowAddr, TimeStamp oldTS, TimeStamp newTS) {
    this->_pbrbAddr = rowAddr;
    if (_timestamp.compare_exchange_weak(oldTS, newTS) == false) {
      return false;
    }
    _isHot.store(true, std::memory_order_release);
    return true;
  }


} // end of namespace NKV
