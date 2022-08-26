//
//  pbrb.cc
//  PROJECT pbrb
//
//  Created by zhenliu on 23/08/2022.
//  Copyright (c) 2022 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "pbrb.h"

namespace NKV {
PBRB::PBRB(int maxPageNumber, TimeStamp *wm, IndexerT *indexer,
           uint32_t maxPageSearchNum) {
  // initialization
  _watermark = *wm;
  _maxPageNumber = maxPageNumber;
  _indexer = indexer;
  _maxPageSearchingNum = maxPageSearchNum;

  // allocate bufferpage
  auto aligned_val = std::align_val_t{_pageSize};
  _bufferPoolPtr = static_cast<BufferPage *>(operator new(
      maxPageNumber * sizeof(BufferPage), aligned_val));

  for (int idx = 0; idx < maxPageNumber; idx++) {
    _freePageList.push_back(_bufferPoolPtr + idx);
  }
}

PBRB::~PBRB() {
  if (_bufferPoolPtr != nullptr) {
    delete _bufferPoolPtr;
  }
}

BufferPage *PBRB::getPageAddr(void *rowAddr) {
  return (BufferPage *)((uint64_t)rowAddr & ~mask);
}

}  // namespace NKV
