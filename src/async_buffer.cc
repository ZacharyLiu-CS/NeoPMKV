//
//  async_buffer.cc
//
//  Created by zhenliu on 23/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//

#include "async_buffer.h"

namespace NKV {

// async buffer entry
AsyncBufferEntry::AsyncBufferEntry(uint32_t entry_size)
    : _entry_size(entry_size) {
  _entry_content.resize(_entry_size);
  _entryReady.store(false, std::memory_order_release);
}
bool AsyncBufferEntry::getContentReady() {
  return _entryReady.load(std::memory_order_acquire);
}
void AsyncBufferEntry::consumeContent() {
  _entryReady.store(false, std::memory_order_release);
}
bool AsyncBufferEntry::copyContent(TimeStamp oldTS, TimeStamp newTS,
                                   IndexerIterator iter, const Value &src) {
  if (_entryReady.load(std::memory_order_acquire) == false) {
    _oldTS = oldTS;
    _newTS = newTS;
    _iter = iter;
    memcpy((char *)_entry_content.data(), src.c_str(), _entry_size);
    _entryReady.store(true, std::memory_order_release);
    return true;
  }
  return false;
}

// async buffer queue
AsyncBufferQueue::AsyncBufferQueue(uint32_t schema_id, uint32_t schema_size,
                                   uint32_t queue_size)
    : _schema_id(schema_id),
      _schema_size(schema_size),
      _queue_size(queue_size) {
  _enqueue_head.store(0, std::memory_order_relaxed);
  _dequeue_tail.store(0, std::memory_order_relaxed);
  _queue_contents.resize(queue_size);
  for (auto i = 0; i < queue_size; i++) {
    _queue_contents[i] = std::make_shared<AsyncBufferEntry>(schema_size);
  }
}
SchemaId AsyncBufferQueue::getSchemaId() { return _schema_id; }

bool AsyncBufferQueue::EnqueueOneEntry(TimeStamp oldTS, TimeStamp newTS,
                                       IndexerIterator iter,
                                       const Value &value) {
  uint32_t allocated_offset =
      _enqueue_head.fetch_add(1, std::memory_order_relaxed);
  if (allocated_offset <
      _dequeue_tail.load(std::memory_order_relaxed) + _queue_size) {
    auto res = _queue_contents[allocated_offset % _queue_size]->copyContent(
        oldTS, newTS, iter, value);
    // NKV_LOG_I(std::cout, "Enqueue Entry [{}]: {}", allocated_offset,
    // value);
    if (res == true) return true;
  }
  _enqueue_head.fetch_sub(1, std::memory_order_relaxed);
  return false;
}

std::shared_ptr<AsyncBufferEntry> AsyncBufferQueue::DequeueOneEntry() {
  uint32_t accessing_offset =
      _dequeue_tail.fetch_add(1, std::memory_order_relaxed);

  if (accessing_offset < _enqueue_head.load(std::memory_order_relaxed)) {
    // NKV_LOG_I(std::cout, "Dequeue entry");
    auto bufferEntry = _queue_contents[accessing_offset % _queue_size];
    if (bufferEntry->getContentReady()) {
      // NKV_LOG_I(std::cout, "Dequeue Entry [{}]: {}", accessing_offset,
      //           bufferEntry->_entry_content);
      bufferEntry->consumeContent();
      return bufferEntry;
    }
  }
  _dequeue_tail.fetch_sub(1, std::memory_order_relaxed);
  return nullptr;
}
bool AsyncBufferQueue::Empty() {
  return _enqueue_head.load(std::memory_order_relaxed) <=
         _dequeue_tail.load(std::memory_order_relaxed);
}

}  // namespace NKV