//
//  async_buffer.h
//
//  Created by zhenliu on 23/08/2023.
//  Copyright (c) 2023 zhenliu <liuzhenm@mail.ustc.edu.cn>.
//
#include <oneapi/tbb/concurrent_map.h>
#include <oneapi/tbb/concurrent_queue.h>
#include <oneapi/tbb/concurrent_set.h>
#include <oneapi/tbb/concurrent_vector.h>
#include "field_type.h"
#include "kv_type.h"
#include "schema.h"

namespace NKV {

using IndexerT =
    oneapi::tbb::concurrent_map<decltype(Key::primaryKey), ValuePtr>;
using IndexerList = std::unordered_map<SchemaId, std::shared_ptr<IndexerT>>;
using IndexerIterator = IndexerT::iterator;

// async buffer entry
struct AsyncBufferEntry {
  uint32_t _entry_size = 0;
  TimeStamp _oldTS;
  TimeStamp _newTS;
  IndexerIterator _iter;
  std::atomic_bool _entryReady{false};
  Value _entry_content;

  AsyncBufferEntry(uint32_t entry_size);
  bool copyContent(TimeStamp oldTS, TimeStamp newTS, IndexerIterator iter,
                   const Value &src);

  inline bool getContentReady() {
    return _entryReady.load(std::memory_order_acquire);
  }
  inline void consumeContent() {
    _entryReady.store(false, std::memory_order_release);
  }
};

// async buffer queue
class AsyncBufferQueue {
 private:
  SchemaId _schema_id = 0;
  uint32_t _schema_size = 0;
  uint32_t _queue_size = 0;

  std::vector<std::shared_ptr<AsyncBufferEntry>> _queue_contents;
  // control the queue push and pop function
  std::atomic_uint32_t _enqueue_head{0};
  std::atomic_uint32_t _dequeue_tail{0};

 public:
  AsyncBufferQueue(uint32_t schema_id, uint32_t schema_size,
                   uint32_t queue_size);
  SchemaId getSchemaId();

  bool EnqueueOneEntry(TimeStamp oldTS, TimeStamp newTS, IndexerIterator iter,
                       const Value &value);
  std::shared_ptr<AsyncBufferEntry> DequeueOneEntry();
  bool Empty();
};

}  // namespace NKV