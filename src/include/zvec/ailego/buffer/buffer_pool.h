#pragma once

#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include "concurrentqueue.h"

namespace zvec {
namespace ailego {

using block_id_t = size_t;
using version_t = size_t;

class LPMap;

class LRUCache {
 public:
  typedef std::pair<block_id_t, version_t> BlockType;
  typedef moodycamel::ConcurrentQueue<BlockType> ConcurrentQueue;

  int init(size_t block_size);

  bool evict_single_block(BlockType &item);

  bool add_single_block(const LPMap *lp_map, const BlockType &block,
                        int block_type);

  void clear_dead_node(const LPMap *lp_map);

 private:
  constexpr static size_t CATCH_QUEUE_NUM = 3;
  int block_size_;
  std::vector<ConcurrentQueue> queues_;
  alignas(64) std::atomic<size_t> evict_queue_insertions_{0};
};

class LPMap {
  struct Entry {
    alignas(64) std::atomic<int> ref_count;
    alignas(64) std::atomic<version_t> load_count;
    char *buffer;
  };

 public:
  LPMap() : entry_num_(0), entries_(nullptr) {}
  ~LPMap() {
    delete[] entries_;
  }

  void init(size_t entry_num);

  char *acquire_block(block_id_t block_id);

  void release_block(block_id_t block_id);

  // need be called under lock
  char *evict_block(block_id_t block_id);

  // need be called under lock
  char *set_block_acquired(block_id_t block_id, char *buffer);

  // need be called under lock
  void recycle(moodycamel::ConcurrentQueue<char *> &free_buffers);

  size_t entry_num() const {
    return entry_num_;
  }

  bool isDeadBlock(LRUCache::BlockType block) const {
    Entry &entry = entries_[block.first];
    return block.second != entry.load_count.load();
  }

 private:
  size_t entry_num_{0};
  Entry *entries_{nullptr};
  LRUCache cache_;
};

class VecBufferPoolHandle;

class VecBufferPool {
 public:
  typedef std::shared_ptr<VecBufferPool> Pointer;

  VecBufferPool(const std::string &filename);
  ~VecBufferPool() {
    close(fd_);
  }

  int init(size_t pool_capacity, size_t block_size);

  VecBufferPoolHandle get_handle();

  char *acquire_buffer(block_id_t block_id, size_t offset, size_t size,
                       int retry = 0);

  int get_meta(size_t offset, size_t length, char *buffer);

  size_t file_size() const {
    return file_size_;
  }

 private:
  int fd_;
  size_t file_size_;
  size_t pool_capacity_;

 public:
  LPMap lp_map_;

 private:
  std::mutex mutex_;
  moodycamel::ConcurrentQueue<char *> free_buffers_;
};

struct VecBufferPoolHandle {
  VecBufferPoolHandle(VecBufferPool &pool) : pool(pool), hit_num_(0) {};
  VecBufferPoolHandle(VecBufferPoolHandle &&other)
      : pool(other.pool), hit_num_(other.hit_num_) {
    other.hit_num_ = 0;
  }

  ~VecBufferPoolHandle() = default;

  typedef std::shared_ptr<VecBufferPoolHandle> Pointer;

  char *get_block(size_t offset, size_t size, size_t block_id);

  int get_meta(size_t offset, size_t length, char *buffer);

  void release_one(block_id_t block_id);

  void acquire_one(block_id_t block_id);

  VecBufferPool &pool;
  int hit_num_;
};

}  // namespace ailego
}  // namespace zvec