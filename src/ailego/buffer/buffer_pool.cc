#include <zvec/ailego/buffer/buffer_pool.h>
#include <zvec/core/framework/index_logger.h>

namespace zvec {
namespace ailego {

int LRUCache::init(size_t block_size) {
  block_size_ = block_size;
  for (size_t i = 0; i < CATCH_QUEUE_NUM; i++) {
    queues_.push_back(ConcurrentQueue(block_size));
  }
  return 0;
}

bool LRUCache::evict_single_block(BlockType &item) {
  bool found = false;
  for (size_t i = 0; i < CATCH_QUEUE_NUM; i++) {
    found = queues_[i].try_dequeue(item);
    if (found) {
      break;
    }
  }
  return found;
}

bool LRUCache::add_single_block(const LPMap *lp_map, const BlockType &block,
                                int block_type) {
  bool ok = queues_[block_type].try_enqueue(block);
  evict_queue_insertions_.fetch_add(1, std::memory_order_relaxed);
  if (evict_queue_insertions_ % block_size_ == 0) {
    this->clear_dead_node(lp_map);
  }
  return ok;
}

void LRUCache::clear_dead_node(const LPMap *lp_map) {
  for (int i = 0; i < CATCH_QUEUE_NUM; i++) {
    int clear_size = block_size_ * 2;
    if (queues_[i].size_approx() < clear_size * 4) {
      continue;
    }
    int clear_count = 0;
    ConcurrentQueue tmp(block_size_);
    BlockType item;
    while (queues_[i].try_dequeue(item) && (clear_count++ < clear_size)) {
      if (!lp_map->isDeadBlock(item)) {
        tmp.try_enqueue(item);
      }
    }
    while (tmp.try_dequeue(item)) {
      if (!lp_map->isDeadBlock(item)) {
        queues_[i].try_enqueue(item);
      }
    }
  }
}

void LPMap::init(size_t entry_num) {
  if (entries_) {
    delete[] entries_;
  }
  entry_num_ = entry_num;
  entries_ = new Entry[entry_num_];
  for (size_t i = 0; i < entry_num_; i++) {
    entries_[i].ref_count.store(std::numeric_limits<int>::min());
    entries_[i].load_count.store(0);
    entries_[i].buffer = nullptr;
  }
  cache_.init(entry_num * 4);
}

char *LPMap::acquire_block(block_id_t block_id) {
  assert(block_id < entry_num_);
  Entry &entry = entries_[block_id];
  if (entry.ref_count.load(std::memory_order_relaxed) == 0) {
    entry.load_count.fetch_add(1, std::memory_order_relaxed);
  }
  entry.ref_count.fetch_add(1, std::memory_order_relaxed);
  if (entry.ref_count.load(std::memory_order_relaxed) < 0) {
    return nullptr;
  }
  return entry.buffer;
}

void LPMap::release_block(block_id_t block_id) {
  assert(block_id < entry_num_);
  Entry &entry = entries_[block_id];

  if (entry.ref_count.fetch_sub(1, std::memory_order_release) == 1) {
    std::atomic_thread_fence(std::memory_order_acquire);
    LRUCache::BlockType block;
    block.first = block_id;
    block.second = entry.load_count.load();
    cache_.add_single_block(this, block, 0);
  }
}

char *LPMap::evict_block(block_id_t block_id) {
  assert(block_id < entry_num_);
  Entry &entry = entries_[block_id];
  int expected = 0;
  if (entry.ref_count.compare_exchange_strong(
          expected, std::numeric_limits<int>::min())) {
    char *buffer = entry.buffer;
    entry.buffer = nullptr;
    return buffer;
  } else {
    return nullptr;
  }
}

char *LPMap::set_block_acquired(block_id_t block_id, char *buffer) {
  assert(block_id < entry_num_);
  Entry &entry = entries_[block_id];
  if (entry.ref_count.load(std::memory_order_relaxed) >= 0) {
    entry.ref_count.fetch_add(1, std::memory_order_relaxed);
    return entry.buffer;
  }
  entry.buffer = buffer;
  entry.ref_count.store(1, std::memory_order_relaxed);
  entry.load_count.fetch_add(1, std::memory_order_relaxed);
  return buffer;
}

void LPMap::recycle(moodycamel::ConcurrentQueue<char *> &free_buffers) {
  LRUCache::BlockType block;
  do {
    bool ok = cache_.evict_single_block(block);
    if (!ok) {
      return;
    }
  } while (isDeadBlock(block));
  char *buffer = evict_block(block.first);
  if (buffer) {
    free_buffers.try_enqueue(buffer);
  }
}

VecBufferPool::VecBufferPool(const std::string &filename) {
  fd_ = open(filename.c_str(), O_RDONLY);
  if (fd_ < 0) {
    throw std::runtime_error("Failed to open file: " + filename);
  }
  struct stat st;
  if (fstat(fd_, &st) < 0) {
    throw std::runtime_error("Failed to stat file: " + filename);
  }
  file_size_ = st.st_size;
}

int VecBufferPool::init(size_t pool_capacity, size_t block_size) {
  pool_capacity_ = pool_capacity;
  size_t buffer_num = pool_capacity_ / block_size + 10;
  size_t block_num = file_size_ / block_size + 10;
  lp_map_.init(block_num);
  for (size_t i = 0; i < buffer_num; i++) {
    char *buffer = (char *)aligned_alloc(64, block_size);
    if (buffer != nullptr) {
      bool ok = free_buffers_.try_enqueue(buffer);
    }
  }
  LOG_DEBUG("Buffer pool num: %zu, entry num: %zu", buffer_num,
            lp_map_.entry_num());
  return 0;
}

VecBufferPoolHandle VecBufferPool::get_handle() {
  return VecBufferPoolHandle(*this);
}

char *VecBufferPool::acquire_buffer(block_id_t block_id, size_t offset,
                                    size_t size, int retry) {
  char *buffer = lp_map_.acquire_block(block_id);
  if (buffer) {
    return buffer;
  }
  {
    bool found = free_buffers_.try_dequeue(buffer);
    if (!found) {
      for (int i = 0; i < retry; i++) {
        lp_map_.recycle(free_buffers_);
        found = free_buffers_.try_dequeue(buffer);
        if (found) {
          break;
        }
      }
    }
    if (!found) {
      LOG_ERROR("Buffer pool failed to get free buffer");
      return nullptr;
    }
  }

  ssize_t read_bytes = pread(fd_, buffer, size, offset);
  if (read_bytes != static_cast<ssize_t>(size)) {
    LOG_ERROR("Buffer pool failed to read file at offset: %zu", offset);
    return nullptr;
  }
  char *placed_buffer = nullptr;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    placed_buffer = lp_map_.set_block_acquired(block_id, buffer);
  }
  if (placed_buffer != buffer) {
    // another thread has set the block
    free_buffers_.try_enqueue(buffer);
  }
  return placed_buffer;
}

int VecBufferPool::get_meta(size_t offset, size_t length, char *buffer) {
  ssize_t read_bytes = pread(fd_, buffer, length, offset);
  if (read_bytes != static_cast<ssize_t>(length)) {
    LOG_ERROR("Buffer pool failed to read file at offset: %zu", offset);
    return -1;
  }
  return 0;
}

char *VecBufferPoolHandle::get_block(size_t offset, size_t size,
                                     size_t block_id) {
  char *buffer = pool.acquire_buffer(block_id, offset, size, 5);
  return buffer;
}

int VecBufferPoolHandle::get_meta(size_t offset, size_t length, char *buffer) {
  return pool.get_meta(offset, length, buffer);
}

void VecBufferPoolHandle::release_one(block_id_t block_id) {
  pool.lp_map_.release_block(block_id);
}

void VecBufferPoolHandle::acquire_one(block_id_t block_id) {
  pool.lp_map_.acquire_block(block_id);
}

}  // namespace ailego
}  // namespace zvec