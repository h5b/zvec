#pragma once

#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <map>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdexcept>
#include <limits>
#include <iostream>

#include "phmap.h"

using block_id_t = int;

#define BLOCK_SIZE (4 * 1024 * 1024)  // 2 MB
#define BLOCK_MASK (BLOCK_SIZE - 1)
#define BLOCK_ID(offset) (offset >> 22)
#define BLOCK_OFFSET(offset) (offset & BLOCK_MASK)

#define USE_LOCAL_CACHE

class LPMap {
    struct Entry {
        std::atomic<int> ref_count;
        char* buffer;
    };

  public:
    LPMap() : entry_num_(0), entries_(nullptr) {}
    ~LPMap() {
        delete[] entries_;
    }

    void init(size_t entry_num) {
        if (entries_) {
            delete[] entries_;
        }
        entry_num_ = entry_num;
        entries_ = new Entry[entry_num_];
        for (size_t i = 0; i < entry_num_; i++) {
            // entries_[i].ref_count.store(0);
            entries_[i].ref_count.store(std::numeric_limits<int>::min());
            entries_[i].buffer = nullptr;
        }
    }

    char* acquire_block(block_id_t block_id) {
        assert(block_id < entry_num_);
        Entry& entry = entries_[block_id];
        int rc = entry.ref_count.fetch_add(1);
        if (rc < 0) {
            return nullptr;
        }
        return entry.buffer;
    }

    void release_block(block_id_t block_id) {
        assert(block_id < entry_num_);
        Entry& entry = entries_[block_id];
        int rc = entry.ref_count.fetch_sub(1);
        assert(rc > 0);
    }

    // need be called under lock
    char* evict_block(block_id_t block_id) {
        assert(block_id < entry_num_);
        Entry& entry = entries_[block_id];
        int expected = 0;
        if (entry.ref_count.compare_exchange_strong(expected, std::numeric_limits<int>::min())) {
            char* buffer = entry.buffer;
            entry.buffer = nullptr;
            return buffer;
        } else {
            return nullptr;
        }
    }

    // need be called under lock
    char* set_block_acquired(block_id_t block_id, char* buffer) {
        // std::cout << "Set block " << block_id << std::endl;
        assert(block_id < entry_num_);
        Entry& entry = entries_[block_id];
        if (entry.ref_count.load() >= 0) {
            entry.ref_count.fetch_add(1);
            return entry.buffer;
        }
        entry.buffer = buffer;
        entry.ref_count.store(1);
        return buffer;
    }

    // need be called under lock
    void recycle(std::queue<char*>& free_buffers) {
        for (size_t i = 0; i < entry_num_; i++) {
            Entry& entry = entries_[i];
            if (entry.ref_count.load() == 0) {
                char* buffer = evict_block(i);
                if (buffer) {
                    free_buffers.push(buffer);
                }
            }
        }
    }

    size_t entry_num() const {
        return entry_num_;
    }

  private:
    Entry* entries_;
    size_t entry_num_;
};

class BufferPool;

struct BufferPoolHandle {
    BufferPoolHandle(BufferPool& pool);
    BufferPoolHandle(BufferPoolHandle&& other) : pool(other.pool), local_cache(std::move(other.local_cache)), hit_num_(other.hit_num_) {
        other.local_cache.clear();
        other.hit_num_ = 0;
    }
    ~BufferPoolHandle();

    char* get_block(size_t offset, size_t size);

    void release_all();

    BufferPool& pool;
#ifdef USE_LOCAL_CACHE
    // std::unordered_map<block_id_t, char*> local_cache;
    phmap::flat_hash_map<block_id_t, char*> local_cache;
#else
    std::vector<block_id_t> local_cache;
#endif
    int hit_num_;
};

class BufferPool {
  public:
    BufferPool(const std::string& filename, size_t pool_capacity) : pool_capacity_(pool_capacity){
        fd_ = open(filename.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to open file: " + filename);
        }
        struct stat st;
        if (fstat(fd_, &st) < 0) {
            throw std::runtime_error("Failed to stat file: " + filename);
        }
        file_size_ = st.st_size;
        lp_map_.init((file_size_ + BLOCK_SIZE - 1) / BLOCK_SIZE);

        size_t buffer_num = pool_capacity_ / BLOCK_SIZE;
        for (size_t i = 0; i < buffer_num; i++) {
            char* buffer = (char*)aligned_alloc(64, BLOCK_SIZE);
            free_buffers_.push(buffer);
        }
        std::cout << "buffer_num: " << buffer_num << std::endl;
        std::cout << "entry_num: " << lp_map_.entry_num() << std::endl;
    }
    ~BufferPool() {
        close(fd_);
    }

    BufferPoolHandle get_handle() {
        return BufferPoolHandle(*this);
    }

    char* acquire_buffer(block_id_t block_id, int retry = 0) {
        char* buffer = lp_map_.acquire_block(block_id);
        if (buffer) {
            return buffer;
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (free_buffers_.empty()) {
                for (int i = 0; i < retry; i++) {
                    lp_map_.recycle(free_buffers_);
                    if (!free_buffers_.empty()) {
                        break;
                    }
                }
            }
            if (free_buffers_.empty()) {
                return nullptr;
            }
            buffer = free_buffers_.front();
            free_buffers_.pop();
        }
        size_t read_offset = static_cast<size_t>(block_id) * BLOCK_SIZE;
        size_t to_read = std::min<size_t>(BLOCK_SIZE, file_size_ - read_offset);

        ssize_t read_bytes = pread(fd_, buffer, to_read, read_offset);
        if (read_bytes != static_cast<ssize_t>(to_read)) {
            std::cerr << "Failed to read file at offset " << read_offset << std::endl;
            exit(-1);
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            char* placed_buffer = lp_map_.set_block_acquired(block_id, buffer);
            if (placed_buffer != buffer) {
                // another thread has set the block
                free_buffers_.push(buffer);
            }
            return placed_buffer;
        }
    }

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
    std::queue<char*> free_buffers_;
};


struct Counter {
    ~Counter() = default;

    static Counter& get_instance() {
        static Counter instance;
        return instance;
    }

    void record(const std::string& name, int64_t value) {
        auto it = static_counters.find(name);
        if (it == static_counters.end()) {
            auto counter = std::make_unique<std::atomic<int64_t>>(0);
            it = static_counters.emplace(name, std::move(counter)).first;
        }
        it->second->fetch_add(value);
    }

    void display() {
        for (const auto& pair : static_counters) {
            std::cout << pair.first << ": " << pair.second->load() << std::endl;
        }
    }

    void clear() {
        static_counters.clear();
    }

  private:
    Counter() {}
    std::map<std::string, std::unique_ptr<std::atomic<int64_t>>> static_counters;
};

BufferPoolHandle::BufferPoolHandle(BufferPool& pool) : pool(pool), hit_num_(0) {}
BufferPoolHandle::~BufferPoolHandle() {
    Counter::get_instance().record("buffer_pool_handle_hit_num", hit_num_);
    release_all();
}

char* BufferPoolHandle::get_block(size_t offset, size_t size) {
    block_id_t block_id = BLOCK_ID(offset);
    assert(block_id == BLOCK_ID(offset + size - 1));
#ifdef USE_LOCAL_CACHE
    auto it = local_cache.find(block_id);
    if (it != local_cache.end()) {
        hit_num_++;
        return it->second + BLOCK_OFFSET(offset);
    }
#endif

    char* buffer = pool.acquire_buffer(block_id, 3);
    if (buffer) {
#ifdef USE_LOCAL_CACHE
        local_cache[block_id] = buffer;
#else
        local_cache.push_back(block_id);
#endif
        return buffer + BLOCK_OFFSET(offset);
    }

    return nullptr;
}

void BufferPoolHandle::release_all() {
#ifdef USE_LOCAL_CACHE
    Counter::get_instance().record("buffer_pool_handle_release_call", local_cache.size());
    for (const auto& pair : local_cache) {
        pool.lp_map_.release_block(pair.first);
    }
#else
    for (block_id_t block_id : local_cache) {
        pool.lp_map_.release_block(block_id);
    }
#endif
    local_cache.clear();
}