#pragma once

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>
#include <string>

class MMapHandle {
  public:
    MMapHandle(char* data, size_t file_size) : data_(data), file_size_(file_size) {}
    ~MMapHandle() = default;

    char* get_block(size_t offset, size_t size) {
        if (offset + size <= file_size_) {
            return data_ + offset;
        } else {
            return nullptr;
        }
    }

  private:
    char* data_;
    size_t file_size_;
};

class MMapContainer {
  public:
    MMapContainer(const std::string& filename) {
        fd_ = open(filename.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to open file: " + filename);
        }
        struct stat st;
        if (fstat(fd_, &st) < 0) {
            throw std::runtime_error("Failed to stat file: " + filename);
        }
        file_size_ = st.st_size;
        data_ = static_cast<char*>(
            mmap(nullptr, file_size_, PROT_READ, MAP_SHARED, fd_, 0)
        );
        if (data_ == MAP_FAILED) {
            throw std::runtime_error("Failed to mmap file: " + filename);
        }
    }
    ~MMapContainer() {
        munmap((void*)data_, file_size_);
        close(fd_);
    }

    MMapHandle get_handle() {
        return MMapHandle(data_, file_size_);
    }

    size_t file_size() const {
        return file_size_;
    }

  private:
    int fd_;
    char* data_;
    size_t file_size_;
};