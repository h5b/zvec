#pragma once

#include "mmap_container.h"

class MallocContainer {
  public:
    MallocContainer(const std::string& filename) {
        FILE* fin = fopen(filename.c_str(), "rb");
        if (!fin) {
            throw std::runtime_error("Failed to open file: " + filename);
        }
        fseek(fin, 0, SEEK_END);
        file_size_ = ftell(fin);
        fseek(fin, 0, SEEK_SET);
        data_ = static_cast<char*>(std::malloc(file_size_));
        size_t read_size = fread(data_, 1, file_size_, fin);
        if (read_size != file_size_) {
            delete[] data_;
            fclose(fin);
            throw std::runtime_error("Failed to read file: " + filename);
        }
        fclose(fin);
    }
    ~MallocContainer() {
        std::free(data_);
    }

    MMapHandle get_handle() {
        return MMapHandle(data_, file_size_);
    }

    size_t file_size() const {
        return file_size_;
    }

  private:
    char* data_;
    size_t file_size_;
};