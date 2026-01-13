// Copyright 2025-present the zvec project
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <map>
#include <mutex>
#include <framework/index_error.h>
#include <framework/index_factory.h>
#include <framework/index_format.h>
#include <framework/index_memory.h>
#include <framework/index_version.h>

namespace zvec {
namespace core {

/*! Memory Storage
 */
class MemoryStorage : public IndexStorage {
 public:
  /*! Index Storage Segment
   */
  class Segment : public IndexStorage::Segment,
                  public std::enable_shared_from_this<Segment> {
   public:
    struct Header {
      IndexFormat::SegmentMeta meta;
      uint8_t data[0];
    };
    //! Costructor
    Segment(Header *header) : hd_(header) {}

    //! Destructor
    virtual ~Segment(void) {}

    //! Retrieve size of data
    size_t data_size(void) const override {
      return hd_->meta.data_size;
    }

    //! Retrieve crc of data
    uint32_t data_crc(void) const override {
      return hd_->meta.data_crc;
    }

    //! Retrieve size of padding
    size_t padding_size(void) const override {
      return hd_->meta.padding_size;
    }

    //! Retrieve capacity of segment
    size_t capacity(void) const override {
      return hd_->meta.padding_size + hd_->meta.data_size;
    }

    //! Fetch data from segment (with own buffer)
    virtual size_t fetch(size_t offset, void *buf, size_t len) const override {
      if (ailego_unlikely(offset + len > hd_->meta.data_size)) {
        if (offset > hd_->meta.data_size) {
          offset = hd_->meta.data_size;
        }
        len = hd_->meta.data_size - offset;
      }
      memcpy(buf, &hd_->data[offset], len);
      return len;
    }

    //! Read data from segment
    size_t read(size_t offset, const void **buf, size_t len) override {
      if (ailego_unlikely(offset + len > hd_->meta.data_size)) {
        if (offset > hd_->meta.data_size) {
          offset = hd_->meta.data_size;
        }
        len = hd_->meta.data_size - offset;
      }
      *buf = &hd_->data[offset];
      return len;
    }

    size_t read(size_t offset, MemoryBlock &data, size_t len) override {
      if (ailego_unlikely(offset + len > hd_->meta.data_size)) {
        if (offset > hd_->meta.data_size) {
          offset = hd_->meta.data_size;
        }
        len = hd_->meta.data_size - offset;
      }
      data.reset(&hd_->data[offset]);
      return len;
    }

    //! Write data into the storage with offset
    size_t write(size_t offset, const void *buf, size_t len) override {
      size_t data_tail = offset + len;
      ailego_zero_if_false(data_tail <= capacity());
      if (data_tail > hd_->meta.data_size) {
        auto total = hd_->meta.data_size + hd_->meta.padding_size;
        hd_->meta.data_size = data_tail;
        hd_->meta.padding_size = total - data_tail;
      }
      memcpy(&hd_->data[offset], buf, len);
      return len;
    }

    //! Resize size of data
    size_t resize(size_t size) override {
      if (hd_->meta.data_size != size) {
        auto total = hd_->meta.data_size + hd_->meta.padding_size;
        if (size > total) {
          size = total;
        }
        hd_->meta.data_size = size;
        hd_->meta.padding_size = total - size;
      }
      return size;
    }

    //! Update crc of data
    void update_data_crc(uint32_t crc) override {
      hd_->meta.data_crc = crc;
    }

    //! Clone the segment
    IndexStorage::Segment::Pointer clone(void) override {
      return shared_from_this();
    }

   private:
    Header *hd_;
  };

  //! Constructor
  MemoryStorage(void) {}

  //! Constructor
  MemoryStorage(bool temporary) : temporary_(temporary) {}

  //! Destructor
  virtual ~MemoryStorage(void) {
    if (temporary_) {
      IndexMemory::Instance()->remove(path_);
    }
  }

  //! Initialize storage
  int init(const ailego::Params & /*params*/) override {
    return 0;
  }

  //! Cleanup storage
  int cleanup(void) override {
    return 0;
  }

  //! Open storage
  int open(const std::string &path, bool create) override {
    if (rope_) {
      return IndexError_Duplicate;
    }

    rope_ = IndexMemory::Instance()->open(path);
    if (!rope_) {
      if (!create) {
        return IndexError_NoExist;
      }
      rope_ = IndexMemory::Instance()->create(path);
      if (!rope_) {
        return IndexError_NoMemory;
      }
      //! add segments meta block. the first 8 Bytes is checkpoint
      rope_->append(sizeof(uint64_t));
    }
    path_ = path;
    return load_segments();
  }

  //! Flush storage
  int flush(void) override {
    return 0;
  }

  //! Close storage
  int close(void) override {
    rope_.reset();
    segments_.clear();
    if (temporary_) {
      IndexMemory::Instance()->remove(path_);
    }
    return 0;
  }

  //! Append a segment into storage
  int append(const std::string &id, size_t size) override {
    std::lock_guard<std::mutex> latch(mutex_);
    if (segments_.find(id) != segments_.end()) {
      return IndexError_Duplicate;
    }
    if (size == 0) {
      return IndexError_InvalidArgument;
    }
    auto segment = add_segment(id, size);
    if (!segment) {
      return IndexError_NoMemory;
    }
    return 0;
  }

  //! Refresh meta information (checksum, update time, etc.)
  void refresh(uint64_t checkpoint) override {
    (*rope_)[0].write(0UL, reinterpret_cast<const void *>(&checkpoint),
                      sizeof(uint64_t));
  }

  //! Retrieve check point of storage
  uint64_t check_point(void) const override {
    const void *data;
    (*rope_)[0].read(0UL, &data, sizeof(uint64_t));
    return *reinterpret_cast<const uint64_t *>(data);
  }

  //! Retrieve a segment by id
  IndexStorage::Segment::Pointer get(const std::string &id, int) override {
    std::lock_guard<std::mutex> latch(mutex_);
    auto it = segments_.find(id);
    return it == segments_.end() ? Segment::Pointer() : it->second;
  }

  //! Test if it a segment exists
  bool has(const std::string &id) const override {
    std::lock_guard<std::mutex> latch(mutex_);
    return (segments_.find(id) != segments_.end());
  }

  //! Retrieve magic number of index
  uint32_t magic(void) const override {
    return magic_;
  }

 private:
  Segment::Pointer add_segment(const std::string &id, size_t size) {
    auto &block = rope_->append(size + sizeof(Segment::Header));
    Segment::Header *hd = get_header(block, size + sizeof(Segment::Header));
    auto segment = std::make_shared<Segment>(hd);
    if (!hd || !segment) {
      return Segment::Pointer();
    }

    hd->meta.data_crc = 0U;
    hd->meta.data_index = rope_->count() - 1;
    hd->meta.data_size = 0UL;
    hd->meta.padding_size = size;

    // copy segment id
    auto &meta_block = (*rope_)[0];
    hd->meta.segment_id_offset = meta_block.size();
    meta_block.append(id.c_str(), std::strlen(id.c_str()));
    char c = '\0';
    meta_block.append(&c, sizeof(char));

    segments_[id] = segment;
    return segment;
  }

  Segment::Header *get_header(IndexMemory::Block &block, size_t size) {
    Segment::Header *hd = nullptr;
    size_t rd_len = block.read(0,
                               reinterpret_cast<const void **>(
                                   const_cast<const Segment::Header **>(&hd)),
                               size);
    return rd_len == size ? hd : nullptr;
  }

  //! Load segment from rope
  int load_segments(void) {
    if (rope_->count() == 0U) {
      return IndexError_InvalidArgument;
    }
    auto &meta_block = (*rope_)[0];
    const void *data;
    meta_block.read(0, &data, meta_block.size());
    for (size_t i = 1UL; i < rope_->count(); ++i) {
      IndexMemory::Block &block = (*rope_)[i];
      Segment::Header *hd = get_header(block, block.size());
      auto segment = std::make_shared<Segment>(hd);
      if (!hd || !segment) {
        return IndexError_NoMemory;
      }
      if (hd->meta.data_size + hd->meta.padding_size + sizeof(*hd) >
          block.size()) {
        return IndexError_InvalidFormat;
      }
      if (hd->meta.data_index != i) {
        return IndexError_InvalidFormat;
      }
      if (hd->meta.segment_id_offset > meta_block.size()) {
        return IndexError_InvalidFormat;
      }

      segments_.emplace(std::string(reinterpret_cast<const char *>(data) +
                                    hd->meta.segment_id_offset),
                        segment);
    }
    return 0;
  }

 private:
  uint32_t magic_{std::random_device()()};
  IndexMemory::Rope::Pointer rope_{};
  mutable std::mutex mutex_{};
  std::map<std::string, Segment::Pointer> segments_{};
  std::string path_{};
  bool temporary_{false};
};

INDEX_FACTORY_REGISTER_STORAGE(MemoryStorage);
INDEX_FACTORY_REGISTER_STORAGE_ALIAS(TemporaryMemoryStorage, MemoryStorage,
                                     true);

}  // namespace core
}  // namespace zvec
