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

#include "diskann_file_reader.h"
#include <cassert>
#include <cstdio>
#include <iostream>

#define MAX_EVENTS 1024

namespace zvec {
namespace core {

typedef struct io_event io_event_t;
typedef struct iocb iocb_t;


int setup_io_ctx(IOContext &ctx) {
  int ret = io_setup(MAX_EVENTS, &ctx);

  return ret;
}

int destroy_io_ctx(IOContext &ctx) {
  int ret = io_destroy(ctx);

  return ret;
}

int execute_io(io_context_t ctx, int fd, std::vector<AlignedRead> &read_reqs,
               uint64_t n_retries = 0) {
  uint64_t iters =
      DiskAnnUtil::round_up(read_reqs.size(), MAX_EVENTS) / MAX_EVENTS;

  for (uint64_t iter = 0; iter < iters; iter++) {
    uint64_t n_ops = std::min((uint64_t)read_reqs.size() - (iter * MAX_EVENTS),
                              (uint64_t)MAX_EVENTS);

    std::vector<iocb_t *> cbs(n_ops, nullptr);
    std::vector<io_event_t> evts(n_ops);
    std::vector<struct iocb> cb(n_ops);
    for (uint64_t j = 0; j < n_ops; j++) {
      io_prep_pread(cb.data() + j, fd, read_reqs[j + iter * MAX_EVENTS].buf,
                    read_reqs[j + iter * MAX_EVENTS].len,
                    read_reqs[j + iter * MAX_EVENTS].offset);
    }

    for (uint64_t i = 0; i < n_ops; i++) {
      cbs[i] = cb.data() + i;
    }

    size_t n_tries = 0;
    while (n_tries <= n_retries) {
      int ret = io_submit(ctx, (int64_t)n_ops, cbs.data());

      if (ret != (int)n_ops) {
        LOG_ERROR(
            "io_submit failed; returned: %d, expected=%lu, ernno=%d, %s, try "
            "#: %lu",
            ret, n_ops, errno, ::strerror(-ret), n_tries + 1);
        return IndexError_Runtime;
      } else {
        ret = io_getevents(ctx, (int64_t)n_ops, (int64_t)n_ops, evts.data(),
                           nullptr);
        if (ret != (int64_t)n_ops) {
          LOG_ERROR(
              "io_getevents failed; returned: %d, expected=%lu, ernno=%d, %s, "
              "try #: %lu",
              ret, n_ops, errno, ::strerror(-ret), n_tries + 1);
          return IndexError_Runtime;
        } else {
          break;
        }
      }
    }
  }

  return 0;
}

LinuxAlignedFileReader::LinuxAlignedFileReader(int file_desc) {
  this->file_desc = file_desc;
}

LinuxAlignedFileReader::LinuxAlignedFileReader() {
  this->file_desc = -1;
}

LinuxAlignedFileReader::~LinuxAlignedFileReader() {
  int64_t ret;
  ret = ::fcntl(this->file_desc, F_GETFD);
  if (ret == -1) {
    if (errno != EBADF) {
      std::cerr << "close() not called" << std::endl;
      ret = ::close(this->file_desc);
      if (ret == -1) {
        std::cerr << "close() failed; returned " << ret << ", errno=" << errno
                  << ":" << ::strerror(errno) << std::endl;
      }
    }
  }
}

io_context_t &LinuxAlignedFileReader::get_ctx() {
  std::unique_lock<std::mutex> lk(ctx_mut);
  if (ctx_map.find(std::this_thread::get_id()) == ctx_map.end()) {
    std::cerr << "bad thread access; returning -1 as io_context_t" << std::endl;
    return this->bad_ctx;
  } else {
    return ctx_map[std::this_thread::get_id()];
  }
}

void LinuxAlignedFileReader::register_thread() {
  auto thread_id = std::this_thread::get_id();
  std::unique_lock<std::mutex> lk(ctx_mut);
  if (ctx_map.find(thread_id) != ctx_map.end()) {
    LOG_ERROR("multiple calls to register_thread from the same thread");

    return;
  }

  io_context_t ctx = 0;

  int ret = io_setup(MAX_EVENTS, &ctx);
  if (ret != 0) {
    lk.unlock();
    if (ret == -EAGAIN) {
      LOG_ERROR(
          "io_setup failed with EAGAIN: Consider increasing "
          "/proc/sys/fs/aio-max-nr");
    } else {
      LOG_ERROR("io_setup failed; returned: %d, %s", ret, ::strerror(-ret));
      ;
    }
  } else {
    LOG_INFO("allocating ctx: %lu", (uint64_t)ctx);

    ctx_map[thread_id] = ctx;
  }

  lk.unlock();
}

void LinuxAlignedFileReader::deregister_thread() {
  auto thread_id = std::this_thread::get_id();
  std::unique_lock<std::mutex> lk(ctx_mut);
  assert(ctx_map.find(thread_id) != ctx_map.end());

  lk.unlock();
  io_context_t ctx = this->get_ctx();
  io_destroy(ctx);
  //  assert(ret == 0);
  lk.lock();
  ctx_map.erase(thread_id);

  LOG_INFO("returned ctx from thread");

  lk.unlock();
}

void LinuxAlignedFileReader::deregister_all_threads() {
  std::unique_lock<std::mutex> lk(ctx_mut);
  for (auto x = ctx_map.begin(); x != ctx_map.end(); x++) {
    io_context_t ctx = x->second;
    io_destroy(ctx);
  }
  ctx_map.clear();
}

void LinuxAlignedFileReader::open(const std::string &fname) {
  int flags = O_DIRECT | O_RDONLY | O_LARGEFILE;
  this->file_desc = ::open(fname.c_str(), flags);

  // error checks
  ailego_assert(this->file_desc != -1);

  LOG_INFO("Opened file : %s", fname.c_str());
}

void LinuxAlignedFileReader::close() {
  ::fcntl(this->file_desc, F_GETFD);
  ::close(this->file_desc);
}

int LinuxAlignedFileReader::read(std::vector<AlignedRead> &read_reqs,
                                 io_context_t &ctx, bool async) {
  if (async == true) {
    LOG_WARN("Async currently not supported");
  }

  assert(this->file_desc != -1);

  int ret = execute_io(ctx, this->file_desc, read_reqs);

  return ret;
}

}  // namespace core
}  // namespace zvec
