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
#pragma once

#include <zvec/core/framework/index_framework.h>
#include "diskann_context.h"
#include "diskann_index.h"

class LinuxAlignedFileReader;

namespace zvec {
namespace core {

class DiskAnnSearcher : public IndexSearcher {
 public:
  using ContextPointer = IndexSearcher::Context::Pointer;

 public:
  DiskAnnSearcher(void);
  ~DiskAnnSearcher(void);

  DiskAnnSearcher(const DiskAnnSearcher &) = delete;
  DiskAnnSearcher &operator=(const DiskAnnSearcher &) = delete;

 protected:
  //! Initialize Searcher
  virtual int init(const ailego::Params &params) override;

  //! Cleanup Searcher
  virtual int cleanup(void) override;

  //! Load Index from storage
  virtual int load(IndexStorage::Pointer storage,
                   IndexMetric::Pointer metric) override;

  //! Unload index from storage
  virtual int unload(void) override;

  //! KNN Search
  virtual int search_impl(const void *query,
                          const IndexQueryMeta &qmeta,
                          ContextPointer &context) const override {
    return search_impl(query, qmeta, 1, context);
  }

  //! KNN Search
  virtual int search_impl(const void *query,
                          const IndexQueryMeta &qmeta, uint32_t count,
                          ContextPointer &context) const override;

  //! Linear Search
  virtual int search_bf_impl(const void *query,
                             const IndexQueryMeta &qmeta,
                             ContextPointer &context) const override {
    return search_bf_impl(query, qmeta, 1, context);
  }

  //! Linear Search
  virtual int search_bf_impl(const void *query,
                             const IndexQueryMeta &qmeta,
                             uint32_t count,
                             ContextPointer &context) const override;

  //! Linear search by primary keys
  virtual int search_bf_by_p_keys_impl(
      const void *query, const std::vector<std::vector<uint64_t>> &p_keys,
      const IndexQueryMeta &qmeta,
      ContextPointer &context) const override {
    return search_bf_by_p_keys_impl(query, p_keys, qmeta, 1, context);
  }

  //! Linear search by primary keys
  virtual int search_bf_by_p_keys_impl(
      const void *query, const std::vector<std::vector<uint64_t>> &p_keys,
      const IndexQueryMeta &qmeta, uint32_t count,
      ContextPointer &context) const override;

  //! Linear search by primary keys
  virtual int search_bf_by_p_keys_impl(
      const void *query, const uint32_t sparse_count,
      const uint32_t *sparse_indices, const void *sparse_query,
      const std::vector<std::vector<uint64_t>> &p_keys,
      const IndexQueryMeta &qmeta,
      ContextPointer &context) const override {
    return search_bf_by_p_keys_impl(query, &sparse_count, sparse_indices,
                                    sparse_query, p_keys, qmeta, 1, context);
  }

  //! Linear search by primary keys
  virtual int search_bf_by_p_keys_impl(
      const void * /*query*/, const uint32_t * /*sparse_count*/,
      const uint32_t * /*sparse_indices*/, const void * /*sparse_query*/,
      const std::vector<std::vector<uint64_t>> & /*p_keys*/,
      const IndexQueryMeta & /*qmeta*/, uint32_t /*count*/,
      ContextPointer & /*context*/) const override {
    return IndexError_NotImplemented;
  }

  //! Get vector by key
  virtual int get_vector(uint64_t key, Context::Pointer &context,
                         std::string &vector) const;

  //! Create a searcher context
  virtual ContextPointer create_context() const;

  //! Create a new iterator
  virtual IndexSearcher::Provider::Pointer create_provider(
      void) const override {
    return nullptr;
  }

  //! Retrieve statistics
  virtual const Stats &stats(void) const override {
    return stats_;
  }

  //! Retrieve meta of index
  virtual const IndexMeta &meta(void) const override {
    return meta_;
  }

  //! Retrieve params of index
  virtual const ailego::Params &params(void) const override {
    return params_;
  }

  virtual void print_debug_info() override;

 private:
  template <typename T, typename LabelT = uint32_t>
  int search_disk_index(const std::string &query_file,
                        const uint32_t num_nodes_to_cache,
                        const uint32_t recall_at, const uint32_t beamwidth);

  //! To share ctx across streamer/searcher, we need to update the context for
  //! current streamer/searcher
  int update_context(DiskAnnContext *ctx) const;

 private:
  enum State { STATE_INIT = 0, STATE_INITED = 1, STATE_LOADED = 2 };

  IndexMetric::Pointer measure_{};
  IndexMeta meta_{};
  ailego::Params params_{};

  uint32_t list_size_{200};
  uint32_t cache_nodes_num_{0};

  bool warm_up_{false};
  uint32_t beam_size_{2};

  DiskAnnIndex::Pointer diskann_index_{nullptr};
  DiskAnnSearcherEntity entity_{};

  uint32_t magic_{0U};

  Stats stats_;
  State state_{STATE_INIT};
};

}  // namespace core
}  // namespace zvec

