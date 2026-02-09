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
#include <zvec/core/framework/index_meta.h>
#include "diskann_context.h"
#include "diskann_pq_table.h"
#include "../cluster/multi_chunk_cluster.h"

namespace zvec {
namespace core {

class DiskAnnAlgorithm {
 public:
  typedef std::unique_ptr<DiskAnnAlgorithm> UPointer;

 public:
  DiskAnnAlgorithm(DiskAnnEntity &entity, uint32_t max_degree,
                   uint32_t max_train_sample_count, double train_sample_ratio);

 public:
  template <typename T>
  static int prepare_pq_train_data(
      const IndexMeta &meta, size_t num_train,
      std::string &train_data, bool use_zero_mean,
      std::vector<uint8_t> &centroid,
      std::shared_ptr<CompactIndexFeatures> &train_features);

  template <typename T>
  static int convert_pivot_data(const IndexMeta &meta,
                                uint32_t num_centers, uint32_t pq_chunk_num,
                                const std::vector<uint32_t> &chunk_dims,
                                const std::vector<uint32_t> &chunk_offsets,
                                IndexCluster::CentroidList &centroids,
                                std::vector<uint8_t> &full_pivot_data);

  int gen_random_sample(IndexHolder::Pointer holder,
                        const IndexMeta &meta,
                        std::string &sample_data, size_t &sample_size);

  int generate_quantized_data(IndexThreads::Pointer threads,
                              IndexHolder::Pointer holder,
                              const IndexMeta &meta,
                              // std::vector<uint8_t> &pq_full_pivot_data,
                              std::vector<uint8_t> &pq_centroid,
                              // std::vector<uint32_t> &pq_chunk_offsets,
                              std::vector<uint8_t> &block_compressed_data,
                              uint64_t num_pq_chunks);

  int generate_pq(IndexThreads::Pointer threads,
                  const IndexMeta &meta,
                  IndexHolder::Pointer holder, uint32_t num_pq_chunks,
                  bool use_zero_mean, std::vector<uint8_t> &centroid,
                  std::vector<uint8_t> &block_compressed_data);

  int train_quantized_data(IndexThreads::Pointer threads,
                           IndexHolder::Pointer holder,
                           const IndexMeta &meta,
                           std::vector<uint8_t> &pq_full_pivot_data,
                           std::vector<uint8_t> &pq_centroid,
                           std::vector<uint32_t> &pq_chunk_offsets,
                           uint64_t num_pq_chunks);

  int train_pq(IndexThreads::Pointer threads,
               const IndexMeta &meta,
               IndexHolder::Pointer holder, std::string &train_data,
               size_t num_train, uint32_t num_centers, uint32_t num_pq_chunks,
               uint32_t max_iterations, bool use_zero_mean,
               std::vector<uint8_t> &full_pivot_data,
               std::vector<uint8_t> &centroid,
               std::vector<uint32_t> &chunk_offsets);

 public:
  int add_node(diskann_id_t id, DiskAnnContext *ctx);
  int prune_node(diskann_id_t id, DiskAnnContext *ctx);

 private:
  int search_neighbor_and_prune(diskann_id_t id,
                                std::vector<diskann_id_t> &pruned_list,
                                DiskAnnContext *ctx);
  int iterate_to_fixed_point(diskann_id_t location,
                             const std::vector<diskann_id_t> &init_ids,
                             DiskAnnContext *ctx);
  int prune_neighbors(diskann_id_t id, std::vector<Neighbor> &pool,
                      std::vector<diskann_id_t> &pruned_list,
                      DiskAnnContext *ctx);
  int inter_insert(diskann_id_t id, std::vector<diskann_id_t> &pruned_list,
                   DiskAnnContext *ctx);
  int occlude_list(diskann_id_t id, std::vector<Neighbor> &pool,
                   std::vector<diskann_id_t> &result, DiskAnnContext *ctx);

  std::vector<diskann_id_t> get_init_ids(DiskAnnContext *ctx);

 private:
  static constexpr uint32_t kLockCnt{1U << 16};
  static constexpr uint32_t kLockMask{kLockCnt - 1U};

  static constexpr uint32_t compress_batch_size_{
      DiskAnnEntity::kDefaultCompressBatchSize};

  DiskAnnEntity &entity_;

  uint32_t max_degree_{DiskAnnEntity::kDefaultMaxDegree};
  uint32_t max_candidate_size_{DiskAnnEntity::kDefaultMaxOcclusionSize};
  uint32_t max_train_sample_count_{PQTable::kMaxTrainSampleCount};
  double train_sample_ratio_{PQTable::kTrainSampleRatio};

  std::vector<std::mutex> lock_pool_{};

  // pq cluster
  MultiChunkCluster chunk_cluster_;
  IndexCluster::CentroidList cluster_centroids_;

  float alpha_{DiskAnnEntity::kDefaultAlpha};
  bool saturate_graph_{true};
};

}  // namespace core
}  // namespace zvec