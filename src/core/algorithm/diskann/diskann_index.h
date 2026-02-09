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

#include <cstdint>
#include <zvec/core/framework/index_framework.h>
#include "diskann_context.h"
#include "diskann_file_reader.h"
#include "diskann_pq_table.h"
#include "diskann_searcher_entity.h"
#include "diskann_util.h"

namespace zvec {
namespace core {

class DiskAnnIndex {
 public:
  typedef std::shared_ptr<DiskAnnIndex> Pointer;

 public:
  DiskAnnIndex(const IndexMeta &meta);
  ~DiskAnnIndex();

 public:
  int init(DiskAnnSearcherEntity &entity);
  int load_cache_list(const std::vector<diskann_id_t> &node_list);

  void cache_bfs_levels(uint64_t num_nodes_to_cache,
                        std::vector<diskann_id_t> &node_list,
                        const bool shuffle = false);

  int cached_beam_search(DiskAnnContext *ctx);
  int cached_beam_search_by_group(DiskAnnContext *ctx);

  int cached_beam_search_in_mem(DiskAnnContext *ctx);

  int knn_search(DiskAnnContext *ctx);
  int linear_search(DiskAnnContext *ctx);
  int keys_search(const std::vector<diskann_key_t> &keys, DiskAnnContext *ctx);

  int get_vector(diskann_id_t id, IndexContext::Pointer &context,
                 std::string &vector);

  diskann_key_t get_key(diskann_id_t id) const;
  diskann_id_t get_id(diskann_key_t key) const;

  std::vector<bool> read_nodes(
      const std::vector<diskann_id_t> &node_ids,
      std::vector<void *> &coord_buffers,
      std::vector<std::pair<uint32_t, diskann_id_t *>> &nbr_buffers);

 protected:
  int use_medroids_data_as_centroids();

 private:
  DiskAnnSearcherEntity *entity_;

  IndexStorage::Pointer storage_{};
  IndexMeta meta_;

  uint32_t max_degree_{0};
  uint32_t node_per_sector_{0};
  uint32_t max_node_size_{0};
  float max_base_norm_{0.0f};
  uint64_t pq_chunk_num_{0};
  uint64_t disk_bytes_per_point_{0};
  uint64_t aligned_dim_{0};
  uint64_t index_segment_offset_{0};
  uint64_t sector_num_per_node_{0};

  float *centroid_data_{nullptr};

  diskann_id_t medoid_;
  std::vector<diskann_id_t> entrypints_;

  std::shared_ptr<LinuxAlignedFileReader> reader_{nullptr};

  PQTable::Pointer pq_table_;

  IOContext init_ctx_{0};

  diskann_id_t *neighbor_cache_buf_{nullptr};
  void *coord_cache_buf_{nullptr};
  uint64_t count_visited_nodes_{0};

  // tsl::robin_map<uint32_t, void *> coord_cache_;
  // tsl::robin_map<uint32_t, std::pair<uint32_t, uint32_t *>> neighbor_cache_;
  std::map<diskann_id_t, void *> coord_cache_;
  std::map<diskann_id_t, std::pair<uint32_t, diskann_id_t *>> neighbor_cache_;

  uint32_t beam_width_{2};
  uint32_t io_limit_{std::numeric_limits<uint32_t>::max()};

  uint64_t doc_cnt_{0};
};

}  // namespace core
}  // namespace zvec
