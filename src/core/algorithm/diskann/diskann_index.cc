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

#include "diskann_index.h"

#include <iostream>
#include <memory>
#include <set>

namespace zvec {
namespace core {

DiskAnnIndex::DiskAnnIndex(const IndexMeta &meta) {
  meta_ = meta;
}

DiskAnnIndex::~DiskAnnIndex() {
  if (centroid_data_) {
    free(centroid_data_);
  }

  if (neighbor_cache_buf_) {
    delete[] neighbor_cache_buf_;
  }
}

int DiskAnnIndex::init(DiskAnnSearcherEntity &entity) {
  entity_ = &entity;

  auto storage = entity.get_storage();
  auto vector_segment = entity.get_vector_segment();

  pq_table_ = entity.get_pq_table();

  index_segment_offset_ = vector_segment->data_offset();

  // reopen file with directo
  bool with_direct_io = true;
  if (with_direct_io) {
    reader_.reset(new LinuxAlignedFileReader());

    auto file_path = storage->file_path();
    reader_->open(file_path);

    storage->cleanup();
  } else {
    auto file = storage->file();
    reader_.reset(new LinuxAlignedFileReader(file->native_handle()));
  }

  int ret = setup_io_ctx(init_ctx_);
  if (ret != 0) {
    LOG_ERROR("setup io ctx error");
    return ret;
  }

  max_node_size_ = entity.max_node_size();
  disk_bytes_per_point_ = meta_.element_size();

  node_per_sector_ = entity.node_per_sector();
  aligned_dim_ = meta_.dimension();

  pq_chunk_num_ = entity.pq_chunk_num();

  medoid_ = entity.medoid();

  entrypints_.push_back(medoid_);
  auto &entrypoints = entity.entrypoints();
  for (size_t i = 0; i < entrypoints.size(); ++i) {
    entrypints_.push_back(entrypoints[i]);
  }

  doc_cnt_ = entity.doc_cnt();

  max_degree_ = entity.max_degree();

  sector_num_per_node_ =
      DiskAnnUtil::div_round_up(max_node_size_, DiskAnnUtil::kSectorSize);
  if (beam_width_ > sector_num_per_node_ * DiskAnnUtil::kMaxSectorReadNum) {
    LOG_ERROR("Beamwidth can not be higher than kMaxSectorReadNum");

    return IndexError_InvalidArgument;
  }

  DiskAnnUtil::alloc_aligned((void **)(&centroid_data_),
                             entrypints_.size() * aligned_dim_ * sizeof(float),
                             32);

  use_medroids_data_as_centroids();

  return 0;
}

int DiskAnnIndex::use_medroids_data_as_centroids() {
  LOG_INFO("Loading centroid data from medoid vector data");

  std::vector<diskann_id_t> nodes_to_read;
  std::vector<void *> medoid_bufs;
  std::vector<std::pair<uint32_t, diskann_id_t *>> neighbor_bufs;

  std::vector<float> centroid_buffer;

  size_t dim = meta_.dimension();
  centroid_buffer.resize(dim);

  nodes_to_read.push_back(medoid_);
  medoid_bufs.push_back(&(centroid_buffer[0]));
  neighbor_bufs.emplace_back(0, nullptr);

  auto read_status = read_nodes(nodes_to_read, medoid_bufs, neighbor_bufs);

  if (read_status[0] == true) {
    for (uint32_t i = 0; i < dim; i++) centroid_data_[i] = centroid_buffer[i];
  } else {
    LOG_ERROR("Failed to read medoid");
    return IndexError_Runtime;
  }

  return 0;
}

diskann_key_t DiskAnnIndex::get_key(diskann_id_t id) const {
  return entity_->get_key(id);
}

diskann_id_t DiskAnnIndex::get_id(diskann_key_t key) const {
  return entity_->get_id(key);
}

std::vector<bool> DiskAnnIndex::read_nodes(
    const std::vector<diskann_id_t> &node_ids,
    std::vector<void *> &coord_buffers,
    std::vector<std::pair<uint32_t, diskann_id_t *>> &neighbor_buffers) {
  std::vector<AlignedRead> read_reqs;
  std::vector<bool> retval(node_ids.size(), true);

  uint8_t *buf = nullptr;
  auto sector_num =
      node_per_sector_ > 0
          ? 1
          : DiskAnnUtil::div_round_up(max_node_size_, DiskAnnUtil::kSectorSize);
  DiskAnnUtil::alloc_aligned(
      (void **)&buf, node_ids.size() * sector_num * DiskAnnUtil::kSectorSize,
      DiskAnnUtil::kSectorSize);

  // create read requests
  for (size_t i = 0; i < node_ids.size(); ++i) {
    auto node_id = node_ids[i];

    AlignedRead read;
    read.len = sector_num * DiskAnnUtil::kSectorSize;
    read.buf = buf + i * sector_num * DiskAnnUtil::kSectorSize;
    read.offset =
        index_segment_offset_ +
        DiskAnnUtil::get_node_sector(node_per_sector_, max_node_size_,
                                     DiskAnnUtil::kSectorSize, node_id) *
            DiskAnnUtil::kSectorSize;
    read_reqs.push_back(read);
  }

  reader_->read(read_reqs, init_ctx_);

  // copy reads into buffers
  for (uint32_t i = 0; i < read_reqs.size(); i++) {
    uint8_t *node_buf =
        DiskAnnUtil::offset_to_node(node_per_sector_, max_node_size_,
                                    (uint8_t *)read_reqs[i].buf, node_ids[i]);

    if (coord_buffers[i] != nullptr) {
      void *node_coords = node_buf;
      memcpy(coord_buffers[i], node_coords, disk_bytes_per_point_);
    }

    if (neighbor_buffers[i].second != nullptr) {
      uint32_t *node_neighbor =
          DiskAnnUtil::offset_to_node_neighbor(node_buf, meta_.element_size());
      uint32_t neighbor_num = *node_neighbor;

      neighbor_buffers[i].first = neighbor_num;
      memcpy(neighbor_buffers[i].second, node_neighbor + 1,
             neighbor_num * sizeof(diskann_id_t));
    }
  }

  DiskAnnUtil::free_aligned(buf);

  return retval;
}

int DiskAnnIndex::load_cache_list(const std::vector<diskann_id_t> &node_list) {
  LOG_INFO("Loading the cache list into memory");

  size_t num_cached_nodes = node_list.size();

  // Allocate space for neighborhood cache
  neighbor_cache_buf_ = new diskann_id_t[num_cached_nodes * (max_degree_ + 1)];
  memset(neighbor_cache_buf_, 0, num_cached_nodes * (max_degree_ + 1));

  // Allocate space for coordinate cache
  size_t coord_cache_buf_len = num_cached_nodes * aligned_dim_;
  DiskAnnUtil::alloc_aligned((void **)&coord_cache_buf_,
                             coord_cache_buf_len * meta_.unit_size(),
                             8 * meta_.unit_size());

  memset(coord_cache_buf_, 0, coord_cache_buf_len * meta_.unit_size());

  constexpr size_t BLOCK_SIZE = 8;
  size_t num_blocks = DiskAnnUtil::div_round_up(num_cached_nodes, BLOCK_SIZE);
  for (size_t block = 0; block < num_blocks; block++) {
    size_t start_idx = block * BLOCK_SIZE;
    size_t end_idx = std::min(num_cached_nodes, (block + 1) * BLOCK_SIZE);

    // Copy offset into buffers to read into
    std::vector<diskann_id_t> nodes_to_read;
    std::vector<void *> coord_buffers;
    std::vector<std::pair<uint32_t, diskann_id_t *>> neighbor_buffers;
    for (size_t node_idx = start_idx; node_idx < end_idx; node_idx++) {
      nodes_to_read.push_back(node_list[node_idx]);
      coord_buffers.push_back(reinterpret_cast<uint8_t *>(coord_cache_buf_) +
                              node_idx * meta_.element_size());
      neighbor_buffers.emplace_back(
          0, neighbor_cache_buf_ + node_idx * (max_degree_ + 1));
    }

    // issue the reads
    auto read_status =
        read_nodes(nodes_to_read, coord_buffers, neighbor_buffers);

    // check for success and insert into the cache.
    for (size_t i = 0; i < read_status.size(); i++) {
      if (read_status[i] == true) {
        coord_cache_.insert(std::make_pair(nodes_to_read[i], coord_buffers[i]));
        neighbor_cache_.insert(
            std::make_pair(nodes_to_read[i], neighbor_buffers[i]));
      }
    }
  }

  LOG_INFO("Load Cache List Done");

  return 0;
}

void DiskAnnIndex::cache_bfs_levels(uint64_t num_nodes_to_cache,
                                    std::vector<diskann_id_t> &node_list,
                                    const bool shuffle) {
  std::random_device rng;
  std::mt19937 urng(rng());

  std::set<diskann_id_t> node_set;

  size_t tenp_cnt = static_cast<uint64_t>(std::round(doc_cnt_ * 0.1));
  if (num_nodes_to_cache > tenp_cnt) {
    LOG_WARN(
        "Reducing nodes to cache from: %zu, to: (10 percent of total nodes: "
        "%zu)",
        num_nodes_to_cache, tenp_cnt);

    num_nodes_to_cache = tenp_cnt == 0 ? 1 : tenp_cnt;
  }

  LOG_INFO("Begin to cache %ld Nodes", num_nodes_to_cache);

  std::unique_ptr<std::set<diskann_id_t>> cur_level(
      new std::set<diskann_id_t>());
  std::unique_ptr<std::set<diskann_id_t>> prev_level(
      new std::set<diskann_id_t>());

  for (uint64_t miter = 0;
       miter < entrypints_.size() && cur_level->size() < num_nodes_to_cache;
       miter++) {
    cur_level->insert(entrypints_[miter]);
  }

  uint64_t level = 1;
  uint64_t prev_node_set_size = 0;
  while ((node_set.size() + cur_level->size() < num_nodes_to_cache) &&
         cur_level->size() != 0) {
    // swap prev_level and cur_level
    std::swap(prev_level, cur_level);

    // clear cur_level
    cur_level->clear();

    std::vector<diskann_id_t> nodes_to_expand;

    for (const diskann_id_t &id : *prev_level) {
      if (node_set.find(id) != node_set.end()) {
        continue;
      }
      node_set.insert(id);
      nodes_to_expand.push_back(id);
    }

    if (shuffle)
      std::shuffle(nodes_to_expand.begin(), nodes_to_expand.end(), urng);
    else
      std::sort(nodes_to_expand.begin(), nodes_to_expand.end());

    bool finish_flag = false;

    constexpr uint64_t BLOCK_SIZE = 1024;
    uint64_t nblocks =
        DiskAnnUtil::div_round_up(nodes_to_expand.size(), BLOCK_SIZE);
    for (size_t block = 0; block < nblocks && !finish_flag; block++) {
      size_t start = block * BLOCK_SIZE;
      size_t end = std::min((block + 1) * BLOCK_SIZE, nodes_to_expand.size());

      std::vector<diskann_id_t> nodes_to_read;
      std::vector<void *> coord_buffers(end - start, nullptr);
      std::vector<std::pair<uint32_t, diskann_id_t *>> neighbor_buffers;

      for (size_t cur_pt = start; cur_pt < end; cur_pt++) {
        nodes_to_read.push_back(nodes_to_expand[cur_pt]);
        neighbor_buffers.emplace_back(0, new diskann_id_t[max_degree_ + 1]);
      }

      auto read_status =
          read_nodes(nodes_to_read, coord_buffers, neighbor_buffers);

      for (uint32_t i = 0; i < read_status.size(); i++) {
        if (read_status[i] == false) {
          continue;
        } else {
          uint32_t neighbor_num = neighbor_buffers[i].first;
          diskann_id_t *neighbors = neighbor_buffers[i].second;

          // explore next level
          for (uint32_t j = 0; j < neighbor_num && !finish_flag; j++) {
            if (node_set.find(neighbors[j]) == node_set.end()) {
              cur_level->insert(neighbors[j]);
            }
            if (cur_level->size() + node_set.size() >= num_nodes_to_cache) {
              finish_flag = true;
            }
          }
        }
        delete[] neighbor_buffers[i].second;
      }
    }

    size_t total_size = node_set.size();

    LOG_INFO("Level: %lu, Cached Size: %zu, Total Cached Size: %zu", level,
             total_size - prev_node_set_size, total_size);

    prev_node_set_size = total_size;
    level++;
  }

  ailego_assert(node_set.size() + cur_level->size() == num_nodes_to_cache ||
                cur_level->size() == 0);

  node_list.clear();
  node_list.reserve(node_set.size() + cur_level->size());

  for (auto node : node_set) {
    node_list.push_back(node);
  }

  for (auto node : *cur_level) {
    node_list.push_back(node);
  }

  size_t total_size = node_list.size();
  LOG_INFO("Level: %lu, Cached Size: %zu, Total Cached Size: %zu", level,
           total_size - prev_node_set_size, total_size);

  return;
}

int DiskAnnIndex::linear_search(DiskAnnContext *ctx) {
  auto &stats = ctx->query_stats();
  auto &dc = ctx->dist_calculator();
  auto &topk_heap = ctx->topk_heap();

  topk_heap.clear();

  IOContext &io_ctx = ctx->io_ctx();
  void *aligned_query_raw = ctx->query();

  void *data_buf = reinterpret_cast<void *>(ctx->coord_buffer());

  uint8_t *sector_buffer = reinterpret_cast<uint8_t *>(ctx->sector_buffer());

  const uint64_t sector_num_per_node =
      node_per_sector_ > 0
          ? 1
          : DiskAnnUtil::div_round_up(max_node_size_, DiskAnnUtil::kSectorSize);

  ailego::ElapsedTime io_timer;
  ailego::ElapsedTime query_timer;
  ailego::ElapsedTime cpu_timer;

  uint32_t num_ios = 0;

  std::vector<diskann_id_t> frontier;
  frontier.reserve(2 * beam_width_);

  std::vector<std::pair<diskann_id_t, uint8_t *>> frontier_neighbors;
  frontier_neighbors.reserve(2 * beam_width_);

  std::vector<AlignedRead> frontier_read_reqs;
  frontier_read_reqs.reserve(2 * beam_width_);

  std::vector<std::pair<diskann_id_t, std::pair<uint32_t, diskann_id_t *>>>
      cached_neighbors;
  cached_neighbors.reserve(2 * beam_width_);

  uint64_t sector_buffer_idx = 0;

  diskann_id_t id = 0;
  while (id < doc_cnt_) {
    while (frontier.size() < beam_width_) {
      if (!ctx->filter().is_valid() || !ctx->filter()(get_key(id))) {
        auto iter = neighbor_cache_.find(id);
        if (iter != neighbor_cache_.end()) {
          cached_neighbors.push_back(std::make_pair(id, iter->second));
          stats.cache_hits++;
        } else {
          frontier.push_back(id);
        }
      }

      id++;
      if (id >= doc_cnt_) {
        break;
      }
    }

    if (!frontier.empty()) {
      for (uint64_t i = 0; i < frontier.size(); i++) {
        diskann_id_t cur_id = frontier[i];

        std::pair<diskann_id_t, uint8_t *> frontier_neighbor;
        frontier_neighbor.first = cur_id;
        frontier_neighbor.second = sector_buffer + sector_num_per_node *
                                                       sector_buffer_idx *
                                                       DiskAnnUtil::kSectorSize;
        frontier_neighbors.push_back(frontier_neighbor);

        sector_buffer_idx++;

        frontier_read_reqs.emplace_back(
            index_segment_offset_ +
                DiskAnnUtil::get_node_sector(node_per_sector_, max_node_size_,
                                             DiskAnnUtil::kSectorSize, cur_id) *
                    DiskAnnUtil::kSectorSize,
            sector_num_per_node * DiskAnnUtil::kSectorSize,
            frontier_neighbor.second);

        stats.disk_page_reads++;
        stats.io_num++;
        num_ios++;
      }

      io_timer.reset();

      reader_->read(frontier_read_reqs, io_ctx);
      stats.io_us += io_timer.micro_seconds();
    }

    // process cached neighbors
    for (auto &cached_neighbor : cached_neighbors) {
      auto global_cache_iter = coord_cache_.find(cached_neighbor.first);
      void *node_fp_coords_copy = global_cache_iter->second;

      float cur_expanded_dist = dc.dist(aligned_query_raw, node_fp_coords_copy);

      std::string vec_value;
      vec_value.resize(meta_.element_size());
      ::memcpy(&(vec_value[0]), node_fp_coords_copy, meta_.element_size());

      topk_heap.emplace(cached_neighbor.first,
                        VectorInfo(cur_expanded_dist, vec_value));
    }

    for (auto &frontier_neighbor : frontier_neighbors) {
      uint8_t *node_disk_buf = DiskAnnUtil::offset_to_node(
          node_per_sector_, max_node_size_, frontier_neighbor.second,
          frontier_neighbor.first);

      void *node_fp_coords = node_disk_buf;
      memcpy(data_buf, node_fp_coords, disk_bytes_per_point_);

      // std::cout << "id: " << frontier_neighbor.first << ", query data: " <<
      // ailego::FloatHelper::ToFP32(*((uint16_t *)aligned_query_raw))
      //         << ", data: " << ailego::FloatHelper::ToFP32(*(uint16_t
      //         *)data_buf) << std::endl;

      float cur_expanded_dist = dc.dist(aligned_query_raw, data_buf);

      std::string vec_value;
      vec_value.resize(meta_.element_size());
      ::memcpy(&(vec_value[0]), data_buf, meta_.element_size());

      topk_heap.emplace(frontier_neighbor.first,
                        VectorInfo(cur_expanded_dist, vec_value));

      stats.cpu_us += cpu_timer.micro_seconds();
    }

    frontier.clear();
    frontier_neighbors.clear();
    frontier_read_reqs.clear();
    cached_neighbors.clear();
    sector_buffer_idx = 0;
  }

  stats.total_us += query_timer.micro_seconds();

  return 0;
}

int DiskAnnIndex::keys_search(const std::vector<uint64_t> &keys,
                              DiskAnnContext *ctx) {
  auto &stats = ctx->query_stats();
  auto &dc = ctx->dist_calculator();
  auto &topk_heap = ctx->topk_heap();

  topk_heap.clear();

  IOContext &io_ctx = ctx->io_ctx();
  void *aligned_query_raw = ctx->query();

  void *data_buf = reinterpret_cast<void *>(ctx->coord_buffer());

  uint8_t *sector_buffer = reinterpret_cast<uint8_t *>(ctx->sector_buffer());

  const uint64_t sector_num_per_node =
      node_per_sector_ > 0
          ? 1
          : DiskAnnUtil::div_round_up(max_node_size_, DiskAnnUtil::kSectorSize);

  ailego::ElapsedTime query_timer;
  ailego::ElapsedTime io_timer;
  ailego::ElapsedTime cpu_timer;

  uint32_t num_ios = 0;

  std::vector<diskann_id_t> frontier;
  frontier.reserve(2 * beam_width_);

  std::vector<std::pair<uint32_t, uint8_t *>> frontier_neighbors;
  frontier_neighbors.reserve(2 * beam_width_);

  std::vector<AlignedRead> frontier_read_reqs;
  frontier_read_reqs.reserve(2 * beam_width_);

  std::vector<std::pair<diskann_id_t, std::pair<uint32_t, diskann_id_t *>>>
      cached_neighbors;
  cached_neighbors.reserve(2 * beam_width_);

  uint64_t sector_buffer_idx = 0;

  size_t idx = 0;
  while (idx < keys.size()) {
    while (frontier.size() < beam_width_) {
      if (!ctx->filter().is_valid() || !ctx->filter()(keys[idx])) {
        diskann_id_t id = get_id(keys[idx]);

        auto iter = neighbor_cache_.find(id);
        if (iter != neighbor_cache_.end()) {
          cached_neighbors.push_back(std::make_pair(id, iter->second));
          stats.cache_hits++;
        } else {
          frontier.push_back(id);
        }
      }

      idx++;
      if (idx >= keys.size()) {
        break;
      }
    }

    if (!frontier.empty()) {
      for (uint64_t i = 0; i < frontier.size(); i++) {
        diskann_id_t cur_id = frontier[i];

        std::pair<diskann_id_t, uint8_t *> frontier_neighbor;
        frontier_neighbor.first = cur_id;
        frontier_neighbor.second = sector_buffer + sector_num_per_node *
                                                       sector_buffer_idx *
                                                       DiskAnnUtil::kSectorSize;
        frontier_neighbors.push_back(frontier_neighbor);

        sector_buffer_idx++;

        frontier_read_reqs.emplace_back(
            index_segment_offset_ +
                DiskAnnUtil::get_node_sector(node_per_sector_, max_node_size_,
                                             DiskAnnUtil::kSectorSize, cur_id) *
                    DiskAnnUtil::kSectorSize,
            sector_num_per_node * DiskAnnUtil::kSectorSize,
            frontier_neighbor.second);

        stats.disk_page_reads++;
        stats.io_num++;
        num_ios++;
      }

      io_timer.reset();

      reader_->read(frontier_read_reqs, io_ctx);
      stats.io_us += io_timer.micro_seconds();
    }

    // process cached neighbors
    for (auto &cached_neighbor : cached_neighbors) {
      auto global_cache_iter = coord_cache_.find(cached_neighbor.first);
      void *node_fp_coords_copy = global_cache_iter->second;

      float cur_expanded_dist = dc.dist(aligned_query_raw, node_fp_coords_copy);

      std::string vec_value;
      vec_value.resize(meta_.element_size());
      ::memcpy(&(vec_value[0]), node_fp_coords_copy, meta_.element_size());

      topk_heap.emplace(cached_neighbor.first,
                        VectorInfo(cur_expanded_dist, vec_value));
    }

    for (auto &frontier_neighbor : frontier_neighbors) {
      uint8_t *node_disk_buf = DiskAnnUtil::offset_to_node(
          node_per_sector_, max_node_size_, frontier_neighbor.second,
          frontier_neighbor.first);

      void *node_fp_coords = node_disk_buf;
      memcpy(data_buf, node_fp_coords, disk_bytes_per_point_);

      float cur_expanded_dist = dc.dist(aligned_query_raw, data_buf);

      std::string vec_value;
      vec_value.resize(meta_.element_size());
      ::memcpy(&(vec_value[0]), data_buf, meta_.element_size());

      topk_heap.emplace(frontier_neighbor.first,
                        VectorInfo(cur_expanded_dist, vec_value));

      stats.cpu_us += cpu_timer.micro_seconds();
    }

    frontier.clear();
    frontier_neighbors.clear();
    frontier_read_reqs.clear();
    cached_neighbors.clear();
    sector_buffer_idx = 0;
  }

  stats.total_us += query_timer.micro_seconds();

  return 0;
}

int DiskAnnIndex::get_vector(diskann_id_t id,
                             IndexContext::Pointer &context,
                             std::string &vector) {
  DiskAnnContext *ctx = dynamic_cast<DiskAnnContext *>(context.get());

  auto &stats = ctx->query_stats();

  IOContext &io_ctx = ctx->io_ctx();
  // void *aligned_query_raw = ctx->query();
  // void *data_buf = reinterpret_cast<void *>(ctx->coord_buffer());

  uint8_t *sector_buffer = reinterpret_cast<uint8_t *>(ctx->sector_buffer());

  const uint64_t sector_num_per_node =
      node_per_sector_ > 0
          ? 1
          : DiskAnnUtil::div_round_up(max_node_size_, DiskAnnUtil::kSectorSize);

  ailego::ElapsedTime query_timer;
  ailego::ElapsedTime io_timer;
  ailego::ElapsedTime cpu_timer;

  uint32_t num_ios = 0;

  std::vector<diskann_id_t> frontier;
  frontier.reserve(2 * beam_width_);

  std::vector<std::pair<diskann_id_t, uint8_t *>> frontier_neighbors;
  frontier_neighbors.reserve(2 * beam_width_);

  std::vector<AlignedRead> frontier_read_reqs;
  frontier_read_reqs.reserve(2 * beam_width_);

  std::vector<std::pair<diskann_id_t, std::pair<uint32_t, diskann_id_t *>>>
      cached_neighbors;
  cached_neighbors.reserve(2 * beam_width_);

  auto iter = neighbor_cache_.find(id);
  if (iter != neighbor_cache_.end()) {
    void *node_fp_coords_copy = iter->second.second;

    vector.resize(meta_.element_size());
    ::memcpy(&(vector[0]), node_fp_coords_copy, meta_.element_size());

    return 0;
  } else {
    std::pair<diskann_id_t, uint8_t *> frontier_neighbor;
    frontier_neighbor.first = id;
    frontier_neighbor.second = sector_buffer;
    frontier_neighbors.push_back(frontier_neighbor);

    frontier_read_reqs.emplace_back(
        index_segment_offset_ +
            DiskAnnUtil::get_node_sector(node_per_sector_, max_node_size_,
                                         DiskAnnUtil::kSectorSize, id) *
                DiskAnnUtil::kSectorSize,
        sector_num_per_node * DiskAnnUtil::kSectorSize,
        frontier_neighbor.second);

    stats.disk_page_reads++;
    stats.io_num++;
    num_ios++;

    io_timer.reset();

    reader_->read(frontier_read_reqs, io_ctx);
    stats.io_us += io_timer.micro_seconds();

    uint8_t *node_disk_buf = DiskAnnUtil::offset_to_node(
        node_per_sector_, max_node_size_, frontier_neighbor.second, id);

    void *node_fp_coords = node_disk_buf;

    vector.resize(meta_.element_size());
    ::memcpy(&(vector[0]), node_fp_coords, meta_.element_size());

    stats.cpu_us += cpu_timer.micro_seconds();
  }

  return 0;
}

int DiskAnnIndex::knn_search(DiskAnnContext *ctx) {
  int ret = cached_beam_search(ctx);
  if (ret != 0) {
    return ret;
  }

  if (ctx->group_by_search()) {
    ret = cached_beam_search_by_group(ctx);
    if (ret != 0) {
      return ret;
    }
  }

  return 0;
}

int DiskAnnIndex::cached_beam_search(DiskAnnContext *ctx) {
  auto &stats = ctx->query_stats();
  auto &dc = ctx->dist_calculator();
  auto &topk_heap = ctx->topk_heap();
  auto &visit_filter = ctx->visit_filter();

  topk_heap.clear();

  IOContext &io_ctx = ctx->io_ctx();

  void *data_buf = reinterpret_cast<void *>(ctx->coord_buffer());
  uint8_t *sector_buffer = reinterpret_cast<uint8_t *>(ctx->sector_buffer());

  const uint64_t sector_num_per_node =
      node_per_sector_ > 0
          ? 1
          : DiskAnnUtil::div_round_up(max_node_size_, DiskAnnUtil::kSectorSize);

  pq_table_->preprocess_pq_dist_table(ctx->query_rotated(),
                                      ctx->pq_table_dist_buffer());

  ailego::ElapsedTime query_timer;
  ailego::ElapsedTime io_timer;
  ailego::ElapsedTime cpu_timer;

  NeighborPriorityQueue candidates;

  candidates.reserve(ctx->list_size());

  diskann_id_t best_medoid = 0;
  float best_dist = (std::numeric_limits<float>::max)();
  for (uint64_t cur_m = 0; cur_m < entrypints_.size(); cur_m++) {
    float cur_expanded_dist =
        dc.dist(ctx->query(), centroid_data_ + aligned_dim_ * cur_m);

    if (cur_expanded_dist < best_dist) {
      best_medoid = entrypints_[cur_m];
      best_dist = cur_expanded_dist;
    }
  }

  float dist;
  pq_table_->compute_dists(1, &best_medoid, pq_chunk_num_,
                           ctx->pq_table_dist_buffer(), ctx->pq_coord_buffer(),
                           &dist);
  candidates.insert(Neighbor(best_medoid, dist));
  visit_filter.set_visited(best_medoid);

  uint32_t cmps = 0;
  uint32_t hops = 0;
  uint32_t num_ios = 0;

  std::vector<diskann_id_t> frontier;
  frontier.reserve(2 * beam_width_);

  std::vector<std::pair<diskann_id_t, uint8_t *>> frontier_neighbors;
  frontier_neighbors.reserve(2 * beam_width_);

  std::vector<AlignedRead> frontier_read_reqs;
  frontier_read_reqs.reserve(2 * beam_width_);

  std::vector<std::pair<diskann_id_t, std::pair<uint32_t, diskann_id_t *>>>
      cached_neighbors;
  cached_neighbors.reserve(2 * beam_width_);

  while (candidates.has_unexpanded_node() && num_ios < io_limit_) {
    frontier.clear();
    frontier_neighbors.clear();
    frontier_read_reqs.clear();
    cached_neighbors.clear();

    uint64_t sector_buffer_idx = 0;

    uint32_t num_seen = 0;
    while (candidates.has_unexpanded_node() && frontier.size() < beam_width_ &&
           num_seen < beam_width_) {
      auto neighbor = candidates.closest_unexpanded();
      num_seen++;

#if 0
      if (neighbor.id == 1099069) {
        std::cout << "node id: 1099069" << std::endl;
      }

      if (neighbor.id == 267404) {
        std::cout << "node id: 267404" << std::endl;
      }
#endif
      auto iter = neighbor_cache_.find(neighbor.id);
      if (iter != neighbor_cache_.end()) {
        cached_neighbors.push_back(std::make_pair(neighbor.id, iter->second));
        stats.cache_hits++;
      } else {
        frontier.push_back(neighbor.id);
      }
    }

    if (!frontier.empty()) {
      stats.hop_num++;

      for (uint64_t i = 0; i < frontier.size(); i++) {
        diskann_id_t cur_id = frontier[i];

        std::pair<diskann_id_t, uint8_t *> frontier_neighbor;
        frontier_neighbor.first = cur_id;
        frontier_neighbor.second = sector_buffer + sector_num_per_node *
                                                       sector_buffer_idx *
                                                       DiskAnnUtil::kSectorSize;
        frontier_neighbors.push_back(frontier_neighbor);

        sector_buffer_idx++;

        frontier_read_reqs.emplace_back(
            index_segment_offset_ +
                DiskAnnUtil::get_node_sector(node_per_sector_, max_node_size_,
                                             DiskAnnUtil::kSectorSize, cur_id) *
                    DiskAnnUtil::kSectorSize,
            sector_num_per_node * DiskAnnUtil::kSectorSize,
            frontier_neighbor.second);

        stats.disk_page_reads++;
        stats.io_num++;
        num_ios++;
      }

      io_timer.reset();

      reader_->read(frontier_read_reqs, io_ctx);  // synchronous IO linux
      stats.io_us += io_timer.micro_seconds();
    }

    for (auto &cached_neighbor : cached_neighbors) {
      auto global_cache_iter = coord_cache_.find(cached_neighbor.first);
      void *node_fp_coords_copy = global_cache_iter->second;

      float cur_expanded_dist = dc.dist(ctx->query(), node_fp_coords_copy);

      if (!ctx->filter().is_valid() ||
          !ctx->filter()(get_key(cached_neighbor.first))) {
        std::string vec_value;
        vec_value.resize(meta_.element_size());
        ::memcpy(&(vec_value[0]), node_fp_coords_copy, meta_.element_size());

        topk_heap.emplace(cached_neighbor.first,
                          VectorInfo(cur_expanded_dist, vec_value));
      }

      uint32_t neighbor_num = cached_neighbor.second.first;
      diskann_id_t *node_neighbors = cached_neighbor.second.second;

      cpu_timer.reset();

      float distances[neighbor_num];
      pq_table_->compute_dists(neighbor_num, node_neighbors, pq_chunk_num_,
                               ctx->pq_table_dist_buffer(),
                               ctx->pq_coord_buffer(), distances);

      stats.dist_num += neighbor_num;
      stats.cpu_us += cpu_timer.micro_seconds();

      // process prefetched neighbor
      for (uint64_t m = 0; m < neighbor_num; ++m) {
        diskann_id_t id = node_neighbors[m];
        visit_filter.set_visited(id);
        cmps++;

        Neighbor nn(id, distances[m]);
        candidates.insert(nn);
      }
    }

    for (auto &frontier_neighbor : frontier_neighbors) {
      uint8_t *node_disk_buf = DiskAnnUtil::offset_to_node(
          node_per_sector_, max_node_size_, frontier_neighbor.second,
          frontier_neighbor.first);
      uint32_t *node_buf = DiskAnnUtil::offset_to_node_neighbor(
          node_disk_buf, meta_.element_size());
      uint32_t neighbor_num = *node_buf;

      void *node_fp_coords = node_disk_buf;
      memcpy(data_buf, node_fp_coords, disk_bytes_per_point_);

      float cur_expanded_dist = dc.dist(ctx->query(), data_buf);

      if (!ctx->filter().is_valid() ||
          !ctx->filter()(get_key(frontier_neighbor.first))) {
        std::string vec_value;
        vec_value.resize(meta_.element_size());
        ::memcpy(&(vec_value[0]), data_buf, meta_.element_size());

        topk_heap.emplace(frontier_neighbor.first,
                          VectorInfo(cur_expanded_dist, vec_value));
      }

      diskann_id_t *node_neighbors =
          reinterpret_cast<diskann_id_t *>(node_buf + 1);

      cpu_timer.reset();
      float distances[neighbor_num];
      pq_table_->compute_dists(neighbor_num, node_neighbors, pq_chunk_num_,
                               ctx->pq_table_dist_buffer(),
                               ctx->pq_coord_buffer(), distances);

      stats.dist_num += neighbor_num;
      stats.cpu_us += cpu_timer.micro_seconds();

      cpu_timer.reset();
      for (uint64_t m = 0; m < neighbor_num; ++m) {
        diskann_id_t id = node_neighbors[m];
        visit_filter.set_visited(id);
        cmps++;
        stats.dist_num++;

        Neighbor nn(id, distances[m]);
        candidates.insert(nn);
      }

      stats.cpu_us += cpu_timer.micro_seconds();
    }

    hops++;
  }

  stats.total_us += query_timer.micro_seconds();

  return 0;
}

int DiskAnnIndex::cached_beam_search_in_mem(DiskAnnContext *ctx) {
  auto &stats = ctx->query_stats();
  auto &dc = ctx->dist_calculator();
  auto &topk_heap = ctx->topk_heap();
  auto &visit_filter = ctx->visit_filter();

  topk_heap.clear();

  IOContext &io_ctx = ctx->io_ctx();

  void *data_buf = reinterpret_cast<void *>(ctx->coord_buffer());

  uint8_t *sector_buffer = reinterpret_cast<uint8_t *>(ctx->sector_buffer());

  const uint64_t sector_num_per_node =
      node_per_sector_ > 0
          ? 1
          : DiskAnnUtil::div_round_up(max_node_size_, DiskAnnUtil::kSectorSize);

  pq_table_->preprocess_pq_dist_table(ctx->query_rotated(),
                                      ctx->pq_table_dist_buffer());

  ailego::ElapsedTime query_timer;
  ailego::ElapsedTime io_timer;
  ailego::ElapsedTime cpu_timer;

  NeighborPriorityQueue candidates;

  candidates.reserve(ctx->list_size());

  diskann_id_t best_medoid = 0;
  float best_dist = (std::numeric_limits<float>::max)();
  for (uint64_t cur_m = 0; cur_m < entrypints_.size(); cur_m++) {
    float cur_expanded_dist =
        dc.dist(ctx->query(), centroid_data_ + aligned_dim_ * cur_m);

    if (cur_expanded_dist < best_dist) {
      best_medoid = entrypints_[cur_m];
      best_dist = cur_expanded_dist;
    }
  }

  float dist;
  pq_table_->compute_dists(1, &best_medoid, pq_chunk_num_,
                           ctx->pq_table_dist_buffer(), ctx->pq_coord_buffer(),
                           &dist);

  candidates.insert(Neighbor(best_medoid, dist));
  visit_filter.set_visited(best_medoid);

  uint32_t cmps = 0;
  uint32_t hops = 0;
  uint32_t num_ios = 0;

  std::vector<diskann_id_t> frontier;
  frontier.reserve(2 * beam_width_);
  std::vector<std::pair<diskann_id_t, uint8_t *>> frontier_neighbors;
  frontier_neighbors.reserve(2 * beam_width_);
  std::vector<AlignedRead> frontier_read_reqs;
  frontier_read_reqs.reserve(2 * beam_width_);
  std::vector<std::pair<diskann_id_t, std::pair<uint32_t, diskann_id_t *>>>
      cached_neighbors;
  cached_neighbors.reserve(2 * beam_width_);

  while (candidates.has_unexpanded_node() && num_ios < io_limit_) {
    frontier.clear();
    frontier_neighbors.clear();
    frontier_read_reqs.clear();
    cached_neighbors.clear();

    uint64_t sector_buffer_idx = 0;

    uint32_t num_seen = 0;

    while (candidates.has_unexpanded_node() && frontier.size() < beam_width_ &&
           num_seen < beam_width_) {
      auto neighbor = candidates.closest_unexpanded();
      num_seen++;

      auto iter = neighbor_cache_.find(neighbor.id);
      if (iter != neighbor_cache_.end()) {
        cached_neighbors.push_back(std::make_pair(neighbor.id, iter->second));
        stats.cache_hits++;
      } else {
        frontier.push_back(neighbor.id);
      }
    }

    if (!frontier.empty()) {
      stats.hop_num++;

      for (uint64_t i = 0; i < frontier.size(); i++) {
        diskann_id_t cur_id = frontier[i];

        std::pair<diskann_id_t, uint8_t *> frontier_neighbor;
        frontier_neighbor.first = cur_id;
        frontier_neighbor.second = sector_buffer + sector_num_per_node *
                                                       sector_buffer_idx *
                                                       DiskAnnUtil::kSectorSize;
        frontier_neighbors.push_back(frontier_neighbor);

        sector_buffer_idx++;

        frontier_read_reqs.emplace_back(
            index_segment_offset_ +
                DiskAnnUtil::get_node_sector(node_per_sector_, max_node_size_,
                                             DiskAnnUtil::kSectorSize, cur_id) *
                    DiskAnnUtil::kSectorSize,
            sector_num_per_node * DiskAnnUtil::kSectorSize,
            frontier_neighbor.second);

        stats.disk_page_reads++;
        stats.io_num++;
        num_ios++;
      }

      io_timer.reset();

      reader_->read(frontier_read_reqs, io_ctx);  // synchronous IO linux
      stats.io_us += io_timer.micro_seconds();
    }

    for (auto &cached_neighbor : cached_neighbors) {
      auto global_cache_iter = coord_cache_.find(cached_neighbor.first);
      void *node_fp_coords_copy = global_cache_iter->second;

      float cur_expanded_dist = dc.dist(ctx->query(), node_fp_coords_copy);

      if (!ctx->filter().is_valid() ||
          !ctx->filter()(get_key(cached_neighbor.first))) {
        std::string vec_value;
        vec_value.resize(meta_.element_size());
        ::memcpy(&(vec_value[0]), node_fp_coords_copy, meta_.element_size());

        topk_heap.emplace(cached_neighbor.first,
                          VectorInfo(cur_expanded_dist, vec_value));
      }

      uint32_t neighbor_num = cached_neighbor.second.first;
      diskann_id_t *node_neighbors = cached_neighbor.second.second;

      cpu_timer.reset();

      float distances[neighbor_num];
      pq_table_->compute_dists(neighbor_num, node_neighbors, pq_chunk_num_,
                               ctx->pq_table_dist_buffer(),
                               ctx->pq_coord_buffer(), distances);

      stats.dist_num += neighbor_num;
      stats.cpu_us += cpu_timer.micro_seconds();

      for (uint64_t m = 0; m < neighbor_num; ++m) {
        diskann_id_t id = node_neighbors[m];
        visit_filter.set_visited(id);
        cmps++;

        Neighbor nn(id, distances[m]);
        candidates.insert(nn);
      }
    }

    for (auto &frontier_neighbor : frontier_neighbors) {
      uint8_t *node_disk_buf = DiskAnnUtil::offset_to_node(
          node_per_sector_, max_node_size_, frontier_neighbor.second,
          frontier_neighbor.first);
      uint32_t *node_buf = DiskAnnUtil::offset_to_node_neighbor(
          node_disk_buf, meta_.element_size());
      uint32_t neighbor_num = *node_buf;

      void *node_fp_coords = node_disk_buf;
      memcpy(data_buf, node_fp_coords, disk_bytes_per_point_);

      float cur_expanded_dist = dc.dist(ctx->query(), data_buf);

      if (!ctx->filter().is_valid() ||
          !ctx->filter()(get_key(frontier_neighbor.first))) {
        std::string vec_value;
        vec_value.resize(meta_.element_size());
        ::memcpy(&(vec_value[0]), data_buf, meta_.element_size());

        topk_heap.emplace(frontier_neighbor.first,
                          VectorInfo(cur_expanded_dist, vec_value));
      }

      diskann_id_t *node_neighbors =
          reinterpret_cast<diskann_id_t *>(node_buf + 1);

      cpu_timer.reset();

      float distances[neighbor_num];
      pq_table_->compute_dists(neighbor_num, node_neighbors, pq_chunk_num_,
                               ctx->pq_table_dist_buffer(),
                               ctx->pq_coord_buffer(), distances);

      stats.dist_num += neighbor_num;
      stats.cpu_us += cpu_timer.micro_seconds();

      cpu_timer.reset();
      for (uint64_t m = 0; m < neighbor_num; ++m) {
        diskann_id_t id = node_neighbors[m];
        visit_filter.set_visited(id);
        cmps++;
        stats.dist_num++;

        Neighbor nn(id, distances[m]);
        candidates.insert(nn);
      }

      stats.cpu_us += cpu_timer.micro_seconds();
    }

    hops++;
  }

  stats.total_us += query_timer.micro_seconds();

  return 0;
}

int DiskAnnIndex::cached_beam_search_by_group(DiskAnnContext *ctx) {
  if (!ctx->group_by().is_valid()) {
    return 0;
  }

  std::function<std::string(diskann_id_t)> group_by = [&](diskann_id_t id) {
    return ctx->group_by()(get_key(id));
  };

  // devide into groups
  auto &topk_heap = ctx->topk_heap();
  auto &visit_filter = ctx->visit_filter();

  std::map<std::string, TopkHeap> &group_topk_heaps = ctx->group_topk_heaps();

  for (uint32_t i = 0; i < topk_heap.size(); ++i) {
    diskann_id_t id = topk_heap[i].first;
    auto info = topk_heap[i].second;

    std::string group_id = group_by(id);

    auto &group_topk_heap = group_topk_heaps[group_id];
    if (group_topk_heap.empty()) {
      group_topk_heap.limit(ctx->group_topk());
    }

    topk_heap.emplace(id, info);
  }

  // stage 2, expand to reach group num as possible
  if (group_topk_heaps.size() < ctx->group_num()) {
    NeighborPriorityQueue candidates;

    candidates.reserve(ctx->list_size());

    for (uint32_t i = 0; i < topk_heap.size(); ++i) {
      diskann_id_t id = topk_heap[i].first;
      float score = topk_heap[i].second.dist_;

      visit_filter.set_visited(id);
      candidates.insert(Neighbor(id, score));
    }

    ailego::ElapsedTime io_timer;
    ailego::ElapsedTime query_timer;
    ailego::ElapsedTime cpu_timer;

    auto &stats = ctx->query_stats();
    auto &dc = ctx->dist_calculator();

    IOContext &io_ctx = ctx->io_ctx();

    void *data_buf = reinterpret_cast<void *>(ctx->coord_buffer());
    uint8_t *sector_buffer = reinterpret_cast<uint8_t *>(ctx->sector_buffer());

    const uint64_t sector_num_per_node =
        node_per_sector_ > 0 ? 1
                             : DiskAnnUtil::div_round_up(
                                   max_node_size_, DiskAnnUtil::kSectorSize);

    pq_table_->preprocess_pq_dist_table(ctx->query_rotated(),
                                        ctx->pq_table_dist_buffer());

    uint32_t cmps = 0;
    uint32_t hops = 0;
    uint32_t num_ios = 0;

    std::vector<diskann_id_t> frontier;
    frontier.reserve(2 * beam_width_);
    std::vector<std::pair<diskann_id_t, uint8_t *>> frontier_neighbors;
    frontier_neighbors.reserve(2 * beam_width_);
    std::vector<AlignedRead> frontier_read_reqs;
    frontier_read_reqs.reserve(2 * beam_width_);
    std::vector<std::pair<diskann_id_t, std::pair<uint32_t, diskann_id_t *>>>
        cached_neighbors;
    cached_neighbors.reserve(2 * beam_width_);

    uint64_t sector_buffer_idx;

    while (candidates.has_unexpanded_node() && num_ios < io_limit_) {
      frontier.clear();
      frontier_neighbors.clear();
      frontier_read_reqs.clear();
      cached_neighbors.clear();
      sector_buffer_idx = 0;

      uint32_t num_seen = 0;
      while (candidates.has_unexpanded_node() &&
             frontier.size() < beam_width_ && num_seen < beam_width_) {
        auto neighbor = candidates.closest_unexpanded();
        num_seen++;

        auto iter = neighbor_cache_.find(neighbor.id);
        if (iter != neighbor_cache_.end()) {
          cached_neighbors.push_back(std::make_pair(neighbor.id, iter->second));
          stats.cache_hits++;
        } else {
          frontier.push_back(neighbor.id);
        }
      }

      if (!frontier.empty()) {
        stats.hop_num++;

        for (uint64_t i = 0; i < frontier.size(); i++) {
          diskann_id_t cur_id = frontier[i];

          std::pair<diskann_id_t, uint8_t *> frontier_neighbor;
          frontier_neighbor.first = cur_id;
          frontier_neighbor.second =
              sector_buffer + sector_num_per_node * sector_buffer_idx *
                                  DiskAnnUtil::kSectorSize;
          frontier_neighbors.push_back(frontier_neighbor);

          sector_buffer_idx++;

          frontier_read_reqs.emplace_back(
              index_segment_offset_ + DiskAnnUtil::get_node_sector(
                                          node_per_sector_, max_node_size_,
                                          DiskAnnUtil::kSectorSize, cur_id) *
                                          DiskAnnUtil::kSectorSize,
              sector_num_per_node * DiskAnnUtil::kSectorSize,
              frontier_neighbor.second);

          stats.disk_page_reads++;
          stats.io_num++;
          num_ios++;
        }

        io_timer.reset();

        reader_->read(frontier_read_reqs, io_ctx);  // synchronous IO linux
        stats.io_us += io_timer.micro_seconds();
      }

      for (auto &cached_neighbor : cached_neighbors) {
        auto global_cache_iter = coord_cache_.find(cached_neighbor.first);
        void *node_fp_coords_copy = global_cache_iter->second;

        float cur_expanded_dist = dc.dist(ctx->query(), node_fp_coords_copy);

        if (!ctx->filter().is_valid() ||
            !ctx->filter()(get_key(cached_neighbor.first))) {
          std::string group_id = group_by(cached_neighbor.first);

          auto &group_topk_heap = group_topk_heaps[group_id];
          if (group_topk_heap.empty()) {
            group_topk_heap.limit(ctx->group_topk());
          }

          std::string vec_value;
          vec_value.resize(meta_.element_size());
          ::memcpy(&(vec_value[0]), node_fp_coords_copy, meta_.element_size());

          group_topk_heap.emplace_back(
              cached_neighbor.first, VectorInfo(cur_expanded_dist, vec_value));

          if (group_topk_heaps.size() >= ctx->group_num()) {
            break;
          }
        }

        uint64_t neighbor_num = cached_neighbor.second.first;
        diskann_id_t *node_neighbors = cached_neighbor.second.second;

        cpu_timer.reset();

        float distances[neighbor_num];
        pq_table_->compute_dists(neighbor_num, node_neighbors, pq_chunk_num_,
                                 ctx->pq_table_dist_buffer(),
                                 ctx->pq_coord_buffer(), distances);

        stats.dist_num += neighbor_num;
        stats.cpu_us += cpu_timer.micro_seconds();

        for (uint64_t m = 0; m < neighbor_num; ++m) {
          diskann_id_t id = node_neighbors[m];
          visit_filter.set_visited(id);
          cmps++;

          Neighbor nn(id, distances[m]);
          candidates.insert(nn);
        }
      }

      for (auto &frontier_neighbor : frontier_neighbors) {
        uint8_t *node_disk_buf = DiskAnnUtil::offset_to_node(
            node_per_sector_, max_node_size_, frontier_neighbor.second,
            frontier_neighbor.first);
        uint32_t *node_buf = DiskAnnUtil::offset_to_node_neighbor(
            node_disk_buf, meta_.element_size());
        uint32_t neighbor_num = *node_buf;

        void *node_fp_coords = node_disk_buf;
        memcpy(data_buf, node_fp_coords, disk_bytes_per_point_);

        float cur_expanded_dist = dc.dist(ctx->query(), data_buf);

        if (!ctx->filter().is_valid() ||
            !ctx->filter()(get_key(frontier_neighbor.first))) {
          std::string group_id = group_by(frontier_neighbor.first);

          auto &group_topk_heap = group_topk_heaps[group_id];
          if (group_topk_heap.empty()) {
            group_topk_heap.limit(ctx->group_topk());
          }

          std::string vec_value;
          vec_value.resize(meta_.element_size());
          ::memcpy(&(vec_value[0]), data_buf, meta_.element_size());

          group_topk_heap.emplace_back(
              frontier_neighbor.first,
              VectorInfo(cur_expanded_dist, vec_value));

          if (group_topk_heaps.size() >= ctx->group_num()) {
            break;
          }
        }

        cpu_timer.reset();

        float distances[neighbor_num];
        diskann_id_t *node_neighbors =
            reinterpret_cast<diskann_id_t *>(node_buf + 1);
        pq_table_->compute_dists(neighbor_num, node_neighbors, pq_chunk_num_,
                                 ctx->pq_table_dist_buffer(),
                                 ctx->pq_coord_buffer(), distances);

        stats.dist_num += neighbor_num;
        stats.cpu_us += cpu_timer.micro_seconds();

        cpu_timer.reset();
        for (uint64_t m = 0; m < neighbor_num; ++m) {
          diskann_id_t id = node_neighbors[m];
          visit_filter.set_visited(id);
          cmps++;
          stats.dist_num++;

          Neighbor nn(id, distances[m]);
          candidates.insert(nn);
        }

        stats.cpu_us += cpu_timer.micro_seconds();
      }

      hops++;
    }

    stats.total_us += query_timer.micro_seconds();
  }

  return 0;
}

}  // namespace core
}  // namespace zvec