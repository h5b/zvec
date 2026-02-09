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

#include <set>
#include "diskann_algorithm.h"
#include <zvec/core/framework/index_holder.h>
#include "diskann_util.h"

namespace zvec {
namespace core {

DiskAnnAlgorithm::DiskAnnAlgorithm(DiskAnnEntity &entity, uint32_t max_degree,
                                   uint32_t max_train_sample_count,
                                   double train_sample_ratio)
    : entity_(entity),
      max_degree_(max_degree),
      max_train_sample_count_{max_train_sample_count},
      train_sample_ratio_{train_sample_ratio},
      lock_pool_(kLockCnt) {}

std::vector<diskann_id_t> DiskAnnAlgorithm::get_init_ids(DiskAnnContext *ctx) {
  const auto &entity = ctx->get_entity();

  std::vector<diskann_id_t> init_ids;

  init_ids.emplace_back(entity.medoid());

  return init_ids;
}

int DiskAnnAlgorithm::add_node(diskann_id_t id, DiskAnnContext *ctx) {
  const void *vec = entity_.get_vector(id);

  ctx->reset_query(vec);

  std::vector<diskann_id_t> pruned_list;

#if 0
  if (id == 629003) {
    std::cout << "id: 629003" << std::endl;
  }

  if (id == 699848) {
    std::cout << "id: 699848" << std::endl;
  }

  if (id == 625133) {
    std::cout << "id: 625133" << std::endl;
  }

  if (id == 624227) {
    std::cout << "id: 624227" << std::endl;
  }

  if (id == 1048576) {
    std::cout << "id: 1048576" << std::endl;
  }
#endif

  int ret = search_neighbor_and_prune(id, pruned_list, ctx);
  if (ret != 0) {
    return ret;
  }

  uint32_t lock_idx = id & kLockMask;
  lock_pool_[lock_idx].lock();
  entity_.set_neighbors(id, pruned_list);
  lock_pool_[lock_idx].unlock();

  ret = inter_insert(id, pruned_list, ctx);

  return 0;
}

int DiskAnnAlgorithm::prune_node(diskann_id_t id, DiskAnnContext *ctx) {
  DistCalculator &dc = ctx->dist_calculator();

  auto neighbors = entity_.get_neighbors(id);

  if (neighbors.first > max_degree_) {
    std::set<diskann_id_t> dummy_visited;
    std::vector<Neighbor> dummy_pool(0);
    std::vector<diskann_id_t> new_out_neighbors;

    for (size_t i = 0; i < neighbors.first; ++i) {
      diskann_id_t node_id = (neighbors.second)[i];

      auto itr = dummy_visited.find(node_id);
      if (itr == dummy_visited.end() && node_id != id) {
        float dist = dc.dist(id, node_id);

        dummy_pool.emplace_back(Neighbor(node_id, dist));
        dummy_visited.insert(node_id);
      }
    }

    prune_neighbors(id, dummy_pool, new_out_neighbors, ctx);

    uint32_t lock_idx = id & kLockMask;
    lock_pool_[lock_idx].lock();
    entity_.set_neighbors(id, new_out_neighbors);
    lock_pool_[lock_idx].unlock();
  }

  return 0;
}

int DiskAnnAlgorithm::inter_insert(diskann_id_t id,
                                   std::vector<diskann_id_t> &pruned_list,
                                   DiskAnnContext *ctx) {
  DistCalculator &dc = ctx->dist_calculator();

  for (auto &des : pruned_list) {
    // if (id == 624227 && des == 592586) {
    //   std::cout << "hello" << std::endl;
    // }

    std::vector<diskann_id_t> new_neighbors;
    bool need_prune = false;

    uint32_t lock_idx = des & kLockMask;
    lock_pool_[lock_idx].lock();

    auto neighbors = entity_.get_neighbors(des);

    bool found = false;
    for (size_t i = 0; i < neighbors.first; ++i) {
      if ((neighbors.second)[i] == id) {
        found = true;
        break;
      }
    }

    if (!found) {
      if (neighbors.first <
          static_cast<uint64_t>(DiskAnnEntity::kDefaultGraphSlackFactor *
                                max_degree_)) {
        entity_.add_neighbor(des, id);
        need_prune = false;
      } else {
        new_neighbors.resize(neighbors.first + 1);
        memcpy(&new_neighbors[0], neighbors.second,
               sizeof(diskann_id_t) * neighbors.first);

        new_neighbors[neighbors.first] = id;

        need_prune = true;
      }
    }

    lock_pool_[lock_idx].unlock();

    if (need_prune) {
      std::set<diskann_id_t> new_visited;
      std::vector<Neighbor> new_pool(0);

      size_t reserve_size = static_cast<size_t>(std::ceil(
          1.05 * DiskAnnEntity::kDefaultGraphSlackFactor * max_degree_));

      new_pool.reserve(reserve_size);

      for (auto node_id : new_neighbors) {
        if (new_visited.find(node_id) == new_visited.end() && node_id != des) {
          float dist = dc.dist(des, node_id);
          new_pool.emplace_back(Neighbor(node_id, dist));
          new_visited.insert(node_id);
        }
      }

      std::vector<diskann_id_t> new_pruned_neighbors;
      prune_neighbors(des, new_pool, new_pruned_neighbors, ctx);

      lock_idx = des & kLockMask;
      lock_pool_[lock_idx].lock();
      entity_.set_neighbors(des, new_pruned_neighbors);
      lock_pool_[lock_idx].unlock();
    }
  }

  return 0;
}

int DiskAnnAlgorithm::iterate_to_fixed_point(
    diskann_id_t location, const std::vector<diskann_id_t> &init_ids,
    DiskAnnContext *ctx) {
  DistCalculator &dc = ctx->dist_calculator();
  std::vector<Neighbor> &expanded_nodes = ctx->expanded_nodes();
  NeighborPriorityQueue &best_list_nodes = ctx->best_list_nodes();
  VisitFilter &visit = ctx->visit_filter();

  best_list_nodes.reserve(ctx->list_size());

  for (auto id : init_ids) {
    const void *vec = entity_.get_vector(id);

    float distance = dc.dist(vec);

    Neighbor nn = Neighbor(id, distance);
    best_list_nodes.insert(nn);
  }

  uint32_t cmps = 0;

#if 0
  std::string expand_path = "";
#endif

  while (best_list_nodes.has_unexpanded_node()) {
    auto neighbor = best_list_nodes.closest_unexpanded();
    auto node_id = neighbor.id;

    expanded_nodes.emplace_back(neighbor);
#if 0
    expand_path += std::to_string(node_id) + ">";
#endif

    uint32_t lock_idx = node_id & kLockMask;

    lock_pool_[lock_idx].lock();
    auto neighbors = entity_.get_neighbors(node_id);

    std::vector<diskann_id_t> id_scratch;

    for (size_t i = 0; i < neighbors.first; ++i) {
      diskann_id_t neighbor_id = (neighbors.second)[i];

      if (!visit.visited(neighbor_id)) {
        id_scratch.push_back(neighbor_id);

        visit.set_visited(neighbor_id);
      }
    }
    lock_pool_[lock_idx].unlock();

    for (size_t i = 0; i < id_scratch.size(); ++i) {
      diskann_id_t id = id_scratch[i];

      const void *vec = entity_.get_vector(id);
      float dist = dc.dist(vec);

      best_list_nodes.insert(Neighbor(id, dist));
    }

    cmps += static_cast<uint32_t>(id_scratch.size());
  }

#if 0
  std::cout << "id: " << location << ", expand path: " << expand_path
            << std::endl;
#endif

  return 0;
}

int DiskAnnAlgorithm::occlude_list(diskann_id_t id, std::vector<Neighbor> &pool,
                                   std::vector<diskann_id_t> &result,
                                   DiskAnnContext *ctx) {
  if (pool.size() == 0) return 0;

  DistCalculator &dc = ctx->dist_calculator();

  ailego_assert(std::is_sorted(pool.begin(), pool.end()));
  ailego_assert(result.size() == 0);

  if (pool.size() > max_candidate_size_) {
    pool.resize(max_candidate_size_);
  }

  std::vector<float> &occlude_factor = ctx->occlude_factor();

  occlude_factor.clear();
  occlude_factor.insert(occlude_factor.end(), pool.size(), 0.0f);

  float cur_alpha = 1;
  while (cur_alpha <= alpha_ && result.size() < max_degree_) {
    for (auto iter = pool.begin();
         result.size() < max_degree_ && iter != pool.end(); ++iter) {
      if (occlude_factor[iter - pool.begin()] > cur_alpha) {
        continue;
      }

      occlude_factor[iter - pool.begin()] = std::numeric_limits<float>::max();

      if (iter->id != id) {
        result.push_back(iter->id);
      }

      for (auto iter2 = iter + 1; iter2 != pool.end(); iter2++) {
        auto t = iter2 - pool.begin();
        if (occlude_factor[t] > alpha_) {
          continue;
        }

        float djk = dc.dist(iter2->id, iter->id);

        if (true) {
          occlude_factor[t] =
              (djk == 0) ? std::numeric_limits<float>::max()
                         : std::max(occlude_factor[t], iter2->distance / djk);
        }
      }
    }
    cur_alpha *= 1.2f;
  }

  return 0;
}

int DiskAnnAlgorithm::prune_neighbors(diskann_id_t id,
                                      std::vector<Neighbor> &pool,
                                      std::vector<diskann_id_t> &pruned_list,
                                      DiskAnnContext *ctx) {
  if (pool.size() == 0) {
    pruned_list.clear();
    return 0;
  }

  std::sort(pool.begin(), pool.end());

  pruned_list.clear();
  pruned_list.reserve(max_degree_);

  occlude_list(id, pool, pruned_list, ctx);

  ailego_assert(pruned_list.size() <= max_degree_);

  if (saturate_graph_ && alpha_ > 1) {
    for (const auto &node : pool) {
      if (pruned_list.size() >= max_degree_) {
        break;
      }

      if ((std::find(pruned_list.begin(), pruned_list.end(), node.id) ==
           pruned_list.end()) &&
          node.id != id) {
        pruned_list.push_back(node.id);
      }
    }
  }

  return 0;
}

int DiskAnnAlgorithm::search_neighbor_and_prune(
    diskann_id_t id, std::vector<diskann_id_t> &pruned_list,
    DiskAnnContext *ctx) {
  const std::vector<diskann_id_t> init_ids = get_init_ids(ctx);

  // if (id == 629003) {
  //   std::cout << "id: 629003" << std::endl;
  // }

  int ret = iterate_to_fixed_point(id, init_ids, ctx);
  if (ret != 0) {
    return ret;
  }

  auto &pool = ctx->expanded_nodes();
  for (uint32_t i = 0; i < pool.size(); i++) {
    if (pool[i].id == id) {
      pool.erase(pool.begin() + i);
      i--;
    }
  }

  ret = prune_neighbors(id, pool, pruned_list, ctx);
  if (ret != 0) {
    return ret;
  }

  return 0;
}

int DiskAnnAlgorithm::gen_random_sample(IndexHolder::Pointer holder,
                                        const IndexMeta &meta,
                                        std::string &sample_data,
                                        size_t &sample_size) {
  double train_sample_ratio =
      max_train_sample_count_ < 1 ? max_train_sample_count_ : 1;

  uint32_t max_train_sample_count = train_sample_ratio * holder->count();
  max_train_sample_count = max_train_sample_count > max_train_sample_count_
                               ? max_train_sample_count_
                               : max_train_sample_count;

  std::vector<std::vector<uint8_t>> sample_vecs;

  // std::random_device rd;
  // uint32_t x = rd();
  uint32_t x = 456321;
  std::mt19937 gen(x);
  std::uniform_real_distribution<float> dist(0, 1);

  uint32_t vec_size = meta.element_size();

  auto iter = holder->create_iterator();
  if (!iter) {
    LOG_ERROR("Create iterator for holder failed");
    return IndexError_Runtime;
  }

  size_t sample_count = 0;
  while (iter->is_valid() && sample_count < max_train_sample_count) {
    float random = dist(gen);

    if (random < train_sample_ratio) {
      const void *vec = iter->data();

      std::vector<uint8_t> temp_vec;
      temp_vec.resize(vec_size);

      std::memcpy(reinterpret_cast<uint8_t *>(&temp_vec[0]), vec, vec_size);

      sample_vecs.push_back(std::move(temp_vec));

      sample_count++;
    }

    iter->next();
  }

  sample_size = sample_vecs.size();
  sample_data.reserve(sample_size * vec_size);

  for (size_t i = 0; i < sample_size; i++) {
    sample_data.append(reinterpret_cast<const char *>(sample_vecs[i].data()),
                       vec_size);
  }

  return 0;
}

template <typename T>
int DiskAnnAlgorithm::prepare_pq_train_data(
    const IndexMeta &meta, size_t num_train, std::string &train_data,
    bool use_zero_mean, std::vector<uint8_t> &centroid,
    std::shared_ptr<CompactIndexFeatures> &train_features) {
  uint32_t dim = meta.dimension();
  uint32_t vec_size = meta.element_size();

  std::string train_data_processed;
  train_data_processed.resize(num_train * vec_size);

  std::memcpy(&(train_data_processed[0]), train_data.data(),
              num_train * vec_size);

  // use fp32 to accumulate to avoid overflow
  float centroid_temp[dim];
  for (uint64_t d = 0; d < dim; d++) {
    centroid_temp[d] = 0;
  }

  T *train_data_processed_ptr = reinterpret_cast<T *>(&train_data_processed[0]);

  if (use_zero_mean) {
    for (uint64_t d = 0; d < dim; d++) {
      for (uint64_t p = 0; p < num_train; p++) {
        centroid_temp[d] += train_data_processed_ptr[p * dim + d];
      }
      centroid_temp[d] /= num_train;
    }

    for (uint64_t d = 0; d < dim; d++) {
      for (uint64_t p = 0; p < num_train; p++) {
        train_data_processed_ptr[p * dim + d] -= centroid_temp[d];
      }
    }
  }

  for (size_t i = 0; i < num_train; ++i) {
    train_features->emplace(train_data_processed_ptr + i * dim);
  }

  // copy the centroid out
  centroid.resize(vec_size);
  T *centroid_ptr = reinterpret_cast<T *>(centroid.data());
  for (uint64_t d = 0; d < dim; d++) {
    centroid_ptr[d] = centroid_temp[d];
  }

  return 0;
}

template <typename T>
int DiskAnnAlgorithm::convert_pivot_data(
    const IndexMeta &meta, uint32_t num_centers,
    uint32_t pq_chunk_num, const std::vector<uint32_t> &chunk_dims,
    const std::vector<uint32_t> &chunk_offsets,
    IndexCluster::CentroidList &centroids,
    std::vector<uint8_t> &full_pivot_data) {
  uint32_t dim = meta.dimension();
  uint32_t element_size = meta.element_size();

  full_pivot_data.resize(num_centers * element_size);

  for (size_t chunk = 0; chunk < pq_chunk_num; ++chunk) {
    for (size_t cluster = 0; cluster < num_centers; ++cluster) {
      size_t idx = chunk * num_centers + cluster;

      T *pivot_data_ptr = reinterpret_cast<T *>(&(full_pivot_data[0])) +
                          cluster * dim + chunk_offsets[chunk];
      const T *feature_ptr =
          reinterpret_cast<const T *>(centroids[idx].feature());
      for (size_t d = 0; d <= chunk_dims[chunk]; ++d) {
        pivot_data_ptr[d] = feature_ptr[d];
      }
    }
  }

  return 0;
}

int DiskAnnAlgorithm::train_pq(IndexThreads::Pointer threads,
                               const IndexMeta &meta,
                               IndexHolder::Pointer holder,
                               std::string &train_data, size_t num_train,
                               uint32_t num_centers, uint32_t pq_chunk_num,
                               uint32_t max_iterations, bool use_zero_mean,
                               std::vector<uint8_t> &full_pivot_data,
                               std::vector<uint8_t> &centroid,
                               std::vector<uint32_t> &chunk_offsets) {
  uint32_t dim = meta.dimension();
  if (pq_chunk_num > dim) {
    LOG_ERROR("Error: number of chunks more than dimension. chunk: %u, dim: %u",
              pq_chunk_num, dim);
    return IndexError_InvalidArgument;
  }

  std::shared_ptr<CompactIndexFeatures> train_features(
      new CompactIndexFeatures(meta));

  uint32_t type = meta.data_type();

  int ret;
  switch (type) {
    case IndexMeta::DataType::DT_FP32:
      ret = prepare_pq_train_data<float>(
          meta, num_train, train_data, use_zero_mean, centroid, train_features);
      if (ret != 0) {
        LOG_ERROR("Failed to prepare pq train data");
        return ret;
      }
      break;

    case IndexMeta::DataType::DT_FP16:
      ret = prepare_pq_train_data<ailego::Float16>(
          meta, num_train, train_data, use_zero_mean, centroid, train_features);
      if (ret != 0) {
        LOG_ERROR("Failed to prepare pq train data");
        return ret;
      }
      break;
  }

  // Do Train
  ailego::Params params;
  params.set("proxima.cluster.multi_chunk_cluster.count", num_centers);
  params.set("proxima.cluster.multi_chunk_cluster.chunk_count", pq_chunk_num);
  params.set("proxima.cluster.multi_chunk_cluster.max_iterations",
             max_iterations);

  ret = chunk_cluster_.init(meta, params);
  if (ret != 0) {
    LOG_ERROR("Failed to get chunk cluster");
    return IndexError_InvalidArgument;
  }

  ret = chunk_cluster_.mount(train_features);
  if (ret != 0) {
    LOG_ERROR("Cannot mount train features");
    return ret;
  }


  std::vector<uint32_t> labels;

  ret = chunk_cluster_.cluster(threads, cluster_centroids_);
  if (ret != 0) {
    LOG_ERROR("Failed to cluster");
    return ret;
  }

  chunk_offsets = chunk_cluster_.chunk_dim_offsets();
  auto chunk_dims = chunk_cluster_.chunk_dims();

  switch (type) {
    case IndexMeta::DataType::DT_FP32:
      ret = convert_pivot_data<float>(meta, num_centers, pq_chunk_num,
                                      chunk_dims, chunk_offsets,
                                      cluster_centroids_, full_pivot_data);
      if (ret != 0) {
        LOG_ERROR("Failed to convert pivot data");
        return ret;
      }
      break;

    case IndexMeta::DataType::DT_FP16:
      ret = convert_pivot_data<ailego::Float16>(
          meta, num_centers, pq_chunk_num, chunk_dims, chunk_offsets,
          cluster_centroids_, full_pivot_data);
      if (ret != 0) {
        LOG_ERROR("Failed to convert pivot data");
        return ret;
      }
      break;
  }

  return 0;
}

int DiskAnnAlgorithm::train_quantized_data(
    IndexThreads::Pointer threads,
    IndexHolder::Pointer holder, const IndexMeta &meta,
    std::vector<uint8_t> &pq_full_pivot_data, std::vector<uint8_t> &pq_centroid,
    std::vector<uint32_t> &pq_chunk_offsets, size_t pq_chunk_num) {
  size_t train_size;
  std::string train_data;

  IndexMeta new_meta = meta;

  // if (meta.metric_name() == "Cosine") {
  //   new_meta.set_metric("InnerProduct", 0, IndexParams());
  // }
  if (meta.metric_name() == "InnerProduct") {
    new_meta.set_metric("SquaredEuclidean", 0, ailego::Params());
  }

  int ret = gen_random_sample(holder, new_meta, train_data, train_size);
  if (ret != 0) {
    LOG_ERROR("Get Random Sample Error, ret: %d", ret);
    return ret;
  }

  LOG_INFO("Training data with %zu samples loaded.", train_size);

  // bool use_zero_mean = (meta.metric_name() != "InnerProduct" ? true :
  // false);
  bool use_zero_mean = true;

  ret = train_pq(threads, new_meta, holder, train_data, train_size,
                 PQTable::kPQCentroidNum, pq_chunk_num, PQTable::kMeanIterNum,
                 use_zero_mean, pq_full_pivot_data, pq_centroid,
                 pq_chunk_offsets);
  if (ret != 0) {
    LOG_ERROR("Train PQ Error, ret: %d", ret);
    return ret;
  }

  return 0;
}

int DiskAnnAlgorithm::generate_pq(IndexThreads::Pointer threads,
                                  const IndexMeta &meta,
                                  IndexHolder::Pointer holder,
                                  uint32_t pq_chunk_num, bool use_zero_mean,
                                  std::vector<uint8_t> &centroid,
                                  std::vector<uint8_t> &block_compressed_data) {
  uint32_t type = meta.data_type();
  uint32_t dim = meta.dimension();
  if (pq_chunk_num > dim) {
    LOG_ERROR("Error: number of chunks more than dimension. chunk: %u, dim: %u",
              pq_chunk_num, dim);
    return IndexError_InvalidArgument;
  }

  // Do Label
  std::vector<uint32_t> labels;
  size_t num_vecs = holder->count();
  size_t batch_size =
      num_vecs <= compress_batch_size_ ? num_vecs : compress_batch_size_;

  std::vector<uint32_t> block_compressed_base(batch_size * pq_chunk_num);

  std::memset(&block_compressed_base[0], 0,
              batch_size * pq_chunk_num * sizeof(uint32_t));

  std::vector<uint8_t> block_data(batch_size * meta.element_size());
  std::vector<uint8_t> block_data_converted(batch_size * meta.element_size());

  size_t block_num = DiskAnnUtil::div_round_up(num_vecs, batch_size);

  block_compressed_data.resize(num_vecs * pq_chunk_num);

  auto iter = holder->create_iterator();
  if (!iter) {
    LOG_ERROR("Create iterator for holder failed");
    return IndexError_Runtime;
  }

  for (size_t block = 0; block < block_num; block++) {
    size_t start_id = block * batch_size;
    size_t end_id = std::min((block + 1) * batch_size, num_vecs);

    size_t cur_block_size = end_id - start_id;

    for (size_t i = 0; i < cur_block_size && iter->is_valid(); i++) {
      const void *vec = iter->data();
      std::memcpy(
          reinterpret_cast<uint8_t *>(&block_data[0]) + i * meta.element_size(),
          vec, meta.element_size());
      iter->next();
    }

    std::memcpy(block_data_converted.data(), block_data.data(),
                cur_block_size * meta.element_size());

    LOG_INFO("Processing Docs, Range: [%zu, %zu)..", start_id, end_id);

    std::shared_ptr<CompactIndexFeatures> block_features(
        new CompactIndexFeatures(meta));

    switch (type) {
      case IndexMeta::DataType::DT_FP32:
        DiskAnnUtil::convert_vector_to_residual<float>(
            reinterpret_cast<float *>(block_data_converted.data()),
            cur_block_size, dim, centroid.data());
        break;
      case IndexMeta::DataType::DT_FP16:
        DiskAnnUtil::convert_vector_to_residual<ailego::Float16>(
            reinterpret_cast<ailego::Float16 *>(block_data_converted.data()),
            cur_block_size, dim, centroid.data());
        break;
      default:
        return IndexError_InvalidArgument;
    }

    for (size_t i = 0; i < cur_block_size; i++) {
      block_features->emplace(block_data_converted.data() +
                              i * meta.element_size());
    }

    int ret = chunk_cluster_.mount(block_features);
    if (ret != 0) {
      LOG_ERROR("Cannot mount block features");
      return ret;
    }

    ret = chunk_cluster_.label(threads, cluster_centroids_, &labels);
    if (ret != 0) {
      LOG_ERROR("Failed to label");
      return ret;
    }

    std::vector<uint8_t> compressed_data(cur_block_size * pq_chunk_num);

    DiskAnnUtil::convert_types_uint32_to_uint8(
        labels.data(), compressed_data.data(), cur_block_size, pq_chunk_num);

    memcpy(&(block_compressed_data[0]) + start_id * pq_chunk_num,
           compressed_data.data(), cur_block_size * pq_chunk_num);

    LOG_INFO("Generate PQ Data Done.");
  }

  return 0;
}

int DiskAnnAlgorithm::generate_quantized_data(
    IndexThreads::Pointer threads,
    IndexHolder::Pointer holder, const IndexMeta &meta,
    std::vector<uint8_t> &pq_centroid,
    std::vector<uint8_t> &block_compressed_data, size_t pq_chunk_num) {
  IndexMeta new_meta = meta;

  // if (meta.metric_name() == "Cosine") {
  //   new_meta.set_metric("InnerProduct", 0, ailego::Params());
  // }
  if (meta.metric_name() == "InnerProduct") {
    new_meta.set_metric("SquaredEuclidean", 0, ailego::Params());
  }

  // bool use_zero_mean = (meta.metric_name() != "InnerProduct" ? true :
  // false);
  bool use_zero_mean = true;

  int ret = generate_pq(threads, new_meta, holder, pq_chunk_num, use_zero_mean,
                        pq_centroid, block_compressed_data);
  if (ret != 0) {
    LOG_ERROR("Generate PQ Error, ret: %d", ret);
    return ret;
  }

  return 0;
}

}  // namespace core
}  // namespace zvec