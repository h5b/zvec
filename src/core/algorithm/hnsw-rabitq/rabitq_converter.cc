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

#include "rabitq_converter.h"
#include <strings.h>
#include <memory>
#include <rabitqlib/utils/rotator.hpp>
#include <zvec/ailego/container/params.h>
#include <zvec/ailego/parallel/thread_pool.h>
#include <zvec/ailego/utility/string_helper.h>
#include "algorithm/hnsw-rabitq/rabitq_reformer.h"
#include "core/algorithm/cluster/cluster_params.h"
#include "zvec/core/framework/index_cluster.h"
#include "zvec/core/framework/index_error.h"
#include "zvec/core/framework/index_factory.h"
#include "zvec/core/framework/index_features.h"
#include "zvec/core/framework/index_helper.h"
#include "zvec/core/framework/index_holder.h"
#include "zvec/core/framework/index_meta.h"
#include "hnsw_rabitq_entity.h"
#include "hnsw_rabitq_index_format.h"
#include "rabitq_holder_wrapper.h"
#include "rabitq_params.h"

namespace zvec {
namespace core {

// Segment ID for storing centroids
static constexpr const char *kRabitqCentroidsSegmentId = "rabitq.centroids";

RabitqConverter::RabitqConverter() {}

RabitqConverter::~RabitqConverter() {
  this->cleanup();
}

int RabitqConverter::init(const IndexMeta &meta, const ailego::Params &params) {
  // Copy meta and ensure it has metric information
  meta_ = meta;
  dimension_ = meta.dimension();

  // Ensure meta has metric set
  if (meta_.metric_name().empty()) {
    meta_.set_metric("SquaredEuclidean", 0, ailego::Params());
  }

  // Round up dimension to multiple of 64
  padded_dim_ = ((dimension_ + 63) / 64) * 64;

  // Get RaBitQ parameters with defaults
  uint32_t total_bits = 0;
  params.get(PARAM_RABITQ_TOTAL_BITS, &total_bits);
  if (total_bits == 0) {
    total_bits = kDefaultRabitqTotalBits;
  }
  if (total_bits < 1 || total_bits > 9) {
    LOG_ERROR("Invalid total_bits: %zu, must be in [1, 9]", (size_t)total_bits);
    return IndexError_InvalidArgument;
  }
  ex_bits_ = total_bits - 1;

  params.get(PARAM_RABITQ_NUM_CLUSTERS, &num_clusters_);
  if (num_clusters_ == 0) {
    num_clusters_ = kDefaultNumClusters;
  }

  // Validate parameters
  if (num_clusters_ == 0 || num_clusters_ > 256) {
    LOG_ERROR("Invalid num_clusters: %zu, must be in [1, 256]", num_clusters_);
    return IndexError_InvalidArgument;
  }

  if (ex_bits_ > 8) {
    LOG_ERROR("Invalid ex_bits: %zu, must be <= 8", ex_bits_);
    return IndexError_InvalidArgument;
  }

  if (meta.data_type() != IndexMeta::DataType::DT_FP32) {
    LOG_ERROR("RaBitQ only supports FP32 data type");
    return IndexError_Unsupported;
  }

  std::string rotator_type_str;
  params.get(PARAM_RABITQ_ROTATOR_TYPE, &rotator_type_str);
  if (rotator_type_str.empty()) {
    rotator_type_ = rabitqlib::RotatorType::FhtKacRotator;
  } else if (strncasecmp(rotator_type_str.c_str(), "fht", 3) == 0) {
    rotator_type_ = rabitqlib::RotatorType::FhtKacRotator;
  } else if (strncasecmp(rotator_type_str.c_str(), "matrix", 6) == 0) {
    rotator_type_ = rabitqlib::RotatorType::MatrixRotator;
  } else {
    LOG_ERROR("Invalid rotator_type: %s", rotator_type_str.c_str());
    return IndexError_InvalidArgument;
  }

  // Create rotator
  rotator_.reset(
      rabitqlib::choose_rotator<float>(dimension_, rotator_type_, padded_dim_));

  LOG_INFO(
      "RabitqConverter initialized: dim=%zu, padded_dim=%zu, "
      "num_clusters=%zu, ex_bits=%zu, rotator_type=%d[%s]",
      dimension_, padded_dim_, num_clusters_, ex_bits_, (int)rotator_type_,
      rotator_type_str.c_str());

  return 0;
}

int RabitqConverter::cleanup() {
  centroids_.clear();
  rotated_centroids_.clear();
  result_holder_.reset();
  rotator_.reset();
  return 0;
}

int RabitqConverter::train(IndexHolder::Pointer holder) {
  if (!holder) {
    LOG_ERROR("Null holder for training");
    return IndexError_InvalidArgument;
  }

  ailego::ElapsedTime timer;

  size_t vector_count = holder->count();
  if (vector_count == 0) {
    LOG_ERROR("No vectors for training");
    return IndexError_InvalidArgument;
  }

  LOG_INFO("Training with %zu vectors from holder", vector_count);

  // do sampling from all data
  // TODO: set sample_count_
  auto sample_count_ = vector_count;
  auto sampler = std::make_shared<SampleIndexFeatures<CompactIndexFeatures>>(
      meta_, sample_count_);
  auto iter = holder->create_iterator();
  if (!iter) {
    LOG_ERROR("Create iterator error");
    return IndexError_Runtime;
  }
  for (; iter->is_valid(); iter->next()) {
    sampler->emplace(iter->data());
  }

  // Holder is not needed, cleanup it.
  holder.reset();

  if (sampler->count() == 0) {
    LOG_ERROR("Load training data error");
    return IndexError_InvalidLength;
  }


  // Create KmeansCluster for training centroids
  auto cluster = IndexFactory::CreateCluster("OptKmeansCluster");
  if (!cluster) {
    LOG_ERROR("Failed to create OptKmeansCluster");
    return IndexError_NoExist;
  }

  // Initialize cluster
  LOG_INFO(
      "Initializing KmeansCluster with meta: dim=%u, data_type=%d, metric=%s",
      meta_.dimension(), (int)meta_.data_type(), meta_.metric_name().c_str());
  ailego::Params cluster_params;
  int ret = cluster->init(meta_, cluster_params);
  if (ret != 0) {
    LOG_ERROR("Failed to initialize KmeansCluster: %d", ret);
    return ret;
  }

  ret = cluster->mount(sampler);
  cluster->suggest(num_clusters_);

  // Perform clustering
  IndexCluster::CentroidList cents;
  // TODO: support specify threads with argument
  auto threads = std::make_shared<SingleQueueIndexThreads>(0, false);
  ret = cluster->cluster(threads, cents);
  if (ret != 0) {
    LOG_ERROR("Failed to perform clustering: %d", ret);
    return ret;
  }

  if (cents.size() != num_clusters_) {
    LOG_WARN("Expected %zu clusters, got %zu", num_clusters_, cents.size());
    num_clusters_ = cents.size();
  }
  // Extract original centroids (for LinearSeeker query)
  centroids_.resize(num_clusters_ * dimension_);
  // Extract rotated centroids (for quantization)
  rotated_centroids_.resize(num_clusters_ * padded_dim_);
  for (uint32_t i = 0; i < num_clusters_; ++i) {
    const float *cent_data = static_cast<const float *>(cents[i].feature());
    // Save original centroids
    std::memcpy(&centroids_[i * dimension_], cent_data,
                dimension_ * sizeof(float));
    // Save rotated centroids
    this->rotator_->rotate(cent_data, &rotated_centroids_[i * padded_dim_]);
  }

  stats_.set_trained_count(sampler->count());
  stats_.set_trained_costtime(timer.milli_seconds());

  LOG_INFO("Training completed: %zu centroids, cost %zu ms", num_clusters_,
           static_cast<size_t>(timer.milli_seconds()));

  return 0;
}


int RabitqConverter::transform(IndexHolder::Pointer holder) {
  if (!holder) {
    LOG_ERROR("Null holder for transformation");
    return IndexError_InvalidArgument;
  }

  if (rotated_centroids_.empty()) {
    LOG_ERROR("Centroids not trained yet");
    return IndexError_NoReady;
  }

  LOG_ERROR("Not implemented");
  return IndexError_NotImplemented;
}

int RabitqConverter::dump(const IndexDumper::Pointer &dumper) {
  if (!dumper) {
    LOG_ERROR("Null dumper");
    return IndexError_InvalidArgument;
  }

  if (rotated_centroids_.empty() || centroids_.empty()) {
    LOG_ERROR("No centroids to dump");
    return IndexError_NoReady;
  }

  ailego::ElapsedTime timer;
  size_t dumped_size = 0;

  int ret = dump_rabitq_centroids(
      dumper, dimension_, padded_dim_, ex_bits_, num_clusters_, rotator_type_,
      rotated_centroids_, centroids_, rotator_, &dumped_size);
  if (ret != 0) {
    return ret;
  }

  stats_.set_dumped_size(dumped_size);
  stats_.set_dumped_costtime(timer.milli_seconds());

  LOG_INFO("Dump completed: %zu bytes, cost %zu ms", stats_.dumped_size(),
           static_cast<size_t>(timer.milli_seconds()));
  return 0;
}

int RabitqConverter::quantize_vector(const float *raw_vector, size_t cluster_id,
                                     std::string *quantized_data) {
  // const float *centroid = &rotated_centroids_[cluster_id * padded_dim_];

  // rabitq::rabitqquantizer quantizer(dimension_, ex_bits_, metric);

  // // quantize
  // rabitq::quantizedvector qvec;
  // qvec.cluster_id = static_cast<uint32_t>(cluster_id);
  // quantizer.quantize_vector(raw_vector, centroid, &qvec);

  // // serialize: cluster_id + bin_data + ex_data + factors
  // quantized_data->clear();
  // quantized_data->reserve(qvec.total_bytes());

  // quantized_data->append(reinterpret_cast<const char *>(&qvec.cluster_id),
  //                        sizeof(qvec.cluster_id));
  // quantized_data->append(reinterpret_cast<const char
  // *>(qvec.bin_data.data()),
  //                        qvec.bin_data.size());
  // quantized_data->append(reinterpret_cast<const char
  // *>(qvec.ex_data.data()),
  //                        qvec.ex_data.size());
  // quantized_data->append(reinterpret_cast<const char *>(&qvec.f_add),
  //                        sizeof(qvec.f_add));
  // quantized_data->append(reinterpret_cast<const char *>(&qvec.f_rescale),
  //                        sizeof(qvec.f_rescale));
  // quantized_data->append(reinterpret_cast<const char *>(&qvec.f_error),
  //                        sizeof(qvec.f_error));

  // return 0;
  return 0;
}

int RabitqConverter::to_reformer(IndexReformer::Pointer *reformer) {
  auto memory_dumper = IndexFactory::CreateDumper("MemoryDumper");
  memory_dumper->init(ailego::Params());
  std::string file_id = ailego::StringHelper::Concat(
      "rabitq_converter_", ailego::Monotime::MilliSeconds(), rand());
  int ret = memory_dumper->create(file_id);
  if (ret != 0) {
    LOG_ERROR("Failed to create memory dumper: %d", ret);
    return ret;
  }
  ret = this->dump(memory_dumper);
  if (ret != 0) {
    LOG_ERROR("Failed to dump RabitqConverter: %d", ret);
    return ret;
  }
  ret = memory_dumper->close();
  if (ret != 0) {
    LOG_ERROR("Failed to close memory dumper: %d", ret);
    return ret;
  }

  auto res = std::make_shared<RabitqReformer>();
  ailego::Params reformer_params;
  reformer_params.set(PARAM_RABITQ_REFORMER_METRIC_NAME, meta_.metric_name());
  ret = res->init(reformer_params);
  if (ret != 0) {
    LOG_ERROR("Failed to initialize RabitqReformer: %d", ret);
    return ret;
  }
  auto memory_storage = IndexFactory::CreateStorage("MemoryReadStorage");
  ret = memory_storage->open(file_id, false);
  if (ret != 0) {
    LOG_ERROR("Failed to open memory storage: %d", ret);
    return ret;
  }
  ret = res->load(memory_storage);
  if (ret != 0) {
    LOG_ERROR("Failed to load RabitqReformer: %d", ret);
    return ret;
  }
  *reformer = std::move(res);
  // TODO: release memory of memory_storage
  return 0;
}

INDEX_FACTORY_REGISTER_CONVERTER_ALIAS(RabitqConverter, RabitqConverter,
                                       IndexMeta::DataType::DT_FP32);

}  // namespace core
}  // namespace zvec
