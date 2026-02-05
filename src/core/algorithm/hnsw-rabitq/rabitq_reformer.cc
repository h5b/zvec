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

#include "rabitq_reformer.h"
#include <string>
#include <rabitqlib/index/query.hpp>
#include <zvec/ailego/logger/logger.h>
#include <zvec/ailego/utility/string_helper.h>
#include "algorithm/hnsw-rabitq/rabitq_converter.h"
#include "rabitqlib/defines.hpp"
#include "zvec/core/framework/index_error.h"
#include "zvec/core/framework/index_factory.h"
#include "zvec/core/framework/index_helper.h"
#include "zvec/core/framework/index_logger.h"
#include "zvec/core/framework/index_meta.h"
#include "zvec/core/framework/index_metric.h"
#include "zvec/core/framework/index_storage.h"
#include "zvec/db/type.h"
#include "hnsw_rabitq_entity.h"
#include "hnsw_rabitq_index_format.h"

namespace zvec {
namespace core {

RabitqReformer::RabitqReformer() {}

RabitqReformer::~RabitqReformer() {
  this->cleanup();
}

int RabitqReformer::init(const ailego::Params &params) {
  std::string metric_name =
      params.get_as_string(PARAM_RABITQ_REFORMER_METRIC_NAME);
  if (metric_name == "SquaredEuclidean") {
    metric_type_ = rabitqlib::METRIC_L2;
  } else if (metric_name == "InnerProduct") {
    metric_type_ = rabitqlib::METRIC_IP;
  } else if (metric_name == "Cosine") {
    metric_type_ = rabitqlib::METRIC_IP;
  } else {
    LOG_ERROR("Unsupported metric name: %s", metric_name.c_str());
    return IndexError_InvalidArgument;
  }
  LOG_DEBUG("Rabitq reformer init done. metric_name=%s metric_type=%d",
            metric_name.c_str(), static_cast<int>(metric_type_));
  return 0;
}

int RabitqReformer::cleanup() {
  centroids_.clear();
  rotated_centroids_.clear();
  centroid_seeker_.reset();
  centroid_features_.reset();
  loaded_ = false;
  rotator_.reset();
  return 0;
}

int RabitqReformer::unload() {
  return this->cleanup();
}

int RabitqReformer::load(IndexStorage::Pointer storage) {
  if (!storage) {
    LOG_ERROR("Invalid storage for load");
    return IndexError_InvalidArgument;
  }

  auto segment = storage->get(RABITQ_CONVERER_SEG_ID);
  if (!segment) {
    LOG_ERROR("Failed to get segment %s", RABITQ_CONVERER_SEG_ID.c_str());
    return IndexError_InvalidFormat;
  }

  size_t offset = 0;
  RabitqConverterHeader header;
  IndexStorage::MemoryBlock block;
  size_t size = segment->read(offset, block, sizeof(header));
  if (size != sizeof(header)) {
    LOG_ERROR("Failed to read header");
    return IndexError_InvalidFormat;
  }
  memcpy(&header, block.data(), sizeof(header));
  dimension_ = header.dim;
  padded_dim_ = header.padded_dim;
  ex_bits_ = header.ex_bits;
  num_clusters_ = header.num_clusters;
  rotator_type_ = static_cast<rabitqlib::RotatorType>(header.rotator_type);
  offset += sizeof(header);

  // Read rotated centroids
  size_t rotated_centroids_size =
      sizeof(float) * header.num_clusters * header.padded_dim;
  size = segment->read(offset, block, rotated_centroids_size);
  if (size != rotated_centroids_size) {
    LOG_ERROR("Failed to read rotated centroids");
    return IndexError_InvalidFormat;
  }
  rotated_centroids_.resize(header.num_clusters * header.padded_dim);
  memcpy(rotated_centroids_.data(), block.data(), rotated_centroids_size);
  offset += size;

  // Read original centroids (for LinearSeeker query)
  size_t centroids_size = sizeof(float) * header.num_clusters * header.dim;
  size = segment->read(offset, block, centroids_size);
  if (size != centroids_size) {
    LOG_ERROR("Failed to read centroids");
    return IndexError_InvalidFormat;
  }
  centroids_.resize(header.num_clusters * header.dim);
  memcpy(centroids_.data(), block.data(), centroids_size);
  offset += size;

  // Read rotator
  size_t rotator_size = header.rotator_size;
  size = segment->read(offset, block, rotator_size);
  if (size != rotator_size) {
    LOG_ERROR("Failed to read rotator");
    return IndexError_InvalidFormat;
  }
  // Create rotator
  rotator_.reset(
      rabitqlib::choose_rotator<float>(dimension_, rotator_type_, padded_dim_));
  rotator_->load(reinterpret_cast<const char *>(block.data()));
  offset += size;

  this->query_config_ = rabitqlib::quant::faster_config(
      padded_dim_, rabitqlib::SplitSingleQuery<float>::kNumBits);
  this->config_ = rabitqlib::quant::faster_config(padded_dim_, ex_bits_ + 1);

  size_bin_data_ = rabitqlib::BinDataMap<float>::data_bytes(padded_dim_);
  size_ex_data_ =
      rabitqlib::ExDataMap<float>::data_bytes(padded_dim_, ex_bits_);

  // Initialize LinearSeeker for centroid search
  IndexMeta centroid_meta;
  centroid_meta.set_data_type(IndexMeta::DataType::DT_FP32);
  centroid_meta.set_dimension(static_cast<uint32_t>(dimension_));
  centroid_meta.set_metric("SquaredEuclidean", 0, ailego::Params());

  centroid_features_ = std::make_shared<CoherentIndexFeatures>();
  centroid_features_->mount(centroid_meta, centroids_.data(),
                            centroids_.size() * sizeof(float));

  centroid_seeker_ = std::make_shared<LinearSeeker>();
  int ret = centroid_seeker_->init(centroid_meta);
  if (ret != 0) {
    LOG_ERROR("Failed to init centroid seeker. ret[%d]", ret);
    return ret;
  }
  ret = centroid_seeker_->mount(centroid_features_);
  if (ret != 0) {
    LOG_ERROR("Failed to mount centroid features. ret[%d]", ret);
    return ret;
  }

  LOG_INFO(
      "Rabitq reformer load done. dimension=%zu, padded_dim=%zu, "
      "ex_bits=%zu, num_clusters=%zu, size_bin_data=%zu, size_ex_data=%zu "
      "rotator_type=%d",
      dimension_, padded_dim_, ex_bits_, num_clusters_, size_bin_data_,
      size_ex_data_, (int)rotator_type_);
  loaded_ = true;
  return 0;
}

int RabitqReformer::convert(const void *record, const IndexQueryMeta &rmeta,
                            std::string *out, IndexQueryMeta *ometa) const {
  if (!loaded_) {
    LOG_ERROR("Centroids not loaded yet");
    return IndexError_NoReady;
  }

  if (!record || !out) {
    LOG_ERROR("Invalid arguments for convert");
    return IndexError_InvalidArgument;
  }

  // Validate input
  // input may be transformed, require rmeta.dimension >= dimension_
  if (rmeta.dimension() < dimension_ ||
      rmeta.data_type() != IndexMeta::DataType::DT_FP32) {
    LOG_ERROR("Invalid record meta: dimension=%zu, data_type=%d",
              static_cast<size_t>(rmeta.dimension()), (int)rmeta.data_type());
    return IndexError_InvalidArgument;
  }

  // Find nearest centroid using LinearSeeker
  Seeker::Document doc;
  int ret = centroid_seeker_->seek(record, dimension_ * sizeof(float), &doc);
  if (ret != 0) {
    LOG_ERROR("Failed to seek centroid. ret[%d]", ret);
    return ret;
  }
  uint32_t cluster_id = doc.index;

  // Quantize vector
  const float *vector = static_cast<const float *>(record);
  ret = quantize_vector(vector, cluster_id, out);
  if (ret != 0) {
    LOG_ERROR("Failed to quantize vector");
    return ret;
  }

  ometa->set_meta(IndexMeta::DataType::DT_INT8, (uint32_t)out->size());

  return 0;
}

int RabitqReformer::transform(const void *, const IndexQueryMeta &,
                              std::string *, IndexQueryMeta *) const {
  return IndexError_NotImplemented;
}

int RabitqReformer::transform_to_entity(const void *query,
                                        HnswRabitqQueryEntity *entity) const {
  if (!loaded_) {
    LOG_ERROR("Centroids not loaded yet");
    return IndexError_NoReady;
  }

  if (!query) {
    LOG_ERROR("Invalid arguments for transform");
    return IndexError_InvalidArgument;
  }

  const float *query_vector = static_cast<const float *>(query);

  // Apply rotator
  entity->rotated_query.resize(padded_dim_);
  rotator_->rotate(query_vector, entity->rotated_query.data());

  // Quantize query to 4-bit representation
  // TODO: add IP support

  entity->query_wrapper = std::make_unique<rabitqlib::SplitSingleQuery<float>>(
      entity->rotated_query.data(), padded_dim_, ex_bits_, query_config_,
      metric_type_);

  // Preprocess - get the distance from query to all centroids
  entity->q_to_centroids.resize(num_clusters_);

  if (metric_type_ == rabitqlib::METRIC_L2) {
    for (size_t i = 0; i < num_clusters_; i++) {
      entity->q_to_centroids[i] = std::sqrt(rabitqlib::euclidean_sqr(
          entity->rotated_query.data(),
          rotated_centroids_.data() + (i * padded_dim_), padded_dim_));
    }
  } else if (metric_type_ == rabitqlib::METRIC_IP) {
    entity->q_to_centroids.resize(num_clusters_ * 2);
    // first half as g_add, second half as g_error
    for (size_t i = 0; i < num_clusters_; i++) {
      entity->q_to_centroids[i] = rabitqlib::dot_product(
          entity->rotated_query.data(),
          rotated_centroids_.data() + (i * padded_dim_), padded_dim_);
      entity->q_to_centroids[i + num_clusters_] =
          std::sqrt(rabitqlib::euclidean_sqr(
              entity->rotated_query.data(),
              rotated_centroids_.data() + (i * padded_dim_), padded_dim_));
    }
  }

  return 0;
}

size_t RabitqReformer::find_nearest_centroid(const float *vector) const {
  size_t nearest_id = 0;
  float min_dist = std::numeric_limits<float>::max();

  for (size_t i = 0; i < num_clusters_; ++i) {
    const float *centroid = &rotated_centroids_[i * padded_dim_];

    // Compute L2 distance
    float dist = 0.0f;
    for (size_t d = 0; d < dimension_; ++d) {
      float diff = vector[d] - centroid[d];
      dist += diff * diff;
    }

    if (dist < min_dist) {
      min_dist = dist;
      nearest_id = i;
    }
  }

  return nearest_id;
}

int RabitqReformer::quantize_vector(const float *raw_vector,
                                    uint32_t cluster_id,
                                    std::string *quantized_data) const {
  // Quantize raw data and initialize quantized data
  std::vector<float> rotated_data(padded_dim_);
  rotator_->rotate(raw_vector, rotated_data.data());

  // quantized format:
  // cluster_id + bin_data + ex_data
  quantized_data->resize(sizeof(cluster_id) + size_bin_data_ + size_ex_data_);
  memcpy(&(*quantized_data)[0], &cluster_id, sizeof(cluster_id));
  int bin_data_offset = sizeof(cluster_id);
  int ex_data_offset = bin_data_offset + size_bin_data_;
  rabitqlib::quant::quantize_split_single(
      rotated_data.data(),
      rotated_centroids_.data() + (cluster_id * padded_dim_), padded_dim_,
      ex_bits_, &(*quantized_data)[bin_data_offset],
      &(*quantized_data)[ex_data_offset], metric_type_, config_);

  return 0;
}

int RabitqReformer::dump(const IndexDumper::Pointer &dumper) {
  if (!dumper) {
    LOG_ERROR("Null dumper");
    return IndexError_InvalidArgument;
  }

  if (!loaded_ || rotated_centroids_.empty() || centroids_.empty()) {
    LOG_ERROR("No centroids to dump");
    return IndexError_NoReady;
  }

  size_t dumped_size = 0;
  int ret = dump_rabitq_centroids(
      dumper, dimension_, padded_dim_, ex_bits_, num_clusters_, rotator_type_,
      rotated_centroids_, centroids_, rotator_, &dumped_size);
  if (ret != 0) {
    return ret;
  }

  LOG_INFO("RabitqReformer dump completed: %zu bytes", dumped_size);
  return 0;
}

int RabitqReformer::dump(const IndexStorage::Pointer &storage) {
  if (!storage) {
    LOG_ERROR("Null storage");
    return IndexError_InvalidArgument;
  }

  if (!loaded_ || rotated_centroids_.empty() || centroids_.empty()) {
    LOG_ERROR("No centroids to dump");
    return IndexError_NoReady;
  }

  auto align_size = [](size_t size) -> size_t {
    return (size + 0x1F) & (~0x1F);
  };

  // Calculate total size
  size_t header_size = sizeof(RabitqConverterHeader);
  size_t rotated_centroids_size = rotated_centroids_.size() * sizeof(float);
  size_t centroids_size = centroids_.size() * sizeof(float);
  size_t rotator_size = rotator_->dump_bytes();
  size_t data_size =
      header_size + rotated_centroids_size + centroids_size + rotator_size;
  size_t total_size = align_size(data_size);

  // Append segment
  int ret = storage->append(RABITQ_CONVERER_SEG_ID, total_size);
  if (ret != 0) {
    LOG_ERROR("Failed to append segment %s, ret=%d",
              RABITQ_CONVERER_SEG_ID.c_str(), ret);
    return ret;
  }

  // Get segment
  auto segment = storage->get(RABITQ_CONVERER_SEG_ID);
  if (!segment) {
    LOG_ERROR("Failed to get segment %s", RABITQ_CONVERER_SEG_ID.c_str());
    return IndexError_ReadData;
  }

  size_t offset = 0;

  // Write header
  RabitqConverterHeader header;
  header.dim = static_cast<uint32_t>(dimension_);
  header.padded_dim = static_cast<uint32_t>(padded_dim_);
  header.num_clusters = static_cast<uint32_t>(num_clusters_);
  header.ex_bits = static_cast<uint8_t>(ex_bits_);
  header.rotator_type = static_cast<uint8_t>(rotator_type_);
  header.rotator_size = static_cast<uint32_t>(rotator_size);
  size_t written = segment->write(offset, &header, header_size);
  if (written != header_size) {
    LOG_ERROR("Failed to write header: written=%zu, expected=%zu", written,
              header_size);
    return IndexError_WriteData;
  }
  offset += header_size;

  // Write rotated centroids
  written =
      segment->write(offset, rotated_centroids_.data(), rotated_centroids_size);
  if (written != rotated_centroids_size) {
    LOG_ERROR("Failed to write rotated centroids: written=%zu, expected=%zu",
              written, rotated_centroids_size);
    return IndexError_WriteData;
  }
  offset += rotated_centroids_size;

  // Write original centroids
  written = segment->write(offset, centroids_.data(), centroids_size);
  if (written != centroids_size) {
    LOG_ERROR("Failed to write centroids: written=%zu, expected=%zu", written,
              centroids_size);
    return IndexError_WriteData;
  }
  offset += centroids_size;

  // Write rotator data
  std::vector<char> buffer(rotator_size);
  rotator_->save(buffer.data());
  written = segment->write(offset, buffer.data(), rotator_size);
  if (written != rotator_size) {
    LOG_ERROR("Failed to write rotator data: written=%zu, expected=%zu",
              written, rotator_size);
    return IndexError_WriteData;
  }

  LOG_INFO("RabitqReformer dump to storage completed: %zu bytes", data_size);
  return 0;
}

INDEX_FACTORY_REGISTER_REFORMER_ALIAS(RabitqReformer, RabitqReformer,
                                      IndexMeta::DataType::DT_FP32);

}  // namespace core
}  // namespace zvec
