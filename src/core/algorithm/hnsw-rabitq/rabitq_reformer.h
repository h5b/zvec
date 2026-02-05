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

#include <vector>
#include <rabitqlib/quantization/rabitq.hpp>
#include <rabitqlib/utils/rotator.hpp>
#include "core/algorithm/cluster/linear_seeker.h"
#include "zvec/core/framework/index_features.h"
#include "zvec/core/framework/index_reformer.h"
#include "zvec/core/framework/index_storage.h"
#include "hnsw_rabitq_query_entity.h"
#include "rabitq_params.h"

namespace zvec {
namespace core {

/*! RaBitQ Reformer
 * Loads centroids and performs query transformation and vector quantization
 */
class RabitqReformer : public IndexReformer {
 public:
  typedef std::shared_ptr<RabitqReformer> Pointer;

  //! Constructor
  RabitqReformer();

  RabitqReformer(IndexMeta::DataType) {}

  //! Destructor
  ~RabitqReformer() override;

  //! Initialize Reformer
  int init(const ailego::Params &params) override;

  //! Cleanup Reformer
  int cleanup(void) override;

  //! Load centroids from storage
  int load(IndexStorage::Pointer storage) override;

  //! Unload index
  int unload(void) override;

  //! Transform query - rotate and quantize for search
  int transform(const void *query, const IndexQueryMeta &qmeta,
                std::string *out, IndexQueryMeta *ometa) const override;

  //! Convert record - quantize vector for add operation
  int convert(const void *record, const IndexQueryMeta &rmeta, std::string *out,
              IndexQueryMeta *ometa) const override;

  //! Dump reformer into dumper
  int dump(const IndexDumper::Pointer &dumper);

  //! Dump reformer into storage
  int dump(const IndexStorage::Pointer &dumper);

  int transform_to_entity(const void *query,
                          HnswRabitqQueryEntity *entity) const;

  size_t num_clusters() const {
    return num_clusters_;
  }

  rabitqlib::MetricType rabitq_metric_type() const {
    return metric_type_;
  }

 private:
  //! Find nearest centroid for a vector
  size_t find_nearest_centroid(const float *vector) const;

  //! Quantize a single vector
  int quantize_vector(const float *raw_vector, uint32_t cluster_id,
                      std::string *quantized_data) const;

 private:
  // RaBitQ parameters
  size_t num_clusters_{0};
  size_t ex_bits_{0};
  size_t dimension_{0};
  size_t padded_dim_{0};

  // Original centroids: num_clusters * dimension (for LinearSeeker query)
  std::vector<float> centroids_;
  // Rotated centroids: num_clusters * padded_dim (for quantization)
  std::vector<float> rotated_centroids_;

  // Rotator for vector transformation
  rabitqlib::RotatorType rotator_type_{rabitqlib::RotatorType::FhtKacRotator};
  std::unique_ptr<rabitqlib::Rotator<float>> rotator_;
  rabitqlib::quant::RabitqConfig query_config_;
  rabitqlib::quant::RabitqConfig config_;
  rabitqlib::MetricType metric_type_{rabitqlib::METRIC_L2};
  size_t size_bin_data_{0};
  size_t size_ex_data_{0};

  // LinearSeeker for centroid search
  LinearSeeker::Pointer centroid_seeker_;
  CoherentIndexFeatures::Pointer centroid_features_;

  bool loaded_{false};
};

}  // namespace core
}  // namespace zvec
