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

#include "diskann_builder.h"
#include <iostream>
#include <thread>
#include <ailego/pattern/defer.h>
#include <zvec/core/framework/index_error.h>
#include <zvec/core/interface/index_factory.h>
#include "diskann_context.h"
#include "diskann_params.h"

namespace zvec {
namespace core {

int DiskAnnBuilder::init(const IndexMeta &meta,
                         const ailego::Params &params) {
  LOG_INFO("Begin DiskAnnBuilder::init");

  params.get(PARAM_DISKANN_BUILDER_MAX_DEGREE, &max_degree_);
  params.get(PARAM_DISKANN_BUILDER_LIST_SIZE, &list_size_);
  params.get(PARAM_DISKANN_BUILDER_THREAD_COUNT, &build_thread_count_);

  if (build_thread_count_ == 0) {
    build_thread_count_ = std::thread::hardware_concurrency();
  }

  if (build_thread_count_ > std::thread::hardware_concurrency()) {
    LOG_WARN("Build thread count [%s] greater than cpu cores %u",
             PARAM_DISKANN_BUILDER_THREAD_COUNT.c_str(),
             std::thread::hardware_concurrency());
  }

  if (params.has(PARAM_DISKANN_BUILDER_MAX_PQ_CHUNK_NUM)) {
    uint32_t max_pq_chunk_num{0};
    params.get(PARAM_DISKANN_BUILDER_MAX_PQ_CHUNK_NUM, &max_pq_chunk_num);
    if (max_pq_chunk_num > meta.dimension()) {
      LOG_ERROR(
          "PQ Chunk Num larger than dimension, PQ Chunk Num: %d, Dimension: %d",
          max_pq_chunk_num, meta.dimension());
      return IndexError_InvalidArgument;
    }

    max_pq_chunk_num_ = max_pq_chunk_num;
  }

  if (params.has(PARAM_DISKANN_BUILDER_MEMORY_LIMIT)) {
    params.get(PARAM_DISKANN_BUILDER_MEMORY_LIMIT, &memory_limit_);
    if (memory_limit_ <= 0) {
      LOG_ERROR("Invalid memory limit: %lf", memory_limit_);
      return IndexError_InvalidArgument;
    }

    memory_limit_set_ = true;
  }

  if (params.has(PARAM_DISKANN_BUILDER_MAX_TRAIN_SAMPLE_COUNT)) {
    params.get(PARAM_DISKANN_BUILDER_MAX_TRAIN_SAMPLE_COUNT,
               &max_train_sample_count_);
  }

  if (params.has(PARAM_DISKANN_BUILDER_TRAIN_SAMPLE_RATIO)) {
    params.get(PARAM_DISKANN_BUILDER_TRAIN_SAMPLE_RATIO, &train_sample_ratio_);
  }

  meta_ = meta;

  std::string metric_name = meta_.metric_name();
  if (metric_name == "InnerProduct") {
    metric_name = "SquaredEuclidean";
  }

  metric_ = IndexFactory::CreateMetric(metric_name);
  if (!metric_) {
    LOG_ERROR("CreateMetric failed, name: %s", metric_name.c_str());
    return IndexError_NoExist;
  }

  int ret = metric_->init(meta_, meta_.metric_params());
  if (ret != 0) {
    LOG_ERROR("IndexMeasure init failed, ret=%d", ret);
    return ret;
  }

  meta_.set_builder("DiskAnnBuilder", DiskAnnEntity::kRevision, params);

  ret = entity_.init(meta, max_degree_, list_size_, memory_limit_,
                     build_thread_count_);
  if (ret != 0) {
    return ret;
  }

  algo_ = DiskAnnAlgorithm::UPointer(new DiskAnnAlgorithm(
      entity_, max_degree_, max_train_sample_count_, train_sample_ratio_));

  state_ = BUILD_STATE_INITED;

  return 0;
}

int DiskAnnBuilder::cleanup(void) {
  LOG_INFO("Begin DiskAnnBuilder::cleanup");

  LOG_INFO("End DiskAnnBuilder::cleanup");

  return 0;
}

int DiskAnnBuilder::calculate_entry_point() {
  std::string centroid;

  uint32_t dim = meta_.dimension();
  uint32_t type = meta_.data_type();

  if (type != IndexMeta::DataType::DT_FP32 &&
      type != IndexMeta::DataType::DT_FP16) {
    LOG_ERROR("Data type not supported");
    return IndexError_InvalidArgument;
  }

  centroid.resize(dim * sizeof(float));

  size_t dimension = meta_.dimension();
  float *centroid_data_ptr = reinterpret_cast<float *>(&centroid[0]);
  for (size_t i = 0; i < dimension; i++) {
    centroid_data_ptr[i] = 0;
  }

  switch (type) {
    case IndexMeta::DataType::DT_FP32:
      for (size_t id = 0; id < entity_.doc_cnt(); id++) {
        const float *data_ptr =
            reinterpret_cast<const float *>(entity_.get_vector(id));

        for (size_t i = 0; i < dimension; i++) {
          centroid_data_ptr[i] += data_ptr[i];
        }
      }
      break;
    case IndexMeta::DataType::DT_FP16:
      for (size_t id = 0; id < entity_.doc_cnt(); id++) {
        const ailego::Float16 *data_ptr =
            reinterpret_cast<const ailego::Float16 *>(entity_.get_vector(id));

        for (size_t i = 0; i < dimension; i++) {
          centroid_data_ptr[i] += data_ptr[i];
        }
      }
      break;
  }

  for (size_t i = 0; i < dimension; i++) {
    centroid_data_ptr[i] /= entity_.doc_cnt();
  }

  // compute all to one distance
  diskann_id_t medoid_id = kInvalidId;
  float min_dist = std::numeric_limits<float>::max();

  switch (type) {
    case IndexMeta::DataType::DT_FP32:
      for (size_t id = 0; id < entity_.doc_cnt(); id++) {
        const float *data_ptr =
            reinterpret_cast<const float *>(entity_.get_vector(id));

        float dist = 0;
        for (size_t i = 0; i < dimension; i++) {
          float diff = (centroid_data_ptr[i] - data_ptr[i]) *
                       (centroid_data_ptr[i] - data_ptr[i]);
          dist += diff;
        }

        if (dist < min_dist) {
          min_dist = dist;
          medoid_id = id;
        }
      }
      break;
    case IndexMeta::DataType::DT_FP16:
      for (size_t id = 0; id < entity_.doc_cnt(); id++) {
        const ailego::Float16 *data_ptr =
            reinterpret_cast<const ailego::Float16 *>(entity_.get_vector(id));

        float dist = 0;
        for (size_t i = 0; i < dimension; i++) {
          float diff = (centroid_data_ptr[i] - data_ptr[i]) *
                       (centroid_data_ptr[i] - data_ptr[i]);
          dist += diff;
        }

        if (dist < min_dist) {
          min_dist = dist;
          medoid_id = id;
        }
      }
      break;
  }

  (*entity_.mutable_medoid()) = medoid_id;

  LOG_INFO("Medroid Calculation Done. ID: %lu",
           static_cast<uint64_t>(medoid_id));

  return 0;
}

int DiskAnnBuilder::calculate_pq_chunk_num() {
  size_t doc_cnt = holder_->count();
  if (doc_cnt == 0) {
    LOG_ERROR("Invalid Input. Empty Vecs.");

    return IndexError_InvalidLength;
  }

  if (memory_limit_set_) {
    size_t memory_limit_bytes = get_memory_in_bytes(memory_limit_);
    size_t pq_chunk_num = std::floor(memory_limit_bytes / doc_cnt);
    if (pq_chunk_num <= 0) {
      LOG_ERROR("Insufficient memory limit for vec, memory: %zu, vec num: %zu",
                memory_limit_bytes, doc_cnt);
      return IndexError_InvalidArgument;
    }
  }

  pq_chunk_num_ =
      pq_chunk_num_ < max_pq_chunk_num_ ? pq_chunk_num_ : max_pq_chunk_num_;
  if (pq_chunk_num_ > meta_.dimension()) {
    LOG_ERROR("PQ Chunk Num is more than dimension, chunk num: %u, dim: %u",
              pq_chunk_num_, meta_.dimension());
    return IndexError_InvalidArgument;
  }

  LOG_INFO("Quantizing %u dimension data into %u bytes.", meta_.dimension(),
           pq_chunk_num_);

  return 0;
}

int DiskAnnBuilder::build_internal(IndexThreads::Pointer threads) {
  auto task_group = threads->make_group();
  if (!task_group) {
    LOG_ERROR("Failed to create task group");
    return IndexError_Runtime;
  }

  std::atomic<uint64_t> finished{0};
  for (size_t i = 0; i < threads->count(); ++i) {
    task_group->submit(ailego::Closure ::New(this, &DiskAnnBuilder::do_build, i,
                                             threads->count(), &finished));
  }

  while (!task_group->is_finished()) {
    std::unique_lock<std::mutex> lk(mutex_);
    cond_.wait_until(lk, std::chrono::system_clock::now() +
                             std::chrono::seconds(check_interval_secs_));
    if (error_.load(std::memory_order_acquire)) {
      LOG_ERROR("Failed to build index while waiting finish");
      return errcode_;
    }
    LOG_INFO("Built cnt %lu, finished percent %.3f%%", finished.load(),
             finished.load() * 100.0f / entity_.doc_cnt());
  }
  if (error_.load(std::memory_order_acquire)) {
    LOG_ERROR("Failed to build index while waiting finish");
    return errcode_;
  }
  task_group->wait_finish();

  return 0;
}

int DiskAnnBuilder::prune_internal(IndexThreads::Pointer threads) {
  auto task_group = threads->make_group();
  if (!task_group) {
    LOG_ERROR("Failed to create task group");
    return IndexError_Runtime;
  }

  std::atomic<uint64_t> finished{0};
  for (size_t i = 0; i < threads->count(); ++i) {
    task_group->submit(ailego::Closure ::New(this, &DiskAnnBuilder::do_prune, i,
                                             threads->count(), &finished));
  }

  while (!task_group->is_finished()) {
    std::unique_lock<std::mutex> lk(mutex_);
    cond_.wait_until(lk, std::chrono::system_clock::now() +
                             std::chrono::seconds(check_interval_secs_));
    if (error_.load(std::memory_order_acquire)) {
      LOG_ERROR("Failed to purne index while waiting finish");
      return errcode_;
    }
    LOG_INFO("Prune cnt %lu, finished percent %.3f%%", finished.load(),
             finished.load() * 100.0f / entity_.doc_cnt());
  }
  if (error_.load(std::memory_order_acquire)) {
    LOG_ERROR("Failed to prune index while waiting finish");
    return errcode_;
  }
  task_group->wait_finish();

  return 0;
}

int DiskAnnBuilder::train_quantized_data(
    IndexThreads::Pointer threads) {
  LOG_INFO("Starting Train: Chunk Num: %u", pq_chunk_num_);

  ailego::ElapsedTime timer;
  int ret = algo_->train_quantized_data(
      threads, holder_, meta_, entity_.pq_full_pivot_data(),
      entity_.pq_centroid(), entity_.pq_chunk_offsets(), pq_chunk_num_);
  if (ret != 0) {
    LOG_ERROR("Train Quantized Data Error, ret=%d", ret);
    return ret;
  }

  size_t pq_time = timer.milli_seconds();
  LOG_INFO("Train Quantized Data Done, time: %zu ms", pq_time);

  (*entity_.mutable_pq_meta()).full_pivot_data_size =
      entity_.pq_full_pivot_data().size();
  (*entity_.mutable_pq_meta()).centroid_data_size =
      entity_.pq_centroid().size();
  (*entity_.mutable_pq_meta()).chunk_num = pq_chunk_num_;

  return 0;
}

int DiskAnnBuilder::generate_quantized_data(
    IndexThreads::Pointer threads) {
  LOG_INFO("Starting PQ Generate: Query Memory Limit: %lf, Chunk Num: %u",
           memory_limit_, pq_chunk_num_);

  ailego::ElapsedTime timer;
  int ret = algo_->generate_quantized_data(
      threads, holder_, meta_, entity_.pq_centroid(),
      entity_.block_compressed_data(), pq_chunk_num_);
  if (ret != 0) {
    LOG_ERROR("Generate Quantized Data Error, ret=%d", ret);
    return ret;
  }

  size_t pq_time = timer.milli_seconds();
  LOG_INFO("Generate Quantized Data Done, time: %zu ms", pq_time);

  return 0;
}

void DiskAnnBuilder::do_build(uint64_t idx, size_t step_size,
                              std::atomic<uint64_t> *finished) {
  AILEGO_DEFER([&]() {
    std::lock_guard<std::mutex> latch(mutex_);
    cond_.notify_one();
  });

  DiskAnnContext *ctx = new (std::nothrow) DiskAnnContext(
      meta_, metric_,
      std::shared_ptr<DiskAnnEntity>(&entity_, [](DiskAnnEntity *) {}));

  if (ailego_unlikely(ctx == nullptr)) {
    if (!error_.exchange(true)) {
      LOG_ERROR("Failed to create context");
      errcode_ = IndexError_NoMemory;
    }
    return;
  }

  ctx->init(DiskAnnContext::kBuilderContext, max_degree_, pq_chunk_num_,
            meta_.element_size());
  ctx->set_list_size(list_size_);

  DiskAnnContext::Pointer auto_ptr(ctx);
  IndexQueryMeta qmeta(meta_.data_type(), meta_.dimension());
  for (uint64_t id = idx; id < entity_.doc_cnt(); id += step_size) {
    ctx->reset_query(entity_.get_vector(id));
    int ret = algo_->add_node(id, ctx);
    if (ailego_unlikely(ret != 0)) {
      if (!error_.exchange(true)) {
        LOG_ERROR("DiskAnn graph add node failed");
        errcode_ = ret;
      }
      return;
    }
    ctx->clear();
    (*finished)++;
  }
}

void DiskAnnBuilder::do_prune(uint64_t idx, size_t step_size,
                              std::atomic<uint64_t> *finished) {
  AILEGO_DEFER([&]() {
    std::lock_guard<std::mutex> latch(mutex_);
    cond_.notify_one();
  });

  DiskAnnContext *ctx = new (std::nothrow) DiskAnnContext(
      meta_, metric_,
      std::shared_ptr<DiskAnnEntity>(&entity_, [](DiskAnnEntity *) {}));

  if (ailego_unlikely(ctx == nullptr)) {
    if (!error_.exchange(true)) {
      LOG_ERROR("Failed to create context");
      errcode_ = IndexError_NoMemory;
    }
    return;
  }

  ctx->init(DiskAnnContext::kBuilderContext, max_degree_, pq_chunk_num_,
            meta_.element_size());
  ctx->set_list_size(list_size_);

  DiskAnnContext::Pointer auto_ptr(ctx);
  IndexQueryMeta qmeta(meta_.data_type(), meta_.dimension());
  for (uint64_t id = idx; id < entity_.doc_cnt(); id += step_size) {
    ctx->reset_query(entity_.get_vector(id));
    int ret = algo_->prune_node(id, ctx);
    if (ailego_unlikely(ret != 0)) {
      if (!error_.exchange(true)) {
        LOG_ERROR("DiskAnn graph add node failed");
        errcode_ = ret;
      }
      return;
    }
    ctx->clear();
    (*finished)++;
  }
}

int DiskAnnBuilder::train(const IndexTrainer::Pointer & /*trainer*/) {
  if (state_ != BUILD_STATE_INITED) {
    LOG_ERROR("Init the builder before DiskAnnBuilder::train");
    return IndexError_NoReady;
  }

  LOG_INFO("Begin DiskAnnBuilder::train by trainer");

  stats_.set_trained_count(0UL);
  stats_.set_trained_costtime(0UL);
  state_ = BUILD_STATE_TRAINED;

  LOG_INFO("End DiskAnnBuilder::train by trainer");

  return 0;
}

int DiskAnnBuilder::train(IndexThreads::Pointer threads,
                          IndexHolder::Pointer holder) {
  if (state_ != BUILD_STATE_INITED) {
    LOG_ERROR("Init the builder before DiskAnnBuilder::train");
    return IndexError_NoReady;
  }

  LOG_INFO("Begin DiskAnnBuilder::train");

  auto start_time = ailego::Monotime::MilliSeconds();

  holder_ = holder;

  LOG_INFO("Start to calculate chunk num");
  int ret = calculate_pq_chunk_num();
  if (ailego_unlikely(ret != 0)) {
    return ret;
  }

  if (!threads) {
    threads = std::make_shared<SingleQueueIndexThreads>(
        build_thread_count_, false);
    if (!threads) {
      return IndexError_NoMemory;
    }
  }

  ret = train_quantized_data(threads);
  if (ailego_unlikely(ret != 0)) {
    return ret;
  }

  stats_.set_trained_count(holder_->count());

  stats_.set_trained_costtime(ailego::Monotime::MilliSeconds() - start_time);

  state_ = BUILD_STATE_TRAINED;

  holder_.reset();

  LOG_INFO("End DiskAnnBuilder::train");

  return 0;
}

int DiskAnnBuilder::do_norm(const void *data_ptr, std::string *norm_data) {
  float norm_pt = std::numeric_limits<float>::epsilon();

  const float *float_data_ptr = reinterpret_cast<const float *>(data_ptr);

  norm_data->resize(meta_.dimension() * sizeof(float));
  float *output_buf = reinterpret_cast<float *>(&((*norm_data)[0]));

  for (uint32_t dim = 0; dim < meta_.dimension(); dim++) {
    norm_pt += *(float_data_ptr + dim) * *(float_data_ptr + dim);
  }
  norm_pt = std::sqrt(norm_pt);

  for (uint32_t dim = 0; dim < meta_.dimension(); dim++) {
    *(output_buf + dim) = *(float_data_ptr + dim) / norm_pt;
  }

  return 0;
}

int DiskAnnBuilder::build(IndexThreads::Pointer threads,
                          IndexHolder::Pointer holder) {
  LOG_INFO("Start DiskAnnBuilder::build");

  auto start_time = ailego::Monotime::MilliSeconds();

  holder_ = holder;

  if (!threads) {
    threads = std::make_shared<SingleQueueIndexThreads>(
        build_thread_count_, false);
    if (!threads) {
      return IndexError_NoMemory;
    }
  }

  auto iter = holder->create_iterator();
  if (!iter) {
    LOG_ERROR("Create iterator for holder failed");
    return IndexError_Runtime;
  }

  int ret = entity_.reserve_space(holder->count());

  error_ = false;
  while (iter->is_valid()) {
#if 0
    // do norm for cosine
    // std::string norm_data;
    // do_norm(iter->data(), &norm_data);

    // ret = entity_.add_vector(iter->key(), norm_data.data());
#endif
    // uint16_t fp16_data0 = ailego::FloatHelper::ToFP16(1 / 100.0f);
    // float float_data0 = ailego::FloatHelper::ToFP32(fp16_data0);

    // std::cout << "data: " << float_data0 << std::endl;

    // uint16_t fp16_data = *(uint16_t *)iter->data();
    // float float_data = ailego::FloatHelper::ToFP32(fp16_data);
    // std::cout << "key: " << iter->key() << ", data: " << float_data << ",
    // data raw: " << fp16_data << std::endl;

    ret = entity_.add_vector(iter->key(), iter->data());
    if (ailego_unlikely(ret != 0)) {
      return ret;
    }

    iter->next();
  }

  LOG_INFO("Finished saving vector");

  LOG_INFO("Start to calculate entrypoint");
  ret = calculate_entry_point();
  if (ailego_unlikely(ret != 0)) {
    return ret;
  }

  LOG_INFO("Start to build vamana graph");
  // auto test_threads = std::make_shared<SingleQueueIndexThreads>(1,
  // false);
  // ret = build_internal(test_threads);
  ret = build_internal(threads);
  if (ret != 0) {
    return ret;
  }

  LOG_INFO("Start final cleanup..");
  // ret = prune_internal(test_threads);
  ret = prune_internal(threads);
  if (ret != 0) {
    return ret;
  }

  LOG_INFO("Start to generate quantized data");
  // auto test_threads = std::make_shared<SingleQueueIndexThreads>(1,
  // false); ret = generate_quantized_data(test_threads);
  ret = generate_quantized_data(threads);
  if (ailego_unlikely(ret != 0)) {
    return ret;
  }

  state_ = BUILD_STATE_BUILT;

  stats_.set_built_count(entity_.doc_cnt());
  stats_.set_built_costtime(ailego::Monotime::MilliSeconds() - start_time);

  LOG_INFO("End DiskAnnBuilder::build");

  return 0;
}

int DiskAnnBuilder::dump(const IndexDumper::Pointer &dumper) {
  if (state_ != BUILD_STATE_BUILT) {
    LOG_INFO("Build the index before HnswBuilder::dump");
    return IndexError_NoReady;
  }

  LOG_INFO("Begin DiskAnnBuilder::dump");

  meta_.set_searcher("DiskAnnSearcher", 0, ailego::Params());
  auto start_time = ailego::Monotime::MilliSeconds();

  int ret = IndexHelper::SerializeToDumper(meta_, dumper.get());
  if (ret != 0) {
    LOG_ERROR("Failed to serialize meta into dumper.");
    return ret;
  }

  ret = entity_.dump(holder_, meta_, dumper);
  if (ret != 0) {
    LOG_ERROR("Index dump failed, ret: %u", ret);

    return IndexError_Runtime;
  }

  stats_.set_dumped_count(holder_->count());
  stats_.set_dumped_costtime(ailego::Monotime::MilliSeconds() - start_time);

  LOG_INFO("DiskAnnBuilder::dump");

  return 0;
}

INDEX_FACTORY_REGISTER_BUILDER(DiskAnnBuilder);

}  // namespace core
}  // namespace zvec
