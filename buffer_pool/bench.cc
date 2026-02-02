#include "buffer_pool.h"
#include "mmap_container.h"
#include "malloc_container.h"

#include <random>
#include <algorithm>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <iostream>
#include <filesystem>

template <typename CONTAINER_T>
void warmup(CONTAINER_T& container, size_t page_num, size_t page_size) {
    auto handle = container.get_handle();
    int64_t sum = 0;
    for (size_t i = 0; i < page_num; i++) {
        char* buffer = handle.get_block(i * page_size, page_size);
        if (buffer == nullptr) {
            std::cerr << "Failed to get block for page " << i << std::endl;
        }
        sum += static_cast<int64_t>(buffer[0]);
    }
    std::cout << "Warmup sum: " << sum << std::endl;
}

template <typename CONTAINER_T>
void benchmark(CONTAINER_T& container,
                size_t vec_num_per_req,
                size_t vec_width,
                const std::vector<size_t>& vec_indices,
                int thread_num,
                size_t req_num) {
    std::vector<std::thread> threads;
    std::atomic<size_t> req_idx{0};
    std::atomic<int64_t> sum_result{0};
    auto start = std::chrono::high_resolution_clock::now();
    for (int t = 0; t < thread_num; t++) {
        threads.emplace_back([&]() {
            int64_t local_sum = 0;
            while (true) {
                size_t i = req_idx.fetch_add(1);
                // std::cout << "Processing request " << i << ", " << vec_num_per_req << " vectors" << std::endl;
                if (i >= req_num) {
                    break;
                }
                auto handle = container.get_handle();
                size_t vec_id = i * vec_num_per_req;
                for (size_t j = 0; j < vec_num_per_req; j++) {
                    size_t vec_idx = vec_indices[(vec_id + j) % vec_indices.size()];
                    char* vec_data = handle.get_block(vec_idx * vec_width, vec_width);
                    // Simulate some processing
                    for (size_t k = 0; k < vec_width; k++) {
                        local_sum += static_cast<int>(vec_data[k]);
                    }
                }
            }
            sum_result.fetch_add(local_sum);
        });
    }
    for (auto& th : threads) {
        th.join();
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Sum result: " << sum_result.load() << std::endl;
    double duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0;
    double qps = req_num / duration;
    std::cout << "Total time: " << duration << " seconds, QPS: " << qps << std::endl;
}

int main(int argc, char** argv) {
    std::string filename = argv[1];
    int pool_size_in_gb = atoi(argv[2]);
    size_t vec_width = atoi(argv[3]);
    size_t vec_num_per_req = atoi(argv[4]);
    int thread_num = atoi(argv[5]);

    size_t pool_size = static_cast<size_t>(pool_size_in_gb) * 1024 * 1024 * 1024;

    size_t file_size = std::filesystem::file_size(filename);
    size_t vec_num = file_size / vec_width;
    std::vector<size_t> vec_indices;
    for (size_t i = 0; i < vec_num; i++) {
        vec_indices.push_back(i);
    }

    std::shuffle(vec_indices.begin(), vec_indices.end(), std::mt19937{std::random_device{}()});

    const size_t page_size = 4096;
    const size_t req_num = 100000;
    {
        BufferPool buffer_pool(filename, pool_size);
        size_t buffer_pool_size = buffer_pool.file_size();
        size_t page_num = buffer_pool_size / page_size;
        std::cout << "warmup buffer pool: " << std::endl;
        warmup(buffer_pool, page_num, page_size);
        std::cout << "benchmark buffer pool: " << std::endl;
        benchmark(buffer_pool, vec_num_per_req, vec_width, vec_indices, thread_num, req_num);
        benchmark(buffer_pool, vec_num_per_req, vec_width, vec_indices, thread_num, req_num);
        Counter::get_instance().clear();
        benchmark(buffer_pool, vec_num_per_req, vec_width, vec_indices, thread_num, req_num);
        Counter::get_instance().display();
    }
    {
        MMapContainer mmap_container(filename);
        size_t mmap_container_size = mmap_container.file_size();
        size_t page_num = mmap_container_size / page_size;
        std::cout << "warmup mmap container: " << std::endl;
        warmup(mmap_container, page_num, page_size);
        std::cout << "benchmark mmap container: " << std::endl;
        benchmark(mmap_container, vec_num_per_req, vec_width, vec_indices, thread_num, req_num);
        benchmark(mmap_container, vec_num_per_req, vec_width, vec_indices, thread_num, req_num);
        benchmark(mmap_container, vec_num_per_req, vec_width, vec_indices, thread_num, req_num);
    }
    {
        MallocContainer malloc_container(filename);
        size_t malloc_container_size = malloc_container.file_size();
        size_t page_num = malloc_container_size / page_size;
        std::cout << "warmup malloc container: " << std::endl;
        warmup(malloc_container, page_num, page_size);
        std::cout << "benchmark malloc container: " << std::endl;
        benchmark(malloc_container, vec_num_per_req, vec_width, vec_indices, thread_num, req_num);
        benchmark(malloc_container, vec_num_per_req, vec_width, vec_indices, thread_num, req_num);
        benchmark(malloc_container, vec_num_per_req, vec_width, vec_indices, thread_num, req_num);
    }

    return 0;
}