//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numa_thread_pool.h
//
// Identification: src/include/common/numa_thread_pool.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <numa.h>
#include <unordered_map>
#include "thread_pool.h"

namespace peloton {

// wrapper for a numa thread pool
class NumaThreadPool {
 private:
	const int num_cpus_;
	std::unordered_map<int, ThreadPool> thread_pool_map_;

 private:
	inline void MapCpuToNuma() {

	}

 public:
	inline NumaThreadPool() : num_cpus_(std::thread::hardware_concurrency()) {
		srand(time(NULL));
	};

	// TODO: extend to accept pool_size?
	// Creates a separate thread pool for each NUMA socket
	void Initialize() {
		std::unordered_map<int, std::vector<int>> numa_cpu_map;
		int numa_socket_id;
		for (int i=0; i<num_cpus_; i++) {
			numa_socket_id = numa_node_of_cpu(i);
			numa_cpu_map[numa_socket_id].push_back(i);
		}

		for (auto itr = numa_cpu_map.begin(); itr != numa_cpu_map.end(); itr++) {
			thread_pool_map_[itr->first].IntiializePinned(itr->second);

		}
	}

	// submit task to numa thread pool.
	// it accepts the numa socket ID, function and a set of function
	// parameters as parameters.
	template <typename FunctionType, typename... ParamTypes>
	void SubmitTask(int numa_socket_id, FunctionType &&func,
									const ParamTypes &&... params) {
		// add task to thread pool of given numa socket
		thread_pool_map_[numa_socket_id].SubmitTask(func, params);
	}

	template <typename FunctionType, typename... ParamTypes>
	void SubmitTaskRandom(FunctionType &&func, const ParamTypes &&... params) {
		int rand_index = rand() % thread_pool_map_.size();
		auto random_it = std::next(std::begin(thread_pool_map_), rand_index);
		// submit task to a random numa socket
		random_it->second.SubmitTask(func, params);
	};

	// Shuts down all thread pools one by one
	void Shutdown() {
		for (auto itr = thread_pool_map_.begin(); itr != thread_pool_map_.end();
				 itr+) {
			itr->second.Shutdown();
		}
	}


};
}
