//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// numa_pool_test.cpp
//
// Identification: test/common/numa_pool_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/numa_thread_pool.h"
#include "common/harness.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// NUMA Thread Pool Test
//===--------------------------------------------------------------------===//

class NumaPoolTest : public PelotonTest {};

struct TaskArg {
	const int socket_id;
	TaskArg *self;
	TaskArg(const int &i) : socket_id(i) {};
};

void run(TaskArg **arg, std::atomic<int> *ctr) {
	auto cpuID = sched_getcpu();
	EXPECT_EQ((*arg)->socket_id, numa_node_of_cpu(cpuID));
	ctr->fetch_add(1);
}

TEST_F(NumaPoolTest, BasicTest) {
	NumaThreadPool numa_thread_pool;
	int num_cpus = std::thread::hardware_concurrency();
	int num_cores_per_socket = num_cpus/(numa_max_node()+1);

	numa_thread_pool.Initialize(num_cpus);
	std::atomic<int> counter(0);

	for (int i=0; i<=numa_max_node(); i++) {
		std::shared_ptr<TaskArg> task_arg(new TaskArg(i));
		task_arg->self = task_arg.get();
		for (int j=0; j<num_cores_per_socket; j++) {
			numa_thread_pool.SubmitTask(i, run, &task_arg->self, &counter);
		}
	}

	// Wait for the test to finish
	while (counter.load() != num_cpus) {
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	numa_thread_pool.Shutdown();
}

}  // End test namespace
}  // End peloton namespace