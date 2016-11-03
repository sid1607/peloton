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

TEST_F(NumaPoolTest, BasicTest) {
	NumaThreadPool numa_thread_pool;
	int num_cpus = std::thread::hardware_concurrency();
	numa_thread_pool.Initialize(num_cpus);

	std::atomic<int> counter(0);

	for (int i=0; i<=numa_max_node(); i++) {
		std::shared_ptr<TaskArg> task_arg(new TaskArg(i));
		task_arg->self = task_arg.get();
		numa_thread_pool.SubmitTask(i,
																[](std::atomic<int> *ctr) {
																	// auto cpuID = sched_getcpu();
																  // EXPECT_EQ((*arg)->socket_id, numa_node_of_cpu(cpuID));
																	ctr->fetch_add(1);
																}, &counter);
	}

	numa_thread_pool.Shutdown();

	// Wait for the test to finish
	usleep(1000);

	EXPECT_EQ(counter.load(), std::thread::hardware_concurrency());

}

}  // End test namespace
}  // End peloton namespace