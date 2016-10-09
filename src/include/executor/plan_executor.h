//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_executor.h
//
// Identification: src/include/executor/plan_executor.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/statement.h"
#include "common/types.h"
#include "executor/abstract_executor.h"
#include "boost/thread/future.hpp"
#include "concurrency/transaction_manager_factory.h"


namespace peloton {
namespace bridge {

//===--------------------------------------------------------------------===//
// Plan Executor
//===--------------------------------------------------------------------===//

typedef struct peloton_status {
  peloton::Result m_result;
  int *m_result_slots;

  // number of tuples processed
  uint32_t m_processed;

  peloton_status() {
    m_processed = 0;
    m_result = peloton::RESULT_SUCCESS;
    m_result_slots = nullptr;
  }

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//
  bool SerializeTo(peloton::SerializeOutput &output);
  bool DeserializeFrom(peloton::SerializeInput &input);

} peloton_status;

/*
 * Struct to hold parameters used by the exchange operator
 */
struct ExchangeParams {
  boost::promise<bridge::peloton_status> p;
  boost::unique_future<bridge::peloton_status> f;
  std::vector<ResultType> result;
  concurrency::Transaction *txn;
  const planner::AbstractPlan *plan;
  const std::vector<common::Value *> params;
  int parallelism_count, partition_id;
  const std::vector<int> result_format;
  bool init_failure;
  ExchangeParams *self;

  inline ExchangeParams(concurrency::Transaction* txn,
                        const planner::AbstractPlan *plan,
                        const std::vector<common::Value *>& params,
                        int parallelism_count, int partition_id,
                        const std::vector<int> &result_format)
      : txn(txn), plan(plan), params(params),
        parallelism_count(parallelism_count), partition_id(partition_id),
        result_format(result_format), init_failure(false) {
    f = p.get_future();
  }
};


class PlanExecutor {
 public:
  PlanExecutor(const PlanExecutor &) = delete;
  PlanExecutor &operator=(const PlanExecutor &) = delete;
  PlanExecutor(PlanExecutor &&) = delete;
  PlanExecutor &operator=(PlanExecutor &&) = delete;

  PlanExecutor(){};

  static void PrintPlan(const planner::AbstractPlan *plan,
                        std::string prefix = "");

  // Copy From
  static inline void copyFromTo(const std::string &src,
                                std::vector<unsigned char> &dst) {
    if (src.c_str() == nullptr) {
      return;
    }
    size_t len = src.size();
    for (unsigned int i = 0; i < len; i++) {
      dst.push_back((unsigned char)src[i]);
    }
  }

  /* TODO: Delete this method
    static peloton_status ExecutePlan(const planner::AbstractPlan *plan,
                                      ParamListInfo m_param_list,
                                      TupleDesc m_tuple_desc);
  */

  /*
   * @brief Based on the Volcano 'Exchange' intra-query parallelism model.
   * Currently supported only for sequential scans. This operator spawns
   * multiple execution trees (tasks), each deployed on a different executor
   * thread. For now, the operator spawns as many tasks as there are cores
   * in the system. The tiles to be processed are divided into modulo
   * partitions such that each thread gets roughly the same amount of work.
   * The intermediate results from each thread are coalesced to give the
   * final output.
   */
  static peloton_status ExchangeOperator(const planner::AbstractPlan *plan,
                               const std::vector<common::Value *> &params,
                               std::vector<ResultType> &result,
                               const std::vector<int> &result_format);

  /*
   * @brief Use std::vector<common::Value *> as params to make it more elegant
   * for
   * networking
   * Before ExecutePlan, a node first receives value list, so we should pass
   * value list directly rather than passing Postgres's ParamListInfo.
   *
   * Also pass parallelism details if we are doing intra-query parallelism
   *
   */
  static void ExecutePlanLocal(ExchangeParams **exchg_params_args);

  /*
   * @brief When a peloton node recvs a query plan in rpc mode,
   * this function is invoked
   * @param plan and params
   * @return the number of tuple it executes and logical_tile_list
   */
  static void ExecutePlan(
      const planner::AbstractPlan *plan,
      const std::vector<common::Value *> &params,
      std::vector<std::unique_ptr<executor::LogicalTile>> &logical_tile_list,
      boost::promise<int> &p);
};

}  // namespace bridge
}  // namespace peloton
