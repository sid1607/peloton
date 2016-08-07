//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_inlining_test.cpp
//
// Identification: test/common/value_inlining_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <cfloat>
#include <limits>

#include "common/harness.h"

#include "common/value.h"
#include "common/value_factory.h"

#include "benchmark/tpcc/tpcc_loader.h"
#include "benchmark/tpcc/tpcc_configuration.h"
#include "catalog/manager.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/insert_executor.h"
#include "executor/executor_context.h"
#include "expression/constant_value_expression.h"
#include "expression/expression_util.h"
#include "index/index_factory.h"
#include "planner/insert_plan.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "storage/database.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Value Inlining Tests
//===--------------------------------------------------------------------===//

class ValueInliningTest : public PelotonTest {};

const bool is_inlined = false;

const size_t name_length = 32;
const size_t data_length = 32;

double item_min_price = 1.0;
double item_max_price = 100.0;

const bool own_schema = true;
const bool adapt_table = false;
const bool unique_index = false;
const bool allocate = true;

static const oid_t tpcc_database_oid = 100;

static const oid_t item_table_oid = 1003;
static const oid_t item_table_pkey_index_oid = 20030;  // I_ID

storage::DataTable *item_table;

void CreateItemTable() {
  /*
   CREATE TABLE ITEM (
   I_ID INTEGER DEFAULT '0' NOT NULL,
   I_IM_ID INTEGER DEFAULT NULL,
   I_NAME VARCHAR(32) DEFAULT NULL,
   I_PRICE FLOAT DEFAULT NULL,
   I_DATA VARCHAR(64) DEFAULT NULL,
   CONSTRAINT I_PK_ARRAY PRIMARY KEY (I_ID)
   );
   */

  // Create schema first
  std::vector<catalog::Column> item_columns;

  auto i_id_column = catalog::Column(
      VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "I_ID", is_inlined);
  item_columns.push_back(i_id_column);
  auto i_im_id_column =
      catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                      "I_IM_ID", is_inlined);
  item_columns.push_back(i_im_id_column);
  auto i_name_column =
      catalog::Column(VALUE_TYPE_VARCHAR, name_length, "I_NAME", is_inlined);
  item_columns.push_back(i_name_column);
  auto i_price_column = catalog::Column(
      VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE), "I_PRICE", is_inlined);
  item_columns.push_back(i_price_column);
  auto i_data_column =
      catalog::Column(VALUE_TYPE_VARCHAR, data_length, "I_DATA", is_inlined);
  item_columns.push_back(i_data_column);

  catalog::Schema *table_schema = new catalog::Schema(item_columns);
  std::string table_name("ITEM");

  item_table = storage::TableFactory::GetDataTable(
      tpcc_database_oid, item_table_oid, table_schema, table_name,
      DEFAULT_TUPLES_PER_TILEGROUP, own_schema, adapt_table);

  // Primary index on I_ID
  std::vector<oid_t> key_attrs = {0};

  auto tuple_schema = item_table->GetSchema();
  catalog::Schema *key_schema =
      catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);
  bool unique = true;

  index::IndexMetadata *index_metadata = new index::IndexMetadata(
      "item_pkey",
      item_table_pkey_index_oid,
      INDEX_TYPE_HASH,
      INDEX_CONSTRAINT_TYPE_DEFAULT,
      tuple_schema,
      key_schema,
      key_attrs,
      unique);

  std::shared_ptr<index::Index> pkey_index(index::IndexFactory::GetInstance(index_metadata));
  item_table->AddIndex(pkey_index);
}

std::random_device rd;
std::mt19937 rng(rd());

std::string GetRandomAlphaNumericString(const size_t string_length) {
  const char alphanumeric[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  std::uniform_int_distribution<> dist(0, sizeof(alphanumeric) - 1);

  char repeated_char = alphanumeric[dist(rng)];
  std::string sample(string_length, repeated_char);
  return sample;
}

double GetRandomDouble(const double lower_bound, const double upper_bound) {
  std::uniform_real_distribution<> dist(lower_bound, upper_bound);

  double sample = dist(rng);
  return sample;
}

std::unique_ptr<storage::Tuple> BuildItemTuple(
    const int item_id, const std::unique_ptr<VarlenPool> &pool) {
  auto item_table_schema = item_table->GetSchema();
  std::unique_ptr<storage::Tuple> item_tuple(
      new storage::Tuple(item_table_schema, allocate));

  // I_ID
  item_tuple->SetValue(0, ValueFactory::GetIntegerValue(item_id), nullptr);
  // I_IM_ID
  item_tuple->SetValue(1, ValueFactory::GetIntegerValue(item_id * 10), nullptr);
  // I_NAME
  auto i_name = GetRandomAlphaNumericString(name_length);
  item_tuple->SetValue(2, ValueFactory::GetStringValue(i_name), pool.get());
  // I_PRICE
  double i_price = GetRandomDouble(item_min_price, item_max_price);
  item_tuple->SetValue(3, ValueFactory::GetDoubleValue(i_price), nullptr);
  // I_DATA
  auto i_data = GetRandomAlphaNumericString(data_length);
  item_tuple->SetValue(4, ValueFactory::GetStringValue(i_data), pool.get());

  return item_tuple;
}

void LoadItems() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  std::size_t item_count = 100;                    // 100000

  for (std::size_t item_itr = 0; item_itr < item_count; item_itr++) {
    auto item_tuple = BuildItemTuple(item_itr, pool);
    planner::InsertPlan node(item_table, std::move(item_tuple));
    executor::InsertExecutor executor(&node, context.get());
    executor.Execute();
  }

  txn_manager.CommitTransaction();
}

TEST_F(ValueInliningTest, ItemsTable) {

  CreateItemTable();

  LoadItems();

  LOG_INFO("item count = %lu", item_table->GetTupleCount());

}


}  // End test namespace
}  // End peloton namespace
