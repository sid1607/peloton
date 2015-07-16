/*-------------------------------------------------------------------------
 *
 * manager.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/manager.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/catalog/catalog.h"
#include "backend/catalog/manager.h"

namespace peloton {
namespace catalog {

Manager& Manager::GetInstance() {
  static Manager manager;
  return manager;
}

void Manager::SetLocation(const oid_t oid, void *location) {
  locator.insert(std::pair<oid_t, void*>(oid, location));
}

void *Manager::GetLocation(const oid_t oid) const {
  void *location = nullptr;
  try {
    location = locator.at(oid);
  }
  catch(std::exception& e) {
    // FIXME
  }
  return location;
}

catalog::Database *Manager::GetDatabase(const oid_t database_id) const {
  auto& catalog = catalog::Catalog::GetInstance();

  // Lookup DB
  catalog::Database *database = catalog.GetDatabase(database_id);

  return database;
}

catalog::Table *Manager::GetTable(const oid_t database_id,
                                  const oid_t table_id) const {
  auto& catalog = catalog::Catalog::GetInstance();

  // Lookup DB
  catalog::Database *database = catalog.GetDatabase(database_id);

  // Lookup table
  if(database != nullptr) {
    catalog::Table *table = database->GetTable(table_id);
    return table;
  }

  return nullptr;
}

catalog::Index *Manager::GetIndex(const oid_t database_id,
                                  const oid_t table_id,
                                  const oid_t index_id) const {
  auto& catalog = catalog::Catalog::GetInstance();

  // Lookup DB
  catalog::Database *database = catalog.GetDatabase(database_id);

  // Lookup table
  if(database != nullptr) {
    catalog::Table *table = database->GetTable(table_id);

    // Get index
    if(table != nullptr)
      return table->GetIndex(index_id);
  }

  return nullptr;
}

catalog::Schema *Manager::GetSchema(const oid_t database_id,
                                    const oid_t table_id) const {
  auto& catalog = catalog::Catalog::GetInstance();

  // Lookup DB
  catalog::Database *database = catalog.GetDatabase(database_id);

  // Lookup table
  if(database != nullptr) {
    catalog::Table *table = database->GetTable(table_id);

    // Get schema
    if(table != nullptr)
      return table->GetSchema();
  }

  return nullptr;
}

} // End catalog namespace
} // End peloton namespace



