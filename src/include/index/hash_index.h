//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_index.h
//
// Identification: src/include/index/hash_index.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>

#include "catalog/manager.h"
#include "common/types.h"
#include "index/index.h"

#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace index {

/**
 * Using libcuckoo for hash index
 *
 * @see Index
 */
template <typename KeyType, typename ValueType, class KeyHasher,
class KeyComparator, class KeyEqualityChecker>
class HashIndex : public Index {
  friend class IndexFactory;

  // Define the container type
  typedef cuckoohash_map<KeyType, std::vector<ValueType>, KeyHasher,
      KeyEqualityChecker> MapType;

 public:
  HashIndex(IndexMetadata *metadata);

  ~HashIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer &location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  bool CondInsertEntry(const storage::Tuple *key, const ItemPointer &location,
                       std::function<bool(const ItemPointer &)> predicate);

  void Scan(const std::vector<Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &exprs,
            const ScanDirectionType &scan_direction,
            std::vector<ItemPointer> &result);

  void ScanAllKeys(std::vector<ItemPointer> &result);

  void ScanKey(const storage::Tuple *key, std::vector<ItemPointer> &result);

  std::string GetTypeName() const;

  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() { return 0; }

  bool NeedGC() {
    return false;
  }

  void PerformGC() {
    return;
  }

 protected:
  MapType container;

  // equality checker and comparator
  KeyHasher hasher;
  KeyEqualityChecker equals;
  KeyComparator comparator;
};

}  // End index namespace
}  // End peloton namespace

