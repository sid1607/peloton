//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_index.cpp
//
// Identification: src/index/hash_index.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "index/hash_index.h"
#include "index/index_key.h"
#include "common/logger.h"
#include "storage/tuple.h"

namespace peloton {
namespace index {

template <typename KeyType, typename ValueType, class KeyHasher,
class KeyComparator, class KeyEqualityChecker>
HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
KeyEqualityChecker>::HashIndex(IndexMetadata *metadata)
: Index(metadata),
  container(KeyHasher(), KeyEqualityChecker()),
  hasher(),
  equals(),
  comparator() { }


template <typename KeyType, typename ValueType, class KeyHasher,
class KeyComparator, class KeyEqualityChecker>
HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
KeyEqualityChecker>::~HashIndex() {
  // Nothing to do here !
}

template <typename KeyType, typename ValueType, class KeyHasher,
class KeyComparator, class KeyEqualityChecker>
bool HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
KeyEqualityChecker>::InsertEntry(const storage::Tuple *key,
                                 const ItemPointer &location) {
  KeyType index_key;

  index_key.SetFromKey(key);

  std::vector<ValueType> val;
  val.push_back(location);

  // if there's no key in the hash map, then insert a vector containing location.
  // otherwise, directly insert location into the vector that already exists in the hash map.
  auto update_function = [location](std::vector<ItemPointer> &existing_vector){
    existing_vector.push_back(location);
  };

  container.upsert(index_key, update_function, val);

  return true;
}

struct ItemPointerEqualityChecker {
  ItemPointer arg_;
  ItemPointerEqualityChecker(ItemPointer arg) : arg_(arg) {}
  bool operator()(ItemPointer other) {
    return other.block == arg_.block && other.offset == arg_.offset;
  }
};

template <typename KeyType, typename ValueType, class KeyHasher,
class KeyComparator, class KeyEqualityChecker>
bool HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
KeyEqualityChecker>::DeleteEntry(const storage::Tuple *key,
                                 const ItemPointer &location) {
  KeyType index_key;

  index_key.SetFromKey(key);

  // TODO: add retry logic
  auto delete_function = [location](std::vector<ItemPointer> &existing_vector) {
    existing_vector.erase(std::remove_if(existing_vector.begin(),
                                         existing_vector.end(),
                                         ItemPointerEqualityChecker(location)),
                          existing_vector.end());
  };

  container.update_fn(index_key, delete_function);

  return true;
}

template <typename KeyType, typename ValueType, class KeyHasher,
class KeyComparator, class KeyEqualityChecker>
bool HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
KeyEqualityChecker>::CondInsertEntry(
    const storage::Tuple *key, const ItemPointer &location,
    UNUSED_ATTRIBUTE std::function<bool(const ItemPointer &)> predicate) {

  KeyType index_key;
  index_key.SetFromKey(key);

  std::vector<ValueType> val;
  val.push_back(location);

  auto cond_insert_function = [location](std::vector<ItemPointer> &existing_vector){
    existing_vector.push_back(location);
  };

  container.upsert(index_key, cond_insert_function, val);

  return true;
}

template <typename KeyType, typename ValueType, class KeyHasher,
class KeyComparator, class KeyEqualityChecker>
void HashIndex<KeyType, ValueType, KeyHasher, KeyComparator, 
KeyEqualityChecker>::Scan(const std::vector<Value> &values,
                          const std::vector<oid_t> &key_column_ids,
                          const std::vector<ExpressionType> &expr_types,
                          UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
                          std::vector<ItemPointer> &result) {
  KeyType index_key;
  std::unique_ptr<storage::Tuple> start_key;
  start_key.reset(new storage::Tuple(metadata->GetKeySchema(), true));

  bool all_constraints_are_equal = ConstructLowerBoundTuple(
      start_key.get(), values, key_column_ids, expr_types);
  if (all_constraints_are_equal == false) {
    LOG_ERROR("not all constraints are equal!");
    assert(false);
  }

  index_key.SetFromKey(start_key.get());

  std::vector<ItemPointer> find_values;
  bool ret = container.find(index_key, find_values);

  if (ret == true) {
    for (auto entry : find_values) {
      result.push_back(entry);
    }
  }

}

template <typename KeyType, typename ValueType, class KeyHasher,
class KeyComparator, class KeyEqualityChecker>
void HashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanAllKeys(
    std::vector<ItemPointer> &result) {

  {
    auto lt = container.lock_table();
    for (const auto &itr : lt) {
      for (const auto entry : itr.second) {
        result.push_back(entry);
      }
    }
  }

}

/**
 * @brief Return all locations related to this key.
 */
template <typename KeyType, typename ValueType, class KeyHasher,
class KeyComparator, class KeyEqualityChecker>
void HashIndex<KeyType, ValueType, KeyHasher, KeyComparator, KeyEqualityChecker>::ScanKey(
    const storage::Tuple *key,
    std::vector<ItemPointer> &result) {
  KeyType index_key;
  index_key.SetFromKey(key);

  std::vector<ItemPointer> find_values;
  bool ret = container.find(index_key, find_values);

  if (ret == true) {
    for (auto entry : find_values) {
      result.push_back(entry);
    }
  }

}

template <typename KeyType, typename ValueType, class KeyHasher,
class KeyComparator, class KeyEqualityChecker>
std::string HashIndex<KeyType, ValueType, KeyHasher, KeyComparator,
KeyEqualityChecker>::GetTypeName() const {
  return "Hash";
}

// Explicit template instantiation

// Ints key
template class HashIndex<IntsKey<1>, ItemPointer, IntsHasher<1>,
IntsComparator<1>, IntsEqualityChecker<1>>;
template class HashIndex<IntsKey<2>, ItemPointer, IntsHasher<2>,
IntsComparator<2>, IntsEqualityChecker<2>>;
template class HashIndex<IntsKey<3>, ItemPointer, IntsHasher<3>,
IntsComparator<3>, IntsEqualityChecker<3>>;
template class HashIndex<IntsKey<4>, ItemPointer, IntsHasher<4>,
IntsComparator<4>, IntsEqualityChecker<4>>;

// Generic key
template class HashIndex<GenericKey<4>, ItemPointer, GenericHasher<4>,
GenericComparator<4>, GenericEqualityChecker<4>>;
template class HashIndex<GenericKey<8>, ItemPointer, GenericHasher<8>,
GenericComparator<8>, GenericEqualityChecker<8>>;
template class HashIndex<GenericKey<16>, ItemPointer, GenericHasher<16>,
GenericComparator<16>, GenericEqualityChecker<16>>;
template class HashIndex<GenericKey<64>, ItemPointer, GenericHasher<64>,
GenericComparator<64>, GenericEqualityChecker<64>>;
template class HashIndex<GenericKey<256>, ItemPointer, GenericHasher<256>,
GenericComparator<256>, GenericEqualityChecker<256>>;

// Tuple key
template class HashIndex<TupleKey, ItemPointer, TupleKeyHasher,
TupleKeyComparator, TupleKeyEqualityChecker>;

}  // End index namespace
}  // End peloton namespace
