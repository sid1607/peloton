//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_factory.cpp
//
// Identification: src/index/index_factory.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <iostream>

#include "common/types.h"
#include "common/logger.h"
#include "common/macros.h"
#include "index/index_factory.h"
#include "index/index_key.h"
#include "index/btree_index.h"
#include "index/bwtree_index.h"
#include "index/skip_list_index.h"

namespace peloton {
namespace index {

Index *IndexFactory::GetInstance(IndexMetadata *metadata) {

  LOG_TRACE("Creating index %s", metadata->GetName().c_str());
  const auto key_size = metadata->key_schema->GetLength();
  LOG_TRACE("key_size : %d", key_size);

  auto index_type = metadata->GetIndexMethodType();
  LOG_TRACE("Index type : %d", index_type);

  bool ints_only = metadata->IsIntsOnly();
  // no int specialization beyond this point
  if (key_size > sizeof(int64_t) * 4) {
    ints_only = false;
  }

  // TODO: Disable ints only for now
  ints_only = false;
  LOG_TRACE("Ints Only : %d", ints_only);

  if (index_type == INDEX_TYPE_BTREE) {

    if (ints_only && (index_type == INDEX_TYPE_BTREE)) {
      if (key_size <= sizeof(uint64_t)) {
        return new BTreeIndex<IntsKey<1>,
            ItemPointer,
            IntsComparator<1>,
            IntsEqualityChecker<1>>(metadata);
      } else if (key_size <= sizeof(int64_t) * 2) {
        return new BTreeIndex<IntsKey<2>,
            ItemPointer,
            IntsComparator<2>,
            IntsEqualityChecker<2>>(metadata);
      } else if (key_size <= sizeof(int64_t) * 3) {
        return new BTreeIndex<IntsKey<3>,
            ItemPointer,
            IntsComparator<3>,
            IntsEqualityChecker<3>>(metadata);
      } else if (key_size <= sizeof(int64_t) * 4) {
        return new BTreeIndex<IntsKey<4>,
            ItemPointer,
            IntsComparator<4>,
            IntsEqualityChecker<4>>(metadata);
      } else {
        throw IndexException(
            "We currently only support tree index on non-unique "
            "integer keys of size 32 bytes or smaller...");
      }
    }

    if (key_size <= 4) {
      return new BTreeIndex<GenericKey<4>,
          ItemPointer,
          GenericComparator<4>,
          GenericEqualityChecker<4>>(metadata);
    } else if (key_size <= 8) {
      return new BTreeIndex<GenericKey<8>,
          ItemPointer,
          GenericComparator<8>,
          GenericEqualityChecker<8>>(metadata);
    } else if (key_size <= 16) {
      return new BTreeIndex<GenericKey<16>,
          ItemPointer,
          GenericComparator<16>,
          GenericEqualityChecker<16>>(metadata);
    } else if (key_size <= 64) {
      return new BTreeIndex<GenericKey<64>,
          ItemPointer,
          GenericComparator<64>,
          GenericEqualityChecker<64>>(metadata);
    } else if (key_size <= 256) {
      return new BTreeIndex<GenericKey<256>,
          ItemPointer,
          GenericComparator<256>,
          GenericEqualityChecker<256>>(metadata);
    } else {
      return new BTreeIndex<TupleKey,
          ItemPointer,
          TupleKeyComparator,
          TupleKeyEqualityChecker>(metadata);
    }
  }

  if (index_type == INDEX_TYPE_SKIPLIST) {

    if (ints_only && (index_type == INDEX_TYPE_SKIPLIST)) {
      if (key_size <= sizeof(uint64_t)) {
        return new SkipListIndex<IntsKey<1>,
            ItemPointer,
            IntsComparatorRaw<1>,
            IntsEqualityChecker<1>>(metadata);
      } else if (key_size <= sizeof(int64_t) * 2) {
        return new SkipListIndex<IntsKey<2>,
            ItemPointer,
            IntsComparatorRaw<2>,
            IntsEqualityChecker<2>>(metadata);
      } else if (key_size <= sizeof(int64_t) * 3) {
        return new SkipListIndex<IntsKey<3>,
            ItemPointer,
            IntsComparatorRaw<3>,
            IntsEqualityChecker<3>>(metadata);
      } else if (key_size <= sizeof(int64_t) * 4) {
        return new SkipListIndex<IntsKey<4>,
            ItemPointer,
            IntsComparatorRaw<4>,
            IntsEqualityChecker<4>>(metadata);
      } else {
        throw IndexException(
            "We currently only support tree index on non-unique "
            "integer keys of size 32 bytes or smaller...");
      }
    }

    if (key_size <= 4) {
      return new SkipListIndex<GenericKey<4>,
          ItemPointer,
          GenericComparatorRaw<4>,
          GenericEqualityChecker<4>>(metadata);
    } else if (key_size <= 8) {
      return new SkipListIndex<GenericKey<8>,
          ItemPointer,
          GenericComparatorRaw<8>,
          GenericEqualityChecker<8>>(metadata);
    } else if (key_size <= 16) {
      return new SkipListIndex<GenericKey<16>,
          ItemPointer,
          GenericComparatorRaw<16>,
          GenericEqualityChecker<16>>(metadata);
    } else if (key_size <= 64) {
      return new SkipListIndex<GenericKey<64>,
          ItemPointer,
          GenericComparatorRaw<64>,
          GenericEqualityChecker<64>>(metadata);
    } else if (key_size <= 256) {
      return new SkipListIndex<GenericKey<256>,
          ItemPointer,
          GenericComparatorRaw<256>,
          GenericEqualityChecker<256>>(metadata);
    } else {
      return new SkipListIndex<TupleKey,
          ItemPointer,
          TupleKeyComparatorRaw,
          TupleKeyEqualityChecker>(metadata);
    }
  }

  if (index_type == INDEX_TYPE_BWTREE) {

    if (ints_only && (index_type == INDEX_TYPE_BWTREE)) {
      if (key_size <= sizeof(uint64_t)) {
        return new BWTreeIndex<IntsKey<1>,
            ItemPointer,
            IntsComparator<1>,
            IntsEqualityChecker<1>,
            IntsHasher<1>,
            ItemPointerComparator,
            ItemPointerHashFunc>(metadata);
      } else if (key_size <= sizeof(int64_t) * 2) {
        return new BWTreeIndex<IntsKey<2>,
            ItemPointer,
            IntsComparator<2>,
            IntsEqualityChecker<2>,
            IntsHasher<2>,
            ItemPointerComparator,
            ItemPointerHashFunc>(metadata);
      } else if (key_size <= sizeof(int64_t) * 3) {
        return new BWTreeIndex<IntsKey<3>,
            ItemPointer,
            IntsComparator<3>,
            IntsEqualityChecker<3>,
            IntsHasher<3>,
            ItemPointerComparator,
            ItemPointerHashFunc>(metadata);
      } else if (key_size <= sizeof(int64_t) * 4) {
        return new BWTreeIndex<IntsKey<4>,
            ItemPointer,
            IntsComparator<4>,
            IntsEqualityChecker<4>,
            IntsHasher<4>,
            ItemPointerComparator,
            ItemPointerHashFunc>(metadata);
      } else {
        throw IndexException(
            "We currently only support tree index on non-unique "
            "integer keys of size 32 bytes or smaller...");
      }
    }

    if (key_size <= 4) {
      return new BWTreeIndex<GenericKey<4>,
          ItemPointer,
          GenericComparator<4>,
          GenericEqualityChecker<4>,
          GenericHasher<4>,
          ItemPointerComparator,
          ItemPointerHashFunc>(metadata);

    } else if (key_size <= 8) {
      return new BWTreeIndex<GenericKey<8>,
          ItemPointer,
          GenericComparator<8>,
          GenericEqualityChecker<8>,
          GenericHasher<8>,
          ItemPointerComparator,
          ItemPointerHashFunc>(metadata);
    } else if (key_size <= 16) {
      return new BWTreeIndex<GenericKey<16>,
          ItemPointer,
          GenericComparator<16>,
          GenericEqualityChecker<16>,
          GenericHasher<16>,
          ItemPointerComparator,
          ItemPointerHashFunc>(metadata);
    } else if (key_size <= 64) {
      return new BWTreeIndex<GenericKey<64>,
          ItemPointer,
          GenericComparator<64>,
          GenericEqualityChecker<64>,
          GenericHasher<64>,
          ItemPointerComparator,
          ItemPointerHashFunc>(metadata);
    } else if (key_size <= 256) {
      return new BWTreeIndex<GenericKey<256>,
          ItemPointer,
          GenericComparator<256>,
          GenericEqualityChecker<256>,
          GenericHasher<256>,
          ItemPointerComparator,
          ItemPointerHashFunc>(metadata);
    } else {
      return new BWTreeIndex<TupleKey,
          ItemPointer,
          TupleKeyComparator,
          TupleKeyEqualityChecker,
          TupleKeyHasher,
          ItemPointerComparator,
          ItemPointerHashFunc>(metadata);
    }
  }


  throw IndexException("Unsupported index scheme.");
  return NULL;
}

}  // End index namespace
}  // End peloton namespace
