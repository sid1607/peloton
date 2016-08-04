//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// gc_test.cpp
//
// Identification: test/gc/gc_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"
#include "concurrency/transaction_tests_util.h"
#include "gc/gc_manager.h"
#include "gc/gc_manager_factory.h"
#include "concurrency/epoch_manager.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// Transaction Tests
//===--------------------------------------------------------------------===//

class GCTest : public PelotonTest {};

}  // End test namespace
}  // End peloton namespace
