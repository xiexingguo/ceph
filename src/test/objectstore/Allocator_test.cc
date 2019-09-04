// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * In memory space allocator test cases.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */
#include <iostream>
#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "include/Context.h"
#include "os/bluestore/Allocator.h"
#include "os/bluestore/BitAllocator.h"


#if GTEST_HAS_PARAM_TEST

class AllocTest : public ::testing::TestWithParam<const char*> {
public:
    boost::scoped_ptr<Allocator> alloc;
    AllocTest(): alloc(0) { }
    void init_alloc(int64_t size, uint64_t min_alloc_size) {
      std::cout << "Creating alloc type " << string(GetParam()) << " \n";
      alloc.reset(Allocator::create(g_ceph_context, string(GetParam()), size,
				    min_alloc_size));
    }

    void init_close() {
      alloc.reset(0);
    }
};


TEST_P(AllocTest, test_a1)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 1000;
  uint64_t want_size = 2ULL << 30;
  int64_t alloc_unit = 1 << 20;

  init_alloc(blocks*block_size, block_size);
  uint64_t offset = 0x354c088000;
  uint64_t length = (2ULL << 30) - (512ULL << 10);
  alloc->init_add_free(offset, length);
  offset = 0x4404000;
  length = 1 << 20;
  alloc->init_add_free(offset, length);

  AllocExtentVector extents;
  alloc->reserve(want_size);
  auto r = alloc->allocate(want_size, alloc_unit, 0, &extents);
  std::cout << "result r = " << r << std::endl;
  std::cout << "result extents = " << extents << std::endl;
}

TEST_P(AllocTest, test_a1p)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 1000;
  uint64_t want_size = 2ULL << 30;
  int64_t alloc_unit = 1 << 20;

  init_alloc(blocks*block_size, block_size);
  uint64_t offset = 0x354c088345;
  uint64_t length = (2ULL << 30) + (512ULL << 10);
  alloc->init_add_free(offset, length);

  AllocExtentVector extents;
  alloc->reserve(want_size);
  auto r = alloc->allocate(want_size, alloc_unit, 0, &extents);
  std::cout << "result r = " << r << std::endl;
  std::cout << "result extents = " << extents << std::endl;
}

TEST_P(AllocTest, test_a1pp)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 1000;
  uint64_t want_size = 2ULL << 30;
  int64_t alloc_unit = 1 << 20;

  init_alloc(blocks*block_size, block_size);
  uint64_t offset = 0x354c000000;
  uint64_t length = (2ULL << 30) + (2 << 20);
  alloc->init_add_free(offset, length);

  AllocExtentVector extents;
  alloc->reserve(want_size);
  auto r = alloc->allocate(want_size, alloc_unit, 0, &extents);
  std::cout << "result r = " << r << std::endl;
  std::cout << "result extents = " << extents << std::endl;
}

TEST_P(AllocTest, test_a2)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 1000;
  uint64_t want_size = 2ULL << 30;
  int64_t alloc_unit = 1 << 20;

  init_alloc(blocks*block_size, block_size);
  uint64_t offset = 0x354c088000;
  uint64_t length = (2ULL << 30) - (1ULL << 20) - (512ULL << 10);
  alloc->init_add_free(offset, length);
  offset = 0x4404000;
  length = 1 << 20;
  alloc->init_add_free(offset, length);

  offset = 0x0;
  length = 2 << 20;
  alloc->init_add_free(offset, length);

  AllocExtentVector extents;
  alloc->reserve(want_size);
  auto r = alloc->allocate(want_size, alloc_unit, 0, &extents);
  std::cout << "result r = " << r << std::endl;
  std::cout << "result extents = " << extents << std::endl;
}

TEST_P(AllocTest, test_a3)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 1000;
  uint64_t want_size = 2ULL << 30;
  int64_t alloc_unit = 1 << 20;

  init_alloc(blocks*block_size, block_size);
  uint64_t offset = 0x354c088000;
  uint64_t length = (2ULL << 30) - (5ULL << 20) - (512ULL << 10);
  alloc->init_add_free(offset, length);
  offset = 0x4404000;
  length = 1 << 20;
  alloc->init_add_free(offset, length);
  alloc->init_add_free(0x1, 2 << 20);
  alloc->init_add_free(0x256398700, 3 << 20);
  alloc->init_add_free(0x698554540, 1 << 20);
  alloc->init_add_free(0x798554000, 5 << 20);

  AllocExtentVector extents;
  alloc->reserve(want_size);
  auto r = alloc->allocate(want_size, alloc_unit, 0, &extents);
  std::cout << "result r = " << r << std::endl;
  std::cout << "result extents = " << extents << std::endl;
}

TEST_P(AllocTest, test_a4)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 1000;
  uint64_t want_size = 2ULL << 30;
  int64_t alloc_unit = 1 << 20;

  init_alloc(blocks*block_size, block_size);
  uint64_t offset = 0x354c088000;
  uint64_t length = (2ULL << 30) - (5ULL << 20) - (512ULL << 10) - 1;
  alloc->init_add_free(offset, length);
  alloc->init_add_free(0x1, 1 << 20);
  alloc->init_add_free(0x256398700, 1 << 20);
  alloc->init_add_free(0x698554540, 1 << 20);
  alloc->init_add_free(0x798554000, 1 << 20);
  alloc->init_add_free(0x1798554001, (1 << 20) + 1);
  alloc->init_add_free(0x2798554001, (1 << 20) + 4096); 

  AllocExtentVector extents;
  alloc->reserve(want_size);
  auto r = alloc->allocate(want_size, alloc_unit, 0, &extents);
  std::cout << "result r = " << r << std::endl;
  std::cout << "result extents = " << extents << std::endl;
}


TEST_P(AllocTest, test_a5)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 1000;
  uint64_t want_size = 2ULL << 30;
  int64_t alloc_unit = 1 << 20;

  init_alloc(blocks*block_size, block_size);
  uint64_t offset = 0x354c088000;
  uint64_t length = (2ULL << 30) - (5ULL << 20) - (512ULL << 10) - 1;
  alloc->init_add_free(offset, length);
  alloc->init_add_free(0x1, 1 << 20);
  alloc->init_add_free(0x500000, 100);
  alloc->init_add_free(0x600000, 5000);
  alloc->init_add_free(0x700000, 32156478);
  alloc->init_add_free(0x256398700, 1 << 20);
  alloc->init_add_free(0x698554540, 1 << 20);
  alloc->init_add_free(0x798554000, 1 << 20);
  alloc->init_add_free(0x1798554001, (1 << 20) + 1);
  alloc->init_add_free(0x2798554001, (1 << 20) + 4096);

  AllocExtentVector extents;
  alloc->reserve(want_size);
  auto r = alloc->allocate(want_size, alloc_unit, 0, &extents);
  std::cout << "result r = " << r << std::endl;
  std::cout << "result extents = " << extents << std::endl;
}

TEST_P(AllocTest, test_a6)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 1000;
  uint64_t want_size = 8192;
  int64_t alloc_unit = 4096;

  init_alloc(blocks*block_size, block_size);
  uint64_t offset = 0x354c088000;
  uint64_t length = (2ULL << 30) - (5ULL << 20) - (512ULL << 10) - 1;
  alloc->init_add_free(offset, length);
  alloc->init_add_free(0x1, 300);
  alloc->init_add_free(0x500000, 100);
  alloc->init_add_free(0x600000, 5000);
  alloc->init_add_free(0x700000, 32156478);
  alloc->init_add_free(0x256398700, 1 << 20);
  alloc->init_add_free(0x698554540, 1 << 20);
  alloc->init_add_free(0x798554000, 1 << 20);
  alloc->init_add_free(0x1798554001, (1 << 20) + 1);
  alloc->init_add_free(0x2798554001, (1 << 20) + 4096);

  AllocExtentVector extents;
  alloc->reserve(want_size);
  auto r = alloc->allocate(want_size, alloc_unit, 0, &extents);
  std::cout << "result r = " << r << std::endl;
  std::cout << "result extents = " << extents << std::endl;
}

TEST_P(AllocTest, test_b)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 1000;
  int64_t want_size = 2 << 30;
  int64_t alloc_unit = 1 << 20;

  init_alloc(blocks*block_size, block_size);
  uint64_t offset = 0x3478a74000;
  uint64_t length = 0x3670000;
  alloc->init_add_free(offset, length);

  AllocExtentVector extents;
  alloc->reserve(length);
  auto r = alloc->allocate(want_size, alloc_unit, 0, &extents);
  std::cout << "result r = " << r << std::endl;
  std::cout << "result extents = " << extents << std::endl;
}

TEST_P(AllocTest, test_c)
{
  int64_t block_size = 1 << 12;
  int64_t blocks = (1 << 20) * 1000;
  int64_t want_size = 2 << 30;
  int64_t alloc_unit = 1 << 20;

  init_alloc(blocks*block_size, block_size);
  uint64_t offset = 0x354c088000;
  uint64_t length = 0x31ac000;
  alloc->init_add_free(offset, length);

  AllocExtentVector extents;
  alloc->reserve(length);
  auto r = alloc->allocate(want_size, alloc_unit, 0, &extents);
  std::cout << "result r = " << r << std::endl;
  std::cout << "result extents = " << extents << std::endl;
}

INSTANTIATE_TEST_CASE_P(
  Allocator,
  AllocTest,
  ::testing::Values("avl"));

#else

TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}
#endif
