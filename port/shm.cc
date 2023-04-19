//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "port/shm.h"

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>

#include <cassert>
#include <cstdio>
#include <cstring>
#include <new>
#include <utility>

#include "util/hash.h"
#include "util/logger.hpp"

namespace ROCKSDB_NAMESPACE {

ShMMapping::~ShMMapping() {
  if (addr_ != nullptr) {
    auto status = shmdt(addr_);
    assert(status == 0);
  }
}

ShMMapping::ShMMapping(ShMMapping&& other) noexcept {
  *this = std::move(other);
}

ShMMapping& ShMMapping::operator=(ShMMapping&& other) noexcept {
  if (&other == this) {
    return *this;
  }
  this->~ShMMapping();
  std::memcpy(this, &other, sizeof(*this));
  new (&other) ShMMapping();
  return *this;
}

ShMMapping ShMMapping::AllocateAnonymous(size_t length) {
  ShMMapping mm;
  mm.length_ = length;
  assert(mm.addr_ == nullptr);
  if (length == 0) {
    return mm;
  }
  mm.shm_id_ = shmget(rand(), length, 0666 | IPC_CREAT);
  assert(mm.shm_id_ != -1);
  mm.addr_ = shmat(mm.GetID(), nullptr, 0);
  LOG("ShMMapping::AllocateAnonymous allocate: ptr =", mm.addr_,
      " id =", mm.GetID());
  return mm;
}

ShMMapping ShMMapping::Allocate(size_t length) {
  return AllocateAnonymous(length);
}

}  // namespace ROCKSDB_NAMESPACE
