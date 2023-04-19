//  Copyright (c) Meta Platforms, Inc. and affiliates.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifdef OS_WIN
#include "port/win/port_win.h"
// ^^^ For proper/safe inclusion of windows.h. Must come first.
#include <memoryapi.h>
#else
#include <sys/mman.h>
#endif  // OS_WIN

#include <cstdint>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

// An RAII wrapper for shared memory
class ShMMapping {
 public:
  static constexpr bool kSharedMemorySupport = true;

  static ShMMapping Allocate(size_t length);

  // No copies
  ShMMapping(const ShMMapping&) = delete;
  ShMMapping& operator=(const ShMMapping&) = delete;
  // Move
  ShMMapping(ShMMapping&&) noexcept;
  ShMMapping& operator=(ShMMapping&&) noexcept;

  // Releases the mapping
  ~ShMMapping();

  [[nodiscard]] inline void* Get() const { return addr_; }
  [[nodiscard]] inline size_t Length() const { return length_; }
  [[nodiscard]] inline int GetID() const { return shm_id_; }

 private:
  ShMMapping() {}

  // The mapped memory, or nullptr on failure / not supported
  void* addr_ = nullptr;
  int shm_id_ = 0;
  // The known usable number of bytes starting at that address
  size_t length_ = 0;

  static ShMMapping AllocateAnonymous(size_t length);
};

}  // namespace ROCKSDB_NAMESPACE
