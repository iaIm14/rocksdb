// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>

#include "rocksdb/env.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

struct IOTraceHeader;
struct IOTraceRecord;
enum TraceIOType { Write = 0, Read };
struct IOTraceUnit {
  uint64_t ts;
  TraceIOType type;
  uint32_t len;
};

struct IOTraceStats {
  double overall_write_speed{0};
  double overall_read_speed{0};
  uint64_t total_read_len{0};
  uint64_t total_write_len{0};
  uint64_t begin_time{0};
  uint64_t end_time{0};

  IOTraceStats() = default;
  ~IOTraceStats() = default;

  IOTraceStats(const IOTraceStats&) = delete;
  IOTraceStats(IOTraceStats&&) = delete;
  IOTraceStats& operator=(const IOTraceStats&) = delete;
  IOTraceStats& operator=(IOTraceStats&&) = delete;
};

// IOTraceRecordParser class reads the IO trace file (in binary format) and
// dumps the human readable records in output_file_.
class IOTraceRecordParser {
 public:
  explicit IOTraceRecordParser(const std::string& input_file);

  // ReadIOTraceRecords reads the binary trace file records one by one and
  // invoke PrintHumanReadableIOTraceRecord to dump the records in output_file_.
  int ReadIOTraceRecords();
  void SetAnalyze(bool if_analyze_);

 private:
  void PrintHumanReadableHeader(const IOTraceHeader& header);
  void PrintHumanReadableIOTraceRecord(const IOTraceRecord& record);
  Status KeyStatsInsertion(const uint64_t ts, const TraceIOType& type,
                           const uint64_t& len);
  // Binary file that contains IO trace records.
  std::string input_file_;
  bool if_analyze{false};
  IOTraceStats stats;
};

int io_tracer_parser(int argc, char** argv);

}  // namespace ROCKSDB_NAMESPACE
