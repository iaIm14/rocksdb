// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <sys/types.h>

#include <cstdint>
#include <fstream>
#include <memory>

#include "tools/trace_analyzer_tool.h"
#include "trace_replay/memtable_tracer.h"

namespace ROCKSDB_NAMESPACE {

struct MemtableTraceReader;
struct MemtableTraceRecord;
struct MemtableTraceHeader;

enum TraceMemtableType { Insert = 0, Lookup };
struct MemtableTraceUnit {
  uint64_t ts;
  TraceMemtableType type;
  uint64_t memtable_id;
  std::pair<std::string, std::string> kv;
  uint32_t key_size;
  uint32_t value_size;
};
struct sUnit {
  uint64_t insert_count;
  double insert_speed;
  uint64_t lookup_count;
  double lookup_speed;
};
struct MemtableTraceStats {
  double overall_insert_speed{0};
  double overall_lookup_speed{0};
  uint64_t total_insert{0};
  uint64_t total_lookup{0};
  uint64_t begin_time{0};
  uint64_t end_time{0};
  std::map<uint64_t /*memtable_id*/, sUnit> m_table_stats;
  std::map<uint64_t, std::map<std::string /*key*/, uint64_t /*access_count*/>>
      m_entry_stats;
  std::map<uint64_t, std::map<uint64_t /*time*/, uint64_t /*insert_count*/>>
      m_time_insert;
  std::map<uint64_t, std::map<uint64_t /*time*/, uint64_t /*lookup_count*/>>
      m_time_lookup;

  MemtableTraceStats() = default;
  ~MemtableTraceStats() = default;

  MemtableTraceStats(const MemtableTraceStats&) = delete;
  MemtableTraceStats(MemtableTraceStats&&) = delete;
  MemtableTraceStats& operator=(const MemtableTraceStats&) = delete;
  MemtableTraceStats& operator=(MemtableTraceStats&&) = delete;
};

// MemtableTraceRecordParser class reads the Memtable trace file (in binary
// format) and dumps the human readable records in output_file_.
class MemtableTraceRecordParser {
 public:
  explicit MemtableTraceRecordParser(const std::string& input_file);

  // ReadMemtableTraceRecords reads the binary trace file records one by one and
  // invoke PrintHumanReadableMemtableTraceRecord to dump the records in
  // output_file_.
  int ReadMemtableTraceRecords();
  void SetAnalyze(bool if_analyze_);

 private:
  void PrintHumanReadableHeader(const MemtableTraceHeader& header);
  void PrintHumanReadableMemtableTraceRecord(const MemtableTraceRecord& record,
                                             std::fstream& fs);
  Status KeyStatsInsertion(const uint64_t ts, const TraceMemtableType& type,
                           const uint32_t& memtable_id, const std::string& key,
                           const std::string& value, const uint32_t& key_size,
                           const uint32_t& value_sizes);
  void DumpAnalyzeResult(std::fstream& fs);
  // Binary file that contains Memtable trace records.
  std::string input_file_;
  bool if_analyze{false};
  MemtableTraceStats stats;
};

int memtable_tracer_parser(int argc, char** argv);

}  // namespace ROCKSDB_NAMESPACE
