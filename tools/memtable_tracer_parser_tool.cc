
#include <gflags/gflags.h>

#include <cassert>
#include <cstdint>
#include <string>
#include <utility>

#include "rocksdb/trace_record.h"
#include "util/logger.hpp"
#include "util/macro.hpp"
#ifdef GFLAGS
#include <cinttypes>
#include <cstdio>
#include <iomanip>
#include <memory>
#include <sstream>

#include "port/lang.h"
#include "rocksdb/trace_reader_writer.h"
#include "tools/memtable_tracer_parser_tool.h"
#include "trace_replay/memtable_tracer.h"
#include "util/gflags_compat.h"

using GFLAGS_NAMESPACE::ParseCommandLineFlags;

DEFINE_string(memtable_trace_file, "", "The memtable trace file path.");
DEFINE_string(output_file, "", "parser output data file");
DEFINE_string(if_statistic, "", "set true if want to analyze");
DEFINE_string(dump_analyze_result_dir, "", "set directory of analyze result");

namespace ROCKSDB_NAMESPACE {
MemtableTraceRecordParser::MemtableTraceRecordParser(
    const std::string& input_file)
    : input_file_(input_file) {}

void MemtableTraceRecordParser::PrintHumanReadableHeader(
    const MemtableTraceHeader& header) {
  std::stringstream ss;
  ss << "Start Time: " << header.start_time
     << "\nRocksDB Major Version: " << header.rocksdb_major_version
     << "\nRocksDB Minor Version: " << header.rocksdb_minor_version << "\n";
  fprintf(stdout, "%s", ss.str().c_str());
  if (if_analyze) {
    stats.begin_time = header.start_time;
    stats.end_time = header.start_time;
  }
}

void MemtableTraceRecordParser::PrintHumanReadableMemtableTraceRecord(
    const MemtableTraceRecord& record, std::fstream& fs) {
  std::stringstream ss;
  ss << "Operation Time : " << std::setw(20) << std::left
     << record.access_timestamp << ", Memtable id : " << record.memtable_id_
     << ' ' << "Operation Type : "
     << (record.trace_type == TraceType::kMemtableInsertV0 ? "insert"
                                                           : "lookup")
     << ' ' << "Operation Sequence Number : " << record.seq_number << ' ';
  switch (record.trace_type) {
    case TraceType::kMemtableInsertV0: {
      ss << "Key size : " << record.key_size << " Key : " << record.kv.first
         << " Value size : " << record.val_size
         << " Value :" << record.kv.second;
      //  TODO:  ValueType
    } break;
    case TraceType::kMemtableLootupV0: {
      ss << "Key : " << record.kv.first << ' '
         << "Value : " << record.kv.second;
    } break;
    default:
      assert(false);
  }
  fs << ss.str().c_str() << std::endl;
}
void MemtableTraceRecordParser::SetAnalyze(bool if_analyze_) {
  this->if_analyze = if_analyze_;
}
int MemtableTraceRecordParser::ReadMemtableTraceRecords() {
  std::fstream fs;
  fs.open(FLAGS_output_file, std::ios::out);
  Status status;
  Env* env(Env::Default());
  std::unique_ptr<TraceReader> trace_reader;
  std::unique_ptr<MemtableTraceReader> memtable_trace_reader;

  status = NewFileTraceReader(env, EnvOptions(), input_file_, &trace_reader);
  if (!status.ok()) {
    fprintf(stderr, "%s: %s\n", input_file_.c_str(), status.ToString().c_str());
    return 1;
  }
  memtable_trace_reader.reset(new MemtableTraceReader(std::move(trace_reader)));

  // Read the header and dump it in a file.
  MemtableTraceHeader header;
  status = memtable_trace_reader->ReadHeader(&header);
  if (!status.ok()) {
    fprintf(stderr, "%s: %s\n", input_file_.c_str(), status.ToString().c_str());
    return 1;
  }
  PrintHumanReadableHeader(header);

  // Read the records one by one and print them in human readable format.
  while (status.ok()) {
    MemtableTraceRecord record;
    record.kv = std::make_pair("", "");
    status = memtable_trace_reader->ReadMemtableOp(&record);
    if (!status.ok()) {
      break;
    }
    PrintHumanReadableMemtableTraceRecord(record, fs);
    if (if_analyze) {
      KeyStatsInsertion(
          record.access_timestamp,
          (record.trace_type == TraceType::kMemtableInsertV0)
              ? TraceMemtableType::Insert
              : TraceMemtableType::Lookup,
          record.memtable_id_, record.kv.first, record.kv.second.data(),
          (record.trace_type == TraceType::kMemtableInsertV0) ? record.key_size
                                                              : 0,
          (record.trace_type == TraceType::kMemtableInsertV0) ? record.val_size
                                                              : 0);
    }
  }

  if (if_analyze) {
    if (FLAGS_dump_analyze_result_dir.empty()) {
      fprintf(stderr, "havn't set analyze result dump dir");
      return 1;
    }
    std::fstream fs1(FLAGS_dump_analyze_result_dir + "/overall.txt",
                     std::ios::out);
    DumpAnalyzeResult(fs1);
  }
  return 0;
}
void MemtableTraceRecordParser::DumpAnalyzeResult(std::fstream& fs) {
  uint64_t duration = (stats.end_time - stats.begin_time) / 1000000;
  LOG("overall duration = ", duration);
  // TODO: need check
  stats.overall_insert_speed = stats.total_insert / duration;
  stats.overall_lookup_speed = stats.total_lookup / duration;
  uint64_t all_op = stats.total_insert + stats.total_lookup;
  fs << "Overall operations: " << all_op << ' '
     << ", Insert: " << stats.total_insert << ' '
     << " Lookup: " << stats.total_lookup << std::endl;
  fs << "Insert Speed: " << stats.overall_insert_speed << ' '
     << "Lookup Speed: " << stats.overall_lookup_speed << std::endl;
  fs.flush();
  fs.close();
  for (auto& mp : stats.m_entry_stats) {
    uint64_t id = mp.first;
    fs.open(FLAGS_dump_analyze_result_dir + "/" + std::to_string(id) +
                "_entry_stats.txt",
            std::ios::out);
    fs << "Memtable id: " << id << std::endl;
    // TODO: sort it
    // std::vector<std::pair<std::string, uint64_t>> sort_;
    for (auto& val : mp.second) {
      fs << "Key : " << val.first << " "
         << "access Time: " << val.second << std::endl;
    }
    fs.flush();
    fs.close();
  }
  fs.open(FLAGS_dump_analyze_result_dir + "/ret_table_stats.txt",
          std::ios::out);
  for (auto& mp : stats.m_table_stats) {
    uint64_t id = mp.first;
    auto& m_stats = mp.second;
    m_stats.insert_speed = (double)m_stats.insert_count / duration;
    m_stats.lookup_speed = (double)m_stats.lookup_count / duration;
    fs << "memtable id: " << id << ' '
       << "total Insert: " << m_stats.insert_count << ' '
       << "speed: " << m_stats.insert_speed
       << " total Lookup: " << m_stats.lookup_count
       << " speed: " << m_stats.lookup_speed << std::endl;
    if (stats.m_time_insert.find(id) == stats.m_time_insert.end()) {
      fs << "Insert peak-qps: 0 ";
    } else {
      auto& qps_mp = stats.m_time_insert.at(id);
      uint64_t peak = 0;
      for (auto& ret : qps_mp) {
        peak = std::max(peak, ret.second);
      }
      fs << "Insert peak-qps: " << peak << " ";
    }
    if (stats.m_time_lookup.find(id) == stats.m_time_lookup.end()) {
      fs << "Lookup peak-qps: 0 ";
    } else {
      auto& qps_mp = stats.m_time_lookup.at(id);
      uint64_t peak = 0;
      for (auto& ret : qps_mp) {
        peak = std::max(peak, ret.second);
      }
      fs << "Lookup peak-qps: " << peak << " ";
    }
  }
  fs.flush();
  fs.close();
}
Status MemtableTraceRecordParser::KeyStatsInsertion(
    const uint64_t ts, const TraceMemtableType& type,
    const uint32_t& memtable_id, const std::string& key,
    const std::string& value, const uint32_t& key_size,
    const uint32_t& value_sizes) {
  Status s = Status::OK();
  switch (type) {
    case TraceMemtableType::Insert:
      stats.total_insert++;
      break;
    case TraceMemtableType::Lookup:
      stats.total_lookup++;
      break;
  }
  stats.end_time = std::max(stats.end_time, ts);
  uint64_t now_time = (ts - stats.begin_time) / 1000000;

  uint64_t id = memtable_id;
  if (stats.m_entry_stats.find(id) == stats.m_entry_stats.end()) {
    stats.m_entry_stats.insert(
        std::make_pair(id, std::map<std::string, uint64_t>{}));
    stats.m_table_stats.insert(std::make_pair(id, sUnit{0, 0, 0, 0}));
    stats.m_time_insert.insert(
        std::make_pair(id, std::map<uint64_t, uint64_t>{}));
    stats.m_time_lookup.insert(
        std::make_pair(id, std::map<uint64_t, uint64_t>{}));
  }
  {
    auto& mp = stats.m_table_stats.at(id);
    if (type == TraceMemtableType::Insert) {
      mp.insert_count++;
    } else {
      mp.lookup_count++;
    }
  }
  {
    auto& mp = stats.m_entry_stats.at(id);
    if (mp.find(key) == mp.end()) {
      mp.insert(std::make_pair(key, 1));
    } else {
      mp.at(key)++;
    }
  }
  {
    auto& mp = (type == TraceMemtableType::Insert) ? stats.m_time_insert.at(id)
                                                   : stats.m_time_lookup.at(id);
    if (mp.find(now_time) == mp.end()) {
      mp.insert(std::make_pair(now_time, 1));
    } else {
      mp.at(now_time)++;
    }
  }

  return s;
}

int memtable_tracer_parser(int argc, char** argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_memtable_trace_file.empty()) {
    fprintf(stderr, "Memtable Trace file path is empty\n");
    return 1;
  }

  MemtableTraceRecordParser memtable_tracer_parser(FLAGS_memtable_trace_file);
  if (FLAGS_if_statistic.empty()) {
    fprintf(stderr, "analyzer set closed as default");
  } else {
    memtable_tracer_parser.SetAnalyze(true);
  }
  return memtable_tracer_parser.ReadMemtableTraceRecords();
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // GFLAGS
