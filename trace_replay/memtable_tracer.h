#pragma once

#include <atomic>
#include <cstdint>
#include <fstream>
#include <memory>

#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "monitoring/instrumented_mutex.h"
#include "port/lang.h"
#include "rocksdb/file_system.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/options.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/types.h"
#include "trace_replay/io_tracer.h"
#include "trace_replay/trace_replay.h"

namespace ROCKSDB_NAMESPACE {
class SystemClock;
class TraceReader;
class TraceWriter;
class Env;
extern const uint64_t kMicrosInSecond;
extern const uint64_t kSecondInMinute;
extern const uint64_t kSecondInHour;

struct MemtableTraceRecord {
  uint64_t access_timestamp = 0;
  TraceType trace_type;
  uint64_t memtable_id_{0};
  SequenceNumber seq_number{0};

  std::pair<Slice, Slice> kv;
  // Add or Update
  ValueType type;
  uint32_t key_size;
  uint32_t val_size;

  MemtableTraceRecord(const uint64_t& _access_timestamp,
                      const TraceType& _trace_type,
                      const uint64_t& _memtable_id,
                      const SequenceNumber& _seq_number, const LookupKey* _key,
                      const std::string& _value)
      : access_timestamp(_access_timestamp),
        trace_type(_trace_type),
        memtable_id_(_memtable_id),
        seq_number(_seq_number),
        kv(std::make_pair<Slice, Slice>(Slice(_key->memtable_key()),
                                        Slice(_value))),
        type(ValueType::kMaxValue),
        key_size(0),
        val_size(0) {}

  MemtableTraceRecord(const uint64_t& _access_timestamp,
                      const TraceType& _trace_type,
                      const uint64_t& _memtable_id,
                      const SequenceNumber& _seq_number, const Slice& _key,
                      const Slice& _value, const uint32_t& _key_size,
                      const uint32_t& _val_size, const ValueType& _value_type)
      : access_timestamp(_access_timestamp),
        trace_type(_trace_type),
        memtable_id_(_memtable_id),
        seq_number(_seq_number),
        kv(std::make_pair(_key, _value)),
        type(_value_type),
        key_size(_key_size),
        val_size(_val_size) {}
};

struct MemtableTraceHeader {
  uint64_t start_time;
  uint32_t rocksdb_major_version;
  uint32_t rocksdb_minor_version;
};

class MemtableTraceWriter {
 public:
  MemtableTraceWriter(SystemClock* clock, const TraceOptions& options,
                      std::unique_ptr<TraceWriter>&& trace_writer);
  ~MemtableTraceWriter() = default;

  MemtableTraceWriter(const MemtableTraceWriter&) = delete;
  MemtableTraceWriter(MemtableTraceWriter&&) = delete;
  MemtableTraceWriter& operator=(const MemtableTraceWriter&) = delete;
  MemtableTraceWriter& operator=(MemtableTraceWriter&&) = delete;

  Status WriteMemtableOp(const MemtableTraceRecord& record);
  Status WriteHeader();

 private:
  SystemClock* clock_;
  TraceOptions trace_options_;
  std::unique_ptr<TraceWriter> writer_;
};

class MemtableTraceReader {
 public:
  explicit MemtableTraceReader(std::unique_ptr<TraceReader>&& reader)
      : trace_reader_(std::move(reader)) {}
  ~MemtableTraceReader() = default;

  MemtableTraceReader(const MemtableTraceReader&) = delete;
  MemtableTraceReader& operator=(const MemtableTraceReader&) = delete;
  MemtableTraceReader(MemtableTraceReader&&) = delete;
  MemtableTraceReader& operator=(MemtableTraceReader&&) = delete;

  Status ReadHeader(MemtableTraceHeader* header);
  Status ReadMemtableOp(MemtableTraceRecord* record);

 private:
  std::unique_ptr<TraceReader> trace_reader_;
};

class MemtableTracer {
 public:
  MemtableTracer();
  ~MemtableTracer();
  MemtableTracer(const MemtableTracer&) = delete;
  MemtableTracer(MemtableTracer&&) = delete;
  MemtableTracer& operator=(const MemtableTracer&) = delete;
  MemtableTracer& operator=(MemtableTracer&&) = delete;

  Status StartMemtableTrace(SystemClock* clock,
                            const TraceOptions& trace_options,
                            std::unique_ptr<TraceWriter>&& trace_writer);
  Status EndMemtableTrace();
  void WriteMemtableOp(const MemtableTraceRecord& record);
  bool is_tracing_enabled() const { return tracing_enabled; }

 private:
  bool tracing_enabled{false};
  TraceOptions options;
  InstrumentedMutex trace_writer_mutex;
  std::atomic<MemtableTraceWriter*> writer_;
};

}  // namespace ROCKSDB_NAMESPACE