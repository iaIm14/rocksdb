#include "memtable_tracer.h"

#include <cassert>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <memory>

#include "db/db_impl/db_impl.h"
#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "monitoring/instrumented_mutex.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/system_clock.h"
#include "rocksdb/trace_reader_writer.h"
#include "rocksdb/trace_record.h"
#include "rocksdb/types.h"
#include "trace_replay/trace_replay.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/string_util.h"

namespace ROCKSDB_NAMESPACE {
MemtableTraceWriter::MemtableTraceWriter(
    SystemClock* clock, const TraceOptions& options,
    std::unique_ptr<TraceWriter>&& trace_writer)
    : clock_(clock),
      trace_options_(options),
      writer_(std::move(trace_writer)) {}

Status MemtableTraceWriter::WriteMemtableOp(const MemtableTraceRecord& record) {
  uint64_t trace_file_size = writer_->GetFileSize();
  if (trace_file_size > trace_options_.max_trace_file_size) {
    return Status::Aborted("trace file size too large");
  }
  Trace trace;
  trace.ts = record.access_timestamp;
  trace.type = record.trace_type;
  PutFixed64(&trace.payload, record.seq_number);
  PutFixed64(&trace.payload, record.memtable_id_);
  switch (trace.type) {
    case TraceType::kMemtableInsertV0:
      PutFixed32(&trace.payload, record.key_size);
      PutFixed32(&trace.payload, record.val_size);
      PutLengthPrefixedSlice(&trace.payload, record.kv.first);
      PutLengthPrefixedSlice(&trace.payload, record.kv.second);
      break;
    case TraceType::kMemtableLootupV0: {
      PutLengthPrefixedSlice(&trace.payload, record.kv.first);
      PutLengthPrefixedSlice(&trace.payload, record.kv.second);
      break;
    }
    default:
      return Status::Aborted("trace record Type unrecognized");
  }
  std::string encoded_trace;
  TracerHelper::EncodeTrace(trace, &encoded_trace);

  return writer_->Write(encoded_trace);
}

Status MemtableTraceWriter::WriteHeader() {
  Trace trace;
  trace.ts = clock_->NowMicros();
  trace.type = TraceType::kTraceBegin;
  PutLengthPrefixedSlice(&trace.payload, kTraceMagic);
  PutFixed32(&trace.payload, kMajorVersion);
  PutFixed32(&trace.payload, kMinorVersion);
  std::string encoded_trace;
  TracerHelper::EncodeTrace(trace, &encoded_trace);
  return writer_->Write(encoded_trace);
}

MemtableTraceReader::MemtableTraceReader(std::unique_ptr<TraceReader>&& reader)
    : trace_reader_(std::move(reader)) {}
Status MemtableTraceReader::ReadHeader(MemtableTraceHeader* header) {
  assert(header != nullptr);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }
  Trace trace;
  s = TracerHelper::DecodeTrace(encoded_trace, &trace);
  if (!s.ok()) {
    return s;
  }
  header->start_time = trace.ts;
  auto enc_slice = Slice(trace.payload);
  Slice magic_number;
  if (!GetLengthPrefixedSlice(&enc_slice, &magic_number)) {
    return Status::Corruption(
        "Corrupted header in the trace file: Failed to read the magic number.");
  }
  if (magic_number.ToString() != kTraceMagic) {
    return Status::Corruption(
        "Corrupted header in the trace file: Magic number does not match.");
  }
  if (!GetFixed32(&enc_slice, &header->rocksdb_major_version)) {
    return Status::Corruption(
        "Corrupted header in the trace file: Failed to read rocksdb major "
        "version number.");
  }
  if (!GetFixed32(&enc_slice, &header->rocksdb_minor_version)) {
    return Status::Corruption(
        "Corrupted header in the trace file: Failed to read rocksdb minor "
        "version number.");
  }
  // We should have retrieved all information in the header.
  if (!enc_slice.empty()) {
    return Status::Corruption(
        "Corrupted header in the trace file: The length of header is too "
        "long.");
  }
  return Status::OK();
}
Status MemtableTraceReader::ReadMemtableOp(MemtableTraceRecord* record) {
  assert(record != nullptr);
  std::string encoded_trace;
  Status s = trace_reader_->Read(&encoded_trace);
  if (!s.ok()) {
    return s;
  }
  Trace trace;
  s = TracerHelper::DecodeTrace(encoded_trace, &trace);
  if (!s.ok()) {
    return s;
  }
  record->access_timestamp = trace.ts;
  record->trace_type = trace.type;
  auto enc_trace = Slice(trace.payload);
  if (!GetFixed64(&enc_trace, &record->seq_number)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read operation seq number");
  }
  if (!GetFixed64(&enc_trace, &record->memtable_id_)) {
    return Status::Incomplete(
        "Incomplete access record: Failed to read memtable id");
  }
  switch (record->trace_type) {
    case TraceType::kMemtableInsertV0: {
      if (!GetFixed32(&enc_trace, &record->key_size)) {
        return Status::Incomplete(
            "Incomplete access record: Failed to read key size");
      }
      if (!GetFixed32(&enc_trace, &record->val_size)) {
        return Status::Incomplete(
            "Incomplete access record: Failed to read value size");
      }
      std::pair<Slice, Slice> kvpair;
      if (!GetLengthPrefixedSlice(&enc_trace, &kvpair.first)) {
        return Status::Incomplete(
            "Incomplete access record: Failed to read insert key");
      }
      if (!GetLengthPrefixedSlice(&enc_trace, &kvpair.second)) {
        return Status::Incomplete(
            "Incomplete access record: Failed to read insert value");
      }
      record->kv =
          std::make_pair(std::string(kvpair.first.data(), kvpair.first.size()),
                         kvpair.second.data());
    } break;
    case TraceType::kMemtableLootupV0: {
      std::pair<Slice, Slice> kvpair;
      if (!GetLengthPrefixedSlice(&enc_trace, &kvpair.first)) {
        return Status::Incomplete(
            "Incomplete access record: Failed to read insert key");
      }
      if (!GetLengthPrefixedSlice(&enc_trace, &kvpair.second)) {
        return Status::Incomplete(
            "Incomplete access record: Failed to read insert value");
      }
      record->kv =
          std::make_pair(std::string(kvpair.first.data(), kvpair.first.size()),
                         kvpair.second.data());
    } break;
    default:
      return Status::Aborted("trace record Type unrecognized");
  }
  return Status::OK();
}

MemtableTracer::MemtableTracer() { writer_.store(nullptr); }
MemtableTracer::~MemtableTracer() { EndMemtableTrace(); }
Status MemtableTracer::StartMemtableTrace(
    SystemClock* clock, const TraceOptions& trace_options,
    std::unique_ptr<TraceWriter>&& trace_writer) {
  InstrumentedMutexLock lock(&trace_writer_mutex);
  if (writer_.load()) {
    return Status::Busy();
  }
  options = trace_options;
  writer_.store(
      new MemtableTraceWriter(clock, trace_options, std::move(trace_writer)));
  tracing_enabled = true;
  return writer_.load()->WriteHeader();
}

Status MemtableTracer::EndMemtableTrace() {
  InstrumentedMutexLock lock(&trace_writer_mutex);
  if (!writer_.load()) {
    return Status::OK();
  }
  delete writer_.load();
  writer_.store(nullptr);
  tracing_enabled = false;
  return Status::OK();
}

void MemtableTracer::WriteMemtableOp(const MemtableTraceRecord& record) {
  if (!writer_.load()) {
    return;
  }
  InstrumentedMutexLock lock(&trace_writer_mutex);
  // TODO: use static
  if (!writer_.load()) {
    return;
  }
  writer_.load()->WriteMemtableOp(record).PermitUncheckedError();
}

}  // namespace ROCKSDB_NAMESPACE