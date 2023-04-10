#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <ios>
#include <iostream>
#include <thread>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"
#include "util/logger.hpp"
#include "util/macro.hpp"
#include "utilities/merge_operators.h"
#define ROOT_DIR "/data/rocksdb/"
using namespace std;
using namespace rocksdb;

signed main() {
  rocksdb::Env* env = rocksdb::Env::Default();
  EnvOptions env_options;
  std::string memtable_trace_path =
      std::string(ROOT_DIR) + "dev/trace_memtable";
  DB* db = nullptr;
  std::string db_name = std::string(ROOT_DIR) + "dev/db/";
  Options opt;
  opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  opt.create_if_missing = true;
  opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  DB::Open(opt, db_name, &db);
  // memtable_tracer
  std::unique_ptr<TraceWriter> trace_writer;
  auto ret =
      NewFileTraceWriter(env, env_options, memtable_trace_path, &trace_writer);
  assert(ret.code() == Status::kOk);
  assert(trace_writer != nullptr);
  TraceOptions trace_opt;
  db->StartMemtableTrace(trace_opt, std::move(trace_writer));

  // operations
  WriteOptions wo;
  //   for (int i = 1;; i++) {
  LOG("Start Test Wb");
  WriteBatch wb;
  std::chrono::steady_clock::time_point tp_begin =
      std::chrono::steady_clock::now();
  int randnum = 0;
  for (int i = 1;; i++) {
    randnum = rand() % 10;
    if (randnum < 3) {
      std::pair<std::string, std::string> kvp;
      kvp.first = std::to_string(i);

      for (int count = 0; count <= 200; count++) {
        int srand = rand() % 26;
        kvp.second.push_back('a' + srand);
      }
      wb.Put(kvp.first, kvp.second);
      // LOG("wb-put: ", kvp.first, " ", kvp.second);
      db->Write(wo, &wb);
      wb.Clear();
    } else {
      std::string search_key = std::to_string(rand() % i);
      ReadOptions ro_;
      std::string ret_value;
      db->Get(ro_, search_key, &ret_value);
      // LOG("op-lookup: ", search_key, " ", ret_value);
      i--;
    }
    auto tp_duration = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::steady_clock::now() - tp_begin);

    if (tp_duration >= std::chrono::milliseconds(2000)) {
      LOG("End Put Operation, total Put op count = ", i);
      break;
    }
  }
  db->EndMemtableTrace();
  db->Close();
  return 0;
}