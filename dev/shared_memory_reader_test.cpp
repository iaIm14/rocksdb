#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/types.h>

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
#include "rocksdb/slice_transform.h"
#include "rocksdb/status.h"
#include "rocksdb/utilities/options_type.h"
#include "rocksdb/write_batch_base.h"
#include "util/logger.hpp"
#include "utilities/merge_operators.h"

#define ROOT_DIR "/data/rocksdb/"
using namespace std;
using namespace rocksdb;
struct ipc_data {
  int written_count{0};
  const static size_t MAXNUM = 1000;
  std::array<char, MAXNUM> payload;
};

auto main() -> signed {
  //   rocksdb::Env* env = rocksdb::Env::Default();
  //   EnvOptions env_options;
  //   std::string block_cache_trace_path = std::string(ROOT_DIR) + "dev/trace";
  //   DB* db = nullptr;
  //   std::string db_name = std::string(ROOT_DIR) + "dev/db/";
  //   Options opt;
  //   opt.prefix_extractor.reset(NewFixedPrefixTransform(3));
  //   opt.create_if_missing = true;
  //   opt.merge_operator = MergeOperators::CreateStringAppendOperator();
  //   DB::Open(opt, db_name, &db);
  //   std::unique_ptr<TraceWriter> trace_writer;
  //   auto ret = NewFileTraceWriter(env, env_options, block_cache_trace_path,
  //                                 &trace_writer);
  //   assert(ret.code() == Status::kOk);
  //   assert(trace_writer != nullptr);
  //   TraceOptions trace_opt;
  //   db->StartBlockCacheTrace(trace_opt, std::move(trace_writer));

  //   db->EndBlockCacheTrace();
  //   db->Close();

  int shmid = shmget(ftok("/data/rocksdb/dev/shared1", 0), sizeof(ipc_data),
                     0666 | IPC_CREAT);
  LOG("shmget id=", shmid);
  void* shm_ptr = shmat(shmid, nullptr, 0);
  LOG("Reader vir_ptr =", shm_ptr);
  while (true) {
    auto* data_ = static_cast<ipc_data*>(shm_ptr);
    if (data_->written_count == 0) {
      std::this_thread::sleep_for(std::chrono::seconds(2));
      continue;
    }
    LOG("reader detect data: ", data_->payload.data());
    break;
  }
  shmdt(shm_ptr);
  return 0;
}