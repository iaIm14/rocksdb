#pragma once
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>

#include "macro.hpp"

#define LOG_LEVEL_INFO 1
#define LOG_LEVEL_BRIEF 0

#define __LOG_LEVEL__ LOG_LEVEL_INFO

#if __LOG_LEVEL__ == 1
#define LOG_PRINT_HEADER(stream, line, filename, function) \
  stream << "[" << (filename) << ':' << (line) << ':' << (function) << "] ";
#else
#define LOG_PRINT_HEADER(stream, line, filename, function)
#endif

namespace LocalLogger {

template <typename OUT, typename T>
void LOG_INFO(OUT &&stream, T arg) {
  stream << arg << ' ' << std::endl;
}
template <typename OUT, typename U, typename... T>
void LOG_INFO(OUT &&stream, U arg, T... Args) {
  stream << arg << ' ';
  LOG_INFO(std::forward<OUT>(stream), std::forward<T>(Args)...);
}

class LocalLogger {
  std::mutex mtx;
  std::fstream _stream_;

 public:
  LocalLogger()
      : _stream_("/data/rocksdb/dev/Log.log", std::ios::out | std::ios::app) {}
  LocalLogger(std::string &logfile) : _stream_(logfile.c_str()) {}
  template <typename... Args>
  void output(const char *filename, const int &line, const char *function_name,
              Args &&...args);
};

template <typename... Args>
void LocalLogger::output(const char *filename, const int &line,
                         const char *function_name, Args &&...args) {
  std::stringstream stream;
  auto time =
      std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
  stream << "[" << std::localtime(&time)->tm_hour << ":"
         << std::localtime(&time)->tm_min << ":"
         << std::localtime(&time)->tm_sec << "] ";
  LOG_PRINT_HEADER(stream, line, filename, function_name);
  LOG_INFO(stream, std::forward<Args>(args)...);
  char buffer[10001]{};
  stream.read(buffer, 10000);
  {
    std::lock_guard<std::mutex> lock(mtx);
    _stream_ << buffer;
    _stream_.flush();
  }
}
}  // namespace LocalLogger