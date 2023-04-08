#!/usr/bin/zsh
PROJECT_ROOT="/data/rocksdb"
cd $PROJECT_ROOT/dev
rm -rf trace
rm -rf ./db/*
cd $PROJECT_ROOT/build
cmake ..
make all -j 18
./trace_block_cache_test
./io_tracer_parser -io_trace_file $PROJECT_ROOT/dev/trace
# ./trace_analyzer \
#   -analyze_get \
#   -analyze_put \
#   -analyze_merge \
#   -analyze_iterator \
#   -output_access_count_stats \
#   -output_dir=/data/rocksdb/dev/result/ \
#   -output_key_stats \
#   -output_qps_stats \
#   -convert_to_human_readable_trace \
#   -output_value_distribution \
#   -output_key_distribution \
#   -print_overall_stats \
#   -print_top_k_access=3 \
#   -output_prefix=test \
#   -trace_path=/data/rocksdb/dev/trace
cd $PROJECT_ROOT/dev


