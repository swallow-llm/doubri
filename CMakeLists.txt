cmake_minimum_required(VERSION 3.0)
project(doubri CXX)

include_directories(${PROJECT_SOURCE_DIR}/contrib)

add_executable(doubri-minhash minhash.cc MurmurHash3.cc)
target_compile_options(doubri-minhash PUBLIC -O3)

add_executable(doubri-self dedup_self.cc)
target_compile_options(doubri-self PUBLIC -O3)

add_executable(doubri-other dedup_other.cc)
target_compile_options(doubri-other PUBLIC -O3)

add_executable(doubri-init flag_init.cc)
target_compile_options(doubri-init PUBLIC -O3)

add_executable(doubri-apply flag_apply.cc)
target_compile_options(doubri-apply PUBLIC -O3)
