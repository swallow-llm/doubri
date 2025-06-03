cmake_minimum_required(VERSION 3.5)
project(doubri CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -s -DNDEBUG -march=native")

include_directories(${PROJECT_SOURCE_DIR}/contrib)

add_executable(doubri-minhash minhash.cc MurmurHash3.cc)
target_compile_options(doubri-minhash PUBLIC)

add_executable(doubri-dedup dedup.cc)
target_compile_options(doubri-dedup PUBLIC)

add_executable(doubri-merge merge.cc)
target_compile_options(doubri-merge PUBLIC)

add_executable(doubri-similarity similarity.cc)
target_compile_options(doubri-similarity PUBLIC)

add_executable(doubri-apply-whole apply-whole.cc)
target_compile_options(doubri-apply-whole PUBLIC)

add_executable(doubri-apply-each apply-each.cc)
target_compile_options(doubri-apply-each PUBLIC)

# OneTBB
find_package(TBB REQUIRED)
target_link_libraries(doubri-dedup TBB::tbb)
target_link_libraries(doubri-similarity TBB::tbb)
