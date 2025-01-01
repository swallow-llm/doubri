cmake_minimum_required(VERSION 3.3)
set(CMAKE_CXX_STANDARD 23)
project(doubri CXX)

include_directories(${PROJECT_SOURCE_DIR}/contrib)

add_executable(doubri-minhash minhash.cc MurmurHash3.cc)
target_compile_options(doubri-minhash PUBLIC -O3)

add_executable(doubri-self dedup_self.cc)
target_compile_options(doubri-self PUBLIC -O3)

# OpenMP
find_package(OpenMP REQUIRED)
if(OpenMP_FOUND)
	set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

#add_executable(doubri-other dedup_other.cc)
#target_compile_options(doubri-other PUBLIC -O3)

#add_executable(doubri-init flag_init.cc)
#target_compile_options(doubri-init PUBLIC -O3)

#add_executable(doubri-apply flag_apply.cc)
#target_compile_options(doubri-apply PUBLIC -O3)
