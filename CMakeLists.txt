cmake_minimum_required(VERSION 3.5)
project(spm_final)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-std=c++17 -pthread")# -g -DTRACE_FASTFLOW
cmake_minimum_required (VERSION 2.6)
include_directories(skyline)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_executable(spm_final skyline/trial.cpp)
