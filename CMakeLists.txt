cmake_minimum_required(VERSION 3.22.1)
project(storage)
set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


include_directories(lib)
add_compile_options(-mwaitpkg)

add_executable(main src/main.cpp)

