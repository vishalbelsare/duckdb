cmake_minimum_required(VERSION 2.8.7)

project(TileDBExtension)

include_directories(include /Users/hannes/source/TileDB/tiledb/sm/cpp_api /Users/hannes/source/TileDB/tiledb/sm/c_api /Users/hannes/source/tiledb/build/tiledb/tiledb)

add_library(
  tiledb_extension STATIC
  tiledb-extension.cpp)
