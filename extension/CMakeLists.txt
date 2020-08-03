if(${BUILD_ICU_EXTENSION})
  add_subdirectory(icu)
endif()

if(${BUILD_PARQUET_EXTENSION})
  add_subdirectory(parquet)
endif()

if(${BUILD_TILEDB_EXTENSION})
  add_subdirectory(tiledb)
endif()
