cmake_minimum_required(VERSION 3.16)

project(learned_rmi_extension)

# CRITICAL: DuckDB provides DUCKDB_EXTENSION_PATH during superbuild.
include(${DUCKDB_EXTENSION_PATH}/duckdb_extension.cmake)

set(TARGET_NAME learned_rmi)

set(EXTENSION_SOURCES
    src/learned_rmi_extension.cpp
    src/learned_index_base.cpp
    src/regression_utils.cpp
    src/linear_rmi_index.cpp
    src/poly_rmi_index.cpp
    src/piecewise_linear_rmi_index.cpp
)

# Register extension
duckdb_extension_definition(${TARGET_NAME} ${EXTENSION_SOURCES})

# Build a DuckDB shell with extension linked
duckdb_extension_load(${TARGET_NAME})

# Build tests
duckdb_extension_test(${TARGET_NAME})
