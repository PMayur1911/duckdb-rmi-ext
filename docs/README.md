# DuckDB RMI Extension

This project adds a Recursive Model Index (RMI) to DuckDB: a learned, single-column numeric index plus a custom scan operator and optimizer rule. It targets workloads with point and short-range predicates and exposes pragmas to inspect model behavior, prediction errors, and overflow handling.

This was built as a course project for `CSCI-543: Foundations of Modern Data Management and Processing` during the Fall 2025 semester at University of Southern California.

## Highlights
- Learned index models: configurable via `WITH (model='linear' | 'poly' | 'two_layer')`, defaulting to linear.
- Single-column numeric support (integer/float types); no unique/primary key constraints.
- Optimizer rule swaps eligible `seq_scan` nodes for an RMI-backed scan when constant equality or range predicates are present on the indexed column.
- Diagnostic pragmas to introspect models, per-key errors, and overflow.

## Build & Run
Make sure you clone this repository by
```bash
git clone --recurse-submodules https://github.com/PMayur1911/duckdb-rmi-ext.git
```
Note that `--recurse-submodules` will ensure DuckDB is pulled which is required to build the extension.
- Start the Dockerized Container
  ```bash
  docker compose build
  docker compose up -d
  docker compose exec dev bash

  docker exec -it <container-id> /bin/bash    # Use this only if you are trying to exec into an already running container
  ```
- Once inside, navigate into the project folder and build the binaries
  ```bash
  cd /workspace/duckdb-rmi-ext
  GEN=ninja make -j2
  ```
- This shall generate a `/build/release` folder, with all the binaries and compiled extensions. To run an instnace of DuckDB:
  ```bash
  ./build/release/duckdb
  ```
- Notes:
  - The docker container installs build essentials, ninja, ccache, gdb, and maps `./` to `/workspace` with shared ccache for faster rebuilds.
  - The above build steps by default injects the RMI extension into the DuckDB's executable binary. If you prefer to run the raw version of DuckDB and load the extension manually:
    ```bash
    ./build/release/duckdb -unsigned        # To allow loading of unsigned extensions

    LOAD '/path/to/rmi.duckdb_extension';   # To load the RMI Extension
    ```

## Build & Debug
To debug the Learned index and model logic, DuckDB must be built with debug symbols enabled. 
- Inside the container
  ```bash
  cd /workspace/duckdb-rmi-ext
  make clean
  ```
- Configure a Debuggable Build using `RelWithDebInfo`, which enables optimizations while preserving debug symbols
  ```bash
  GEN=ninja cmake \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -S duckdb \
  -B build/rwdi

  cmake --build build/rwdi -j2
  ```

- Start DuckDB inside GDB:
  ```bash
  gdb ./build/rwdi/duckdb     # Starts GDB
  ```
  You can attach GDB to an already running DuckDB instance by:
  ```bash
  ps aux | grep duckdb      # Extract Process ID of the running DuckDB instance
  gdb -p <pid-of-duckdb>
  ```

## Diagnostics (Pragmas)
- `PRAGMA rmi_index_info();` — list RMI indexes (catalog/schema/index/table).
- `SELECT * FROM rmi_index_model_info('schema.index');` — model metadata (type, errors, overflow, coefficients).
- `SELECT * FROM rmi_index_model_stats('schema.index');` — per-key stats: key, row_id, actual_position, predicted_position, error, abs_error.
- `SELECT * FROM rmi_index_overflow('schema.index');` — overflow map contents.
- `SELECT * FROM rmi_index_dump('schema.index');` — dump sorted key/row_id pairs from the main index.

## Learned RMI Index Usage Example
```sql
CREATE TABLE t (id INTEGER, value DOUBLE);

-- Linear Data
INSERT INTO t SELECT id, CAST(id AS DOUBLE) FROM range(1, 10001) AS r(id);
-- Uniformly Distributed Data
INSERT INTO t SELECT id, random() * 100000, random() * 100000 FROM range(1, 1001) AS r(id);
-- Randomly Distributed Skewed Data
INSERT INTO t SELECT id, (exp(random() * 5) - 1) * 1000 FROM range(1, 10001) AS r(id);

-- Build an RMI index
CREATE INDEX idx_rmi ON t USING RMI (value) WITH (model = 'linear');

-- Inspect index internals (Pragmas)
SELECT * FROM rmi_index_info();
SELECT * FROM rmi_index_dump('idx_rmi') LIMIT 20;
SELECT * FROM rmi_index_model_stats('idx_rmi') LIMIT 20;
SELECT * FROM rmi_index_overflow('idx_rmi');
SELECT * FROM rmi_index_model_info('idx_rmi');

-- See plan and runtime
EXPLAIN SELECT * FROM t WHERE value < 5.0;
EXPLAIN ANALYZE SELECT * FROM t WHERE value < 5.0;
```

## Project Artifacts
- Core sources:
  - `src/include/`: public headers for the RMI index, models, and module registration.
  - `src/rmi`: Implementation files for Index, models and module
    - `rmi_index.cpp`: RMI index implementation (build/train, insert/delete overflow, search).
    - `rmi_index_plan.cpp`: planner hook to build the physical create-index pipeline.
    - `rmi_index_physical_create.cpp`: physical operator to collect data, train, and register the index.
    - `rmi_optimize_scan.cpp`: optimizer extension that swaps `seq_scan` with `rmi_index_scan` when predicates qualify.
    - `rmi_index_scan.cpp`: table function for index-backed scans and result fetching.
    - `rmi_index_pragmas.cpp`: PRAGMA/table functions to introspect indexes, models, stats, and overflow.
    - `rmi_linear_model.cpp`: linear model implementation for predictions/errors/overflow.
    - `rmi_poly_model.cpp`: polynomial model implementation.
    - `rmi_two_layer_model.cpp`: two-layer model (root + segmented leaves).
  - `src/rmi_extension.cpp`: entry point wiring all registrations into DuckDB.

- Benchmarks: Contain synthetic workloads (uniform/skewed distributions) for point and short-range queries.
  - `benchmarks_1k`
  - `benchmarks_10k`
  - `benchmarks_100k` 

- Reports and slides: 
  - `docs/CSCI-543_Project Report.pdf`
  - `docs/CSCI-543_Project Slides.pdf`
  - `docs/CSCI-543_Project Proposal.pdf`.

## Benchmarks
Each benchmark folder (`benchmarks_1k`, `benchmarks_10k`, `benchmarks_100k`) includes setup scripts, inserts, queries, and a master runner.
Typical flow (example for 10k):
```bash
cd /workspace/duckdb-rmi-ext
./clean_benchmarks.sh

cd benchmarks_10k
./setup_benchmark_env.sh      # Setup Benchmarking Environment
bash run_all.sh               # Benchmarks all index variants (RMI, vanilla) and builds results

# results logged to benchmarks_10k/master_run.log and per-subdir logs
```
