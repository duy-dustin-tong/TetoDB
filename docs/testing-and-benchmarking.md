# Testing And Benchmarking

TetoDB uses:

- Google Test for unit/regression tests
- Google Benchmark for performance benchmarks

## Covered Areas

Primary unit coverage (`src/test_units.cpp`) includes:

- `Value` arithmetic/comparison behavior
- `DiskManager` page allocation and IO
- `BufferPoolManager` pin/fetch/eviction paths
- `TwoQueueReplacer` FIFO to LRU behavior
- `Tuple` serialization/deserialization
- `LockManager` shared/exclusive locking

Additional focused tests:

- `test/test_empty_agg.cpp`
- `test/test_like_bug.cpp`
- `test/test_savepoint.cpp`

Benchmark coverage (`src/benchmarks.cpp`) includes:

- Value arithmetic and string comparison
- Sequential disk page writes
- Buffer pool cache-hit and random-access scenarios
- B+Tree random lookup workload
- Hash join build/probe pressure

## Build Test Targets

```powershell
cmake --build build --config Release --target tetodb_tests
cmake --build build --config Release --target test_bug22_empty_agg
cmake --build build --config Release --target test_like_bug
cmake --build build --config Release --target test_savepoint
```

## Run Tests

```powershell
./build/Release/tetodb_tests.exe
./build/Release/test_bug22_empty_agg.exe
./build/Release/test_like_bug.exe
./build/Release/test_savepoint.exe
```

Linux/macOS executables are typically under `./build/`.

## Build And Run Benchmarks

```powershell
cmake --build build --config Release --target tetodb_benchmarks
./build/Release/tetodb_benchmarks.exe
```

Filter examples:

```powershell
./build/Release/tetodb_benchmarks.exe --benchmark_filter="BPlusTree|HashJoin"
./build/Release/tetodb_benchmarks.exe --benchmark_filter="BPM"
```

## Important Benchmark Note

For B+Tree write operations in benchmark/test code, pass a valid `Transaction*` for insert/remove paths; do not pass `nullptr` for those write operations.
