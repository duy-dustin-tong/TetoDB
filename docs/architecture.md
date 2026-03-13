# TetoDB Architecture

## Query Pipeline

1. Lexer tokenizes SQL input
2. Parser builds AST nodes
3. Planner builds logical plan nodes and expressions
4. Optimizer applies rule-based rewrites
5. Execution engine constructs and runs executor trees

Core executor families include:

- Scan: seq scan, index scan
- Join: nested loop join, hash join
- DML: insert, update, delete
- Relational: projection, filter, sort, top-N, distinct, aggregation, set operations

## Storage Engine

- `DiskManager` handles page and WAL file IO
- `BufferPoolManager` caches pages with two-queue replacement behavior
- `TableHeap` stores tuples across linked table pages
- B+Tree index subsystem supports key lookup and uniqueness enforcement

## Transactions And Concurrency

- Transaction manager tracks txn states and write sets
- Lock manager provides tuple-level shared/exclusive locks and upgrade path
- Wait-die strategy is used to avoid deadlock cycles

## Logging, Checkpointing, And Recovery

- Mutating operations append WAL records
- Background log flush thread persists WAL
- Background checkpoint thread flushes WAL and dirty pages
- Recovery manager performs ARIES-style redo then undo on startup

## Server Model

- `teto_main` starts `TetoDBInstance` and TCP server
- TCP server handles connections concurrently via worker threads
- Protocol is custom TetoWire packet format
