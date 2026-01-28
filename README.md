# Transaction Processing System - Two-Phase Locking (2PL)

A concurrent transaction processing system implementing strict Two-Phase Locking with per-key locking for high concurrency. Ensures serializability while allowing parallel execution of non-conflicting transactions.

## Features

- **Two-Phase Locking (2PL)**: Strict 2PL scheduler with growing, execution, and shrinking phases
- **Per-Key Locking**: Fine-grained exclusive locking allows parallel execution of non-conflicting transactions
- **Deadlock Prevention**: Timeout-based abort strategy prevents circular waits
- **Lock Striping**: 256-striped mutexes in storage for maximum concurrency
- **Distributed Support**: Framework for distributed transaction coordination

## Test Results

| Metric | Result |
|--------|--------|
| **Test Duration** | 600 seconds (10 minutes) |
| **Availability** | 100% (`ok-fraction: 1.0`) |
| **Transactions** | 2,963 processed, 0 failed |
| **Serializability** | No anomalies detected |

## Build

```bash
cmake --preset dev
cmake --build --preset dev
```

## Run

```bash
./build/dev/tx_system
```

## Maelstrom Test

```bash
cd build/dev/_deps/maelstrom-src
lein run test --workload txn-list-append --time-limit 600 --node-count 1 --bin ./tx_system
```

## Implementation Details

### Two-Phase Locking Scheduler
- **Growing Phase**: Acquire all required locks before any operation
- **Execution Phase**: Execute all operations while holding locks
- **Shrinking Phase**: Release all locks after completion

### Thread-Safe Storage
- 256 independent mutex stripes (lock striping pattern)
- Each key hashes to a specific stripe
- Operations on different stripes proceed in parallel

### Dependencies
- C++20 STL
- Boost (thread, json, asio, log)

## License

MIT License
