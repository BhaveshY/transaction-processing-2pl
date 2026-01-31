# Implementation Description

## Transaction Protocol: Two-Phase Locking (2PL)

I implemented strict 2PL with per-key exclusive locks.

### How it works

Each transaction goes through three phases:

1. **Growing phase** - grab all the locks we need before doing anything
2. **Execution phase** - run the reads/appends while holding locks
3. **Shrinking phase** - release everything at the end

### Concurrency

The key insight is that transactions touching different keys don't block each other. I use a `LockManager` that maintains a separate lock entry per key. If two transactions want different keys, they both proceed in parallel.

For the storage layer, I split the hash map into 256 "stripes", each with its own mutex. This way threads accessing different keys don't contend on a single global lock.

### Deadlock handling

Instead of detection, I went with prevention via timeouts. If a transaction can't get a lock after spinning and yielding for a while, it just aborts and the caller can retry. Simple but effective.

### Files I wrote

- `TwoPLScheduler.h/cpp` - the main scheduler logic
- `LockManager.h/cpp` - per-key locking with timeout-based abort
- `ThreadSafeKVStore.h/cpp` - striped concurrent hash map
- `DistributedCoordinator.h/cpp` - routing keys to nodes for distributed mode

The distributed stuff uses message passing between nodes to coordinate locks and operations across the cluster.
