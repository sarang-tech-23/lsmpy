### Description
- storage engine inspired by LSM tree.

### Commands to run
- start server `python3 server.py`
- run client.py in another terminal to interact with server `python3 client.py`

### Features
- **Memtable (uses skiplist)**: in memory store to perform read, writes in O(Log N) time complexity.
- **WAL**: Write ahead log to recover memtable in case of crash.
- **SStable**: Flush memtable to immutable sorted files in disk after a certain size threshold is reached.
- **Sparse index**: Finds keys offset in particular data file using binary search.
- **Bloom filter**: To check whether a key may present in data file or not.
- **Compaction**: Process to merge multiple SStable using Minheap.
