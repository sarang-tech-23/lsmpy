### Commands to run
- start server 'python3 server.py'
- run client.py in another terminal to interact with server `python3 client.py`

### Features
- Uses skiplist (memtable) data structure for in memory read/writes/update/delete in O (log N) time complexity.
- WAL to recover memtable in case of crash.
- Flush memtable to data file (SStable) in disk after a certain size threshold is reached.
- Sparse index to easily find keys offset in data file using binary search.
- Bloom filter to check whether a key is present in datafile or not.
- Compaction process to merge multiple SStable using Minheap.
