-> client
    -> read_req: support multiple processes
    -> write: single process

-> server
    -> mem table + wal
    -> write to sst
    -> bloom filter
