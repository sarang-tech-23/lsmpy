from sstable import SsTable

def migrate_memtable_to_sstable(memtable, key_cnt):
    node = memtable.skiplist.start_node
    level = 0
    prev_key = None

    SsTable.create_sstable_from_memtable(memtable, key_cnt)
    # ss_table = SStable()
    # while node.levels[level]:
    #     if prev_key != node.levels[level].k:
    #         print(node.levels[level].k, node.levels[level].v)
    #         # instead of printing store it to file 
    #         # ss_table.add(key, val)
        
    #     prev_key = node.levels[level].k
    #     node = node.levels[level]

    # add it to the some ss table list

    # after datablock file is created, assign it to level 0 and highest 
    # sequqnce no. on that level
    # create bloom filter
    # check whats and index file and create it too
    # and then delete the wal of memtable
    print(f'=>> migration finished')