import random
import hashlib
import uuid
import os
import struct



class Node():
    def __init__(self, val=-1, levels = 16, k=None, v=None):
        self.val = val
        self.k = k
        self.v = v
        
        self.levels = [None] * levels 


class Skiplist():
    def __init__(self):
        self.max_levels = 16
        self.start_node = Node(levels=self.max_levels)
    
    def _toss(self):
        lvl = 1
        # Standard Skip List level generation: p = 0.5
        while random.random() < 0.5 and lvl < self.max_levels:
            lvl += 1
        return lvl # Returns 1 to 16


    def _iter(self, key):
        # find the starting node level for particular value and iterate through all its levels top to down
        node = self.start_node
        
        for cur_level in range(self.max_levels-1, -1, -1):
            while (node.levels[cur_level] is not None) and key > node.levels[cur_level].k:
                node = node.levels[cur_level] # move right
            yield node, cur_level     

    def find(self, k):
        for node, cur_level in self._iter(k):
            if node.levels[cur_level] and node.levels[cur_level].k == k:
                return node.levels[cur_level]
        return False   

    def add(self, value, k, v):
        new_lev = self._toss()
        new_node = Node(val=value, levels=new_lev, k=k, v=v)
        
        for node, cur_lev in self._iter(k):
            if cur_lev < new_lev:
                new_node.levels[cur_lev] = node.levels[cur_lev]
                node.levels[cur_lev] = new_node

    def update(self, value, k, v):
        node = self.find(k)
        if node is not False:
            node.val = value
            node.k = k
            node.v = v
            return True
        return False

    # write with tombstone value
    def delete(self, k):
        node = self.find(k)
        if node is not False:
            node.val = -1
            return True
        return False

    def print_levels(self):
        for lvl in range(self.max_levels - 1, -1, -1):
            node = self.start_node.levels[lvl]
            print(f"Level {lvl:02d}: ", end="")
            while node:
                print(node.k, end=" -> ")
                node = node.levels[lvl]
            print("None")


class WAL():
    def __init__(self, file_name = None):
        self.dir = '/tmp/lsmpy/wal'
        self._create_or_get_wal(file_name)

    # ??? todo: fix this logic, 'file_name' arg is not being used here
    def _create_or_get_wal(self, file_name):
        os.makedirs(self.dir, exist_ok=True)
        if not file_name:
            self.file_name = f'{uuid.uuid4()}.wal'
            self.path = os.path.join(self.dir, self.file_name)
            open(self.path, "a").close()
        else:
            wal_files = [
                os.path.join(self.dir, f)
                for f in os.listdir(self.dir)
                if f.endswith(".wal")
            ]
            if len(wal_files) == 1:
                self.path = wal_files[0]
                self.file_name = os.path.basename(wal_files[0])
            else:
                self.file_name = f'{uuid.uuid4()}.wal'
                self.path = os.path.join(self.dir, self.file_name)
                open(self.path, "a").close()

    def append(self, k, v):
        if isinstance(k, str):
            k = k.encode("utf-8")
        if isinstance(v, str):
            v = v.encode("utf-8")
            
        with open(self.path, 'ab') as f:
            f.write(struct.pack('>II', len(k), len(v))) # I : 4 bytes integer
            f.write(k)
            f.write(v)
            f.flush() # push to OS cache
            os.fsync(f.fileno()) # push to actual hard drive


class Memtable():
    def __init__(self, walpath=None):
        self.skiplist = Skiplist() # this stays in memory
        self.current_size = 0
        self.key_cnt = 0
        self.walpath = walpath

    def _string_to_unique_int(self, input_string):
        hash_object = hashlib.sha256(input_string.encode())
        hex_dig = hash_object.hexdigest()
        return int(hex_dig, 16)
    
    def find(self, k):
        print(f'->> memtable_find_k: {k}::{type(k)}')
        value = self._string_to_unique_int(k)
        node = self.skiplist.find(k)
        if node is not False:
            return node.v
        return ''
    
    def add(self, k, v):
        value = self._string_to_unique_int(k)
        self.skiplist.add(value, k, v)
        self.current_size += len(k) + len(v) + 28 # interger = 28 byts
        self.key_cnt += 1

    def update(self, k, v):
        value = self._string_to_unique_int(k)
        self.skiplist.update(value, k, v)
        self.current_size += len(k) + len(v) + 28 # interger = 28 byts
        self.key_cnt += 1

    def delete(self, k):
        value = self._string_to_unique_int(k)
        self.skiplist.delete(k)
        self.current_size += len(k) + 28 # interger = 28 byts
        self.key_cnt += 1



