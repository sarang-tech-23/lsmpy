import threading, pathlib, struct, sys, os
from memtable import Memtable, WAL
from sstable import SSTable

class LSMTree():
    def __init__(self, mem_size=640):
        self.waldir = '/tmp/lsmpy/wal'
        self.sstdir = '/tmp/lsmpy/sst'
        self.inactive_memtables = []
        self.mem_size = mem_size
        self.write_lock = threading.Lock()
        self.sstable_list = []
        self._init_schema()
        self._load_sstable()
        self._recover_memtables()
    
    def _init_schema(self):
        os.makedirs(self.waldir, exist_ok=True)
        os.makedirs(self.sstdir, exist_ok=True)

    def _load_sstable(self):
        ls_sstable = list(pathlib.Path(self.sstdir).glob("*.sst"))
        ls_sstable.sort(reverse=True)

        # load create instances for each sst data file and loads its sparse index and
        # bloom filter in memory for fast reads
        for sst_file in ls_sstable:
            self.sstable_list.append(SSTable(sst_file.stem))
    
    def _replay_wal(self, walfile):
        '''
        there could be multiple wal
        wal naming convention should also be in ascending order
        '''
        with open(walfile, 'rb') as f:
            while True:
                lenbytes = f.read(8)
                if not lenbytes:
                    return b'', b''
                klen, vlen = struct.unpack('>II', lenbytes)
                k = f.read(klen)
                v = f.read(vlen)
                yield k.decode('utf-8'), v.decode('utf-8')

    def _recover_memtables(self):
        # get list of all WAL from self.waldir
        lsowal = os.listdir(self.waldir)
        for wal in lsowal:
            self.active_wal = WAL(wal)
            self.active_memtable = Memtable(walpath = self.active_wal.path)
            for k, v in self._replay_wal(self.active_wal.path):
                if not k:
                    break
                self.active_memtable.add(k, v)
        if not lsowal:
            self.active_wal = WAL()
            self.active_memtable = Memtable(walpath = self.active_wal.path)

    def get(self, k):
        # 1, check active memtable
        val = self.active_memtable.find(k)
        if val:
            return val
        
        # 2. inactive memtable
        for inactive_memtable in self.inactive_memtables:
            val = inactive_memtable.find(k)
            if val:
                return val
            
        # 3. check sstable
        for sst in reversed(self.sstable_list):
            val = sst.get(k)
            if val:
                return val
        return ''

    def delete(self, k):
        with self.write_lock:
            self.active_memtable.delete(k=k)
            self.active_wal.append(k=k, v='')
        return 'ok'

    def update(self, k, v):
        with self.write_lock:
            self.active_memtable.update(k=k, v=v)
            self.active_wal.append(k=k, v=v)
        return 'ok'

    def add(self, k, v):
        with self.write_lock:
            self.active_memtable.add(k=k, v=v)
            self.active_wal.append(k=k, v=v)
            print(f'=>> memtable_size: {self.active_memtable.current_size} / {self.mem_size}')
            if self.active_memtable.current_size >= self.mem_size:
                # todo
                sstable = SSTable.create_sstable_from_memtable(self.active_memtable, self.active_memtable.key_cnt)
                self.sstable_list.append(sstable)

                '''
                1. migrate data to SStable
                2. functionality to query SStable
                3. bloom filter
                4. SStable compaction
                '''
                # self.inactive_memtables.append(self.active_memtable)
                self.active_wal = WAL()
                self.active_memtable = Memtable(self.active_wal.path)

        return 'ok'

