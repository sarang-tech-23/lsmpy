import os, struct, hashlib, pickle, traceback

def get_sst_file_with_max_filename():
    sst_files = [
        os.path.join('/tmp/lsmpy/sst', f)
        for f in os.listdir('/tmp/lsmpy/sst')
        if f.endswith(".sst") and os.path.isfile(os.path.join('/tmp/lsmpy/sst', f))
    ]
    if len(sst_files):
        sst_files.sort()
        return sst_files[-1]
    return '0'

def get_hashes(key, m, k=3):
    '''
    key = key
    m = length of bloom filter
    k = no. of different hash functions 
    '''
    digest = hashlib.md5(key).digest()

    h1 = int.from_bytes(digest[:8], 'big')
    h2 = int.from_bytes(digest[8:], 'big')

    for i in range(k):
        yield (h1 + i * h2) % m

class SsTable:
    def __init__(self, stem):
        self.sstdir = '/tmp/lsmpy/sst'
        self.filepath = os.path.join(self.sstdir, stem)
        self.sstable_path = f'{self.filepath}.sst'
        self.sparse_index_path = f'{self.filepath}.spx'
        self.bloom_filter_path = f'{self.filepath}.blf'

        self.sparse_index = [] # array of tuple (key, offset)
        self.bloom_filter = [] # array with binary values None/0
        self._load_metadata()

    def _load_metadata(self):
        if os.path.exists(self.sparse_index_path):
            with open(self.sparse_index_path, 'rb') as f:
                self.sparse_index = pickle.load(f)

        if os.path.exists(self.bloom_filter_path):
            with open(self.bloom_filter_path, 'rb') as f:
                self.bloom_filter = pickle.load(f)

    def get(self, key):
        # check the bloom filter
        for pos in get_hashes(key.encode("utf-8"), len(self.bloom_filter)):
            if not self.bloom_filter[pos]:
                return ''
        
        # check the sparse index -> binary search over keys
        # sparse_index(key, current_offset)
        lp, rp = 0, len(self.sparse_index)
        while(lp <= rp):
            mid = lp + (rp-lp) // 2
            if key >= self.sparse_index[mid][0]:
                lp = mid + 1
            else:
                rp = mid - 1

        with open(self.sstable_path, 'rb') as f:
            f.seek(self.sparse_index[rp][1])
            key_cnt = 10
            while key_cnt:
                key_cnt -= 1
                lenbytes = f.read(8)
                klen, vlen = struct.unpack('>II', lenbytes)
                curr_key = f.read(klen)
                value = f.read(vlen)
                if curr_key.decode() == key:
                    return value.decode()

        return ''
    
    @staticmethod
    def create_sstable_from_memtable(memtable, total_key = 1000):
        try:
            pathname = '/tmp/lsmpy/sst/' + str(int(get_sst_file_with_max_filename()) + 1)
            sstable = SsTable(pathname)
            node = memtable.skiplist.start_node
            level_zero = 0
            prev_key = ''
            cnt = 0
            sstable.bloom_filter = [None] * total_key * 10
            bloom_filter_len = len(sstable.bloom_filter)
            current_offset = 0

            while node and node.levels[level_zero]:
                if prev_key == node.levels[level_zero].k:
                    node = node.levels[level_zero]
                    continue
                cnt += 1
                key = node.levels[level_zero].k
                value = node.levels[level_zero].v
                
                # 1. create data block file
                if isinstance(key, str):
                    key = key.encode("utf-8")
                if isinstance(value, str):
                    value = value.encode("utf-8")
                with open(sstable.sstable_path, 'ab') as f:
                    current_offset = f.tell()
                    f.write(struct.pack('>II', len(key), len(value))) # I : 4 bytes integer
                    f.write(key)
                    f.write(value)
                    f.flush() # push to OS cache
                    os.fsync(f.fileno()) # push to actual hard drive
            
                # 2. create sparse index file
                if cnt % 10 == 0: 
                    sstable.sparse_index.append((key.decode(), current_offset))

                # 3. create bloom filter
                for pos in get_hashes(key, bloom_filter_len):
                    sstable.bloom_filter[pos] = 1
                
                print(f'=>> sstable_write: {prev_key}')
                prev_key = node.levels[level_zero].k
                node = node.levels[level_zero]
            
            # save sparse index
            print(f'=>> dumping_sparse_index: {sstable.sparse_index_path}')
            with open(sstable.sparse_index_path, 'wb') as f:
                pickle.dump(sstable.sparse_index, f)

            # save bloom filter 
            with open(sstable.bloom_filter_path, 'wb') as f:
                pickle.dump(sstable.bloom_filter, f)

            # delete wal todo
            if os.path.exists(memtable.walpath):
                os.remove(memtable.walpath)
            
            return sstable
        except Exception as e:
            traceback.print_exc()
