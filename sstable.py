import os, struct, hashlib, pickle, traceback, glob
from constants import BLOOM_FILTER_LEN
from heap import MinHeap

def get_sst_file_with_max_filename():
    sst_files = [
        os.path.join('/tmp/lsmpy/sst', f)
        for f in os.listdir('/tmp/lsmpy/sst')
        if f.endswith(".sst") and os.path.isfile(os.path.join('/tmp/lsmpy/sst', f))
    ]
    if len(sst_files):
        sst_files.sort()
        return sst_files[-1].split('/')[-1][:-4]
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

def fetch_latest_kv_from_offset(f):
    lenbytes = f.read(8) # this 8 bytes store the len of key and value
    if len(lenbytes) < 8:
        return None, None
    klen, vlen = struct.unpack('>II', lenbytes)
    curr_key = f.read(klen)
    value = f.read(vlen)
    return curr_key.decode(), value.decode()

class SSTCursor:
    '''
    sotre the state of open .sst file, 
    we can't keep all the .sst files to be compacted in RAM, hence we use this method
    we store it current state (filename, offset etc) and fetch data as required
    '''
    def __init__(self, filepath):
        self.filepath = filepath
        self.f = open(filepath, 'rb')
        self.offset = 0
    
    def read_next(self):
        key, value = fetch_latest_kv_from_offset(self.f)
        return key, value

class SSTable:
    def __init__(self, stem):
        self.sstdir = '/tmp/lsmpy/sst'
        self.filepath = os.path.join(self.sstdir, stem)
        self.sstable_path = f'{self.filepath}.sst'
        self.sparse_index_path = f'{self.filepath}.spx'
        self.bloom_filter_path = f'{self.filepath}.blf'

        self.sparse_index = [] # array of tuple (key, offset)
        self.bloom_filter = [None] * BLOOM_FILTER_LEN # array with binary values None/0
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
        lp, rp = 0, len(self.sparse_index)-1
        while(lp <= rp):
            mid = lp + (rp-lp) // 2
            if key >= self.sparse_index[mid][0]:
                lp = mid + 1
            else:
                rp = mid - 1

        offset = 0
        if rp >= 0:
            offset = self.sparse_index[rp][1]

        with open(self.sstable_path, 'rb') as f:
            f.seek(offset)
            key_cnt = 10
            while key_cnt:
                key_cnt -= 1
                curr_key, value = self.fetch_latest_kv_from_offset(f)
                if curr_key == key:
                    return value

        return ''
    
    def create_sst_files(sstable = None, key = None, value = None, sparse_yn = False):
        current_offset = 0
        # 1. create data block file
        if isinstance(key, str):
            key = key.encode("utf-8")
        if isinstance(value, str):
            value = value.encode("utf-8")
        
        # write to data filr (.sst)
        with open(sstable.sstable_path, 'ab') as f:
            current_offset = f.tell()
            f.write(struct.pack('>II', len(key), len(value))) # I : 4 bytes integer
            f.write(key)
            f.write(value)
            f.flush() # push to OS cache
            os.fsync(f.fileno()) # push to actual hard drive
    
        # 2. create sparse index file (.spx)
        if sparse_yn: 
            sstable.sparse_index.append((key.decode(), current_offset))

        # 3. create bloom filter file (.blf)
        for pos in get_hashes(key, BLOOM_FILTER_LEN):
            sstable.bloom_filter[pos] = 1
    
    def _commit_index_file(self):
        '''
        save the sparse index and bloom filter binary file to disk
        '''
        # save sparse index file (.spx)
        print(f'=>> dumping_sparse_index: {self.sparse_index_path}')
        with open(self.sparse_index_path, 'wb') as f:
            pickle.dump(self.sparse_index, f)

        # save bloom filter file (.blf)
        with open(self.bloom_filter_path, 'wb') as f:
            pickle.dump(self.bloom_filter, f)

    def _delete_wal(self, memtable):
        '''
        deletes the WAL associated with memtable
        '''
        if os.path.exists(memtable.walpath):
            os.remove(memtable.walpath)
    
    @staticmethod
    def create_sstable_from_memtable(memtable, total_key = 1000):
        '''
        persist skiplist data to datafile in disk, this function also persist deleted keys
        deleted keys are removed at time of sst datafile compaction
        '''
        try:
            pathname = '/tmp/lsmpy/sst/' + str(int(get_sst_file_with_max_filename()) + 1)
            sstable = SSTable(pathname)
            node = memtable.skiplist.start_node
            level_zero = 0
            cnt = 0

            while node and node.levels[level_zero]:
                key = node.levels[level_zero].k
                value = node.levels[level_zero].v
                cnt += 1
                SSTable.create_sst_files(
                    sstable = sstable,
                    key = key,
                    value = value,
                    sparse_yn = cnt % 10 == 0
                )
                node = node.levels[level_zero]

            sstable._commit_index_file()
            sstable._delete_wal(memtable)

            SSTable._compaction()
            
            return sstable
        except Exception as e:
            traceback.print_exc()

    @staticmethod
    def _compaction():
        # todo
        # 1. get all .sst file, if count is >= 3, continue compaction
        # 2. open all .sst files with 
        # 3. create a new .sst file which will have combined keys (create cursor)
        # 4. push all the top (keys, filename) to minimumHeap. MinimumHeap will be sorted
        # according to Keys and then according to filename
        # 5. get the minumum (key, filename) using .peek() method
        # 6. put the key to new .sst file
        # 7. move ahead in the .sst file which was just peeked, and push it to the heap
        # 8. peek the heap again and repeat step 6

        sst_files = glob.glob(f"{SSTable.sstdir}*.sst")
        if len(sst_files < 3):
            return

        minheap = MinHeap()

        sst_files_state = { sst_file: SSTCursor(sst_file) for sst_file in sst_files}
        
        for _, file_st in sst_files_state.items():
            key, value = file_st.read_next()
            minheap.push(key=key, filename=file_st.filepath, value=value)

        # create new sst file
        pathname = '/tmp/lsmpy/sst/' + str(int(get_sst_file_with_max_filename()) + 1)
        sstable = SSTable(pathname)
        cnt = 0

        while True:
            min_node = minheap.pop()
            sstable.create_sst_files(
                sstable=sstable,
                key=min_node.key,
                value=min_node.value,
                sparse_yn =  cnt % 10 == 0
            )

            sst_st = sst_files_state[min_node.filename]
            key, value = sst_st.read_next()
            minheap.push(key=key, filename=file_st.filepath, value=min_node.filename)

            # open sst_file
            # key, value = _fetch_latest_kv_from_offset(fileObject)
            # minheap.push(k, sst_file.name)
            

            

        


        return
