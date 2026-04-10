class Node:
    def __init__(self, key=None, value=None, filename=None):
        self.key = key
        self.value = value
        self.filename = filename

class MinHeap:
    '''
    Heap is binary tree to efficiently fetch min/max value, with heap
    peek -> O(1)
    push/pop -> O log(K) -> k: no. of nodes in heap
    '''
    def __init__(self):
        self.arr = []
    
    def push(self, key=None, value=None, filename=None):
        node = Node(key=key, value=value, filename=filename)
        self.arr.append(node)
        self._heapify_up()

    def _heapify_up(self):
        idx = len(self.arr) - 1
        while idx:
            if self.arr[idx].key < self.arr[(idx-1) // 2].key:
                self.arr[idx], self.arr[(idx-1) // 2] = self.arr[(idx-1) // 2], self.arr[idx]
                idx = (idx-1) // 2
            elif self.arr[idx].key == self.arr[(idx-1) // 2].key and \
                    self.arr[idx].filename < self.arr[(idx-1) // 2].filename:
                self.arr[idx], self.arr[(idx-1) // 2] = self.arr[(idx-1) // 2], self.arr[idx]
                idx = (idx-1) // 2
            else:
                break
                
    def pop(self):
        node = self.arr[0]
        heap_sz = len(self.arr)
        self.arr[0] = self.arr[heap_sz - 1]
        self.arr.pop() # remove last element of arr, reducing its length by 1
        self._heapify_down()
        return node
    
    def _heapify_down(self):
        heap_sz = len(self.arr)
        idx = 0 # -> this is my parent

        while True:
            # if my parent is greater than any of the child swap with that child
            c1_idx = (idx * 2) + 1
            c2_idx = (idx * 2) + 2

            if c1_idx < heap_sz and self.arr[idx].key > self.arr[c1_idx].key:
                self.arr[idx], self.arr[c1_idx] = self.arr[c1_idx], self.arr[idx]
                idx = c1_idx
            elif c2_idx <len and self.arr[idx].key > self.arr[c2_idx].key:
                self.arr[idx], self.arr[c2_idx] = self.arr[c2_idx], self.arr[idx]
                idx = c2_idx
            elif c2_idx <len and self.arr[idx].key == self.arr[c2_idx].key and \
                    self.arr[idx].filename > self.arr[c2_idx].filename:
                self.arr[idx], self.arr[c2_idx] = self.arr[c2_idx], self.arr[idx]
                idx = c2_idx
            else:
                break
