def getTopKUsers():
    '''
    APPROACH #1: Handle large file and do computation on single machine
        T = ~ O(n)

    - Pass 1: make dictionary of (K=IP, V=count)
            Doable in O(n) time

    - Pass 2: make dictionary of (K=count, V=list(IP))
            Doable in O(n) time, O(k) space
    OR
    - Pass 2: Maintain a min-count heap (count,IP) of size k, replace min-count head-node with larger value node, do heapify
            Doable in O(n*logk) but general case will be quiet below this, O(k) space


    APPROACH #2: Break large file and do Map-Reduce jobs (try later)
    '''
    pass