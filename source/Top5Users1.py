'''
    APPROACH #1: Handle large file and do computation on single machine
        T = ~ O(n)

    - Pass 1: make dictionary of (K=IP, V=count)
            Doable in O(n) time, O(n) space

    - Pass 2.1: make dictionary of (K=count, V=list(IP))
            Doable in O(n) time, O(n) space
    OR
    - Pass 2.2: Maintain a min-count heap (count,IP) of size k, replace min-count head-node with larger value node, do heapify
            Doable in O(n * log k) but general case will be quiet below this, O(k) space


    APPROACH #2: Break large file and do Map-Reduce jobs (try later)
'''

import csv
import os
from os import listdir
from os.path import isfile, join
import heapq

# constant
ONE_GIGABYTE = pow(2,30)

def getTopKUsers(k=5):
    IP_Count = dict()

    cwd = os.getcwd()
    dirPath = cwd+'/../logs'
    onlyfiles = [f for f in listdir(dirPath) if (isfile(join(dirPath, f)) and f.endswith(".csv"))]
    for aFile in onlyfiles[1:]:
        fullFilePath = join(dirPath, aFile)
        with open(fullFilePath, "rb") as csvfile:
            # line by line csv-reader-object iterator for csv-file-object, csv-file-object points to disk file!
            datareader = csv.reader(csvfile)
            for row in datareader:
                print(datareader)
                if (row[1] in IP_Count.keys()):
                    IP_Count[row[1]] += 1
                else:
                    IP_Count[row[1]] = 1

    heap = []
    for key in IP_Count.keys()[:k]:
        heapq.heappush(heap, (IP_Count[key],key))

    for key in IP_Count.keys()[k:]:
        if (heap[0][0] < IP_Count[key]):
            heapq.heappop(heap)
            heapq.heappush(heap, (IP_Count[key], key))

    for item in heap:
        print(item)


if __name__ == '__main__':
    getTopKUsers()
