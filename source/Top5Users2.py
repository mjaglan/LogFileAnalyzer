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

import os
from os import listdir
from os.path import isfile, join
import heapq

# constant
ONE_GIGABYTE = pow(2,30)

# solution: http://stackoverflow.com/a/8009942
# solution: http://stackoverflow.com/a/519653
def readInChunks(fileObj, chunkSize=ONE_GIGABYTE):
    """
        Lazy function to read a file piece by piece.
        Default chunk size: ONE GIGABYTE
    """
    while True:
        data = fileObj.read(chunkSize)
        if not data:
            break
        yield data


def getTopKUsers(k=5):
    cwd = os.getcwd()
    dirPath = cwd+'/../logs'

    dFullFilePath = join(dirPath, 'data_store1.txt') # 0
    dictionaryFile1 = open(dFullFilePath,"a+")
    dictionaryFile1.write("0.0.0.0" + ',' + "0" + '\n') # dummy write

    dFullFilePath = join(dirPath, 'data_store2.txt') # 1
    dictionaryFile2 = open(dFullFilePath,"a+")
    dictionaryFile2.write("0.0.0.0" + ',' + "0" + '\n') # dummy write

    dictObjList = [dictionaryFile1, dictionaryFile2]
    rToggle = 0
    wToggle = 1
    IP_Count = dict() # local dictionary for current raw chunk
    onlyfiles = [f for f in listdir(dirPath) if (isfile(join(dirPath, f)) and f.endswith(".csv"))]
    for aFile in onlyfiles[1:]:
        fullFilePath = join(dirPath, aFile)
        fo = open(fullFilePath,"r")
        for chunk in readInChunks(fo):
            # read raw chunk
            for line in chunk.splitlines():
                ip = line.split(',')[1]
                if (ip in IP_Count.keys()):
                    IP_Count[ip] += 1
                else:
                    IP_Count[ip] = 1

            # merge this chunk and write dictionary, TODO: how to put it first time?
            for dChunk in readInChunks(dictObjList[rToggle]):
                writeBuff = ''
                for line in dChunk.splitlines():
                    print(line)
                    ip,cntStr = line.split(',')
                    cnt = int(cntStr)
                    if (ip in IP_Count.keys()):
                        IP_Count[ip] += cnt
                        writeBuff = writeBuff + ip + ',' + IP_Count[ip] + '\n'
                    else:
                        # WARNING: DO NOT add to IP_Count, otherwise memory overflow may happen!
                        writeBuff = writeBuff + ip + ',' + cnt + '\n'
                dictObjList[wToggle].write(writeBuff)

            # update read-write turns swapped for next chunk
            temp = rToggle
            rToggle = wToggle
            wToggle = temp

            # clear local chunk dictionary
            IP_Count.clear()

    for dChunk in readInChunks(dictObjList[rToggle]):
        for line in dChunk.splitlines():
            ipn,cntStr = line.split(',')
            cnt = int(cntStr)
            print(ipn,cnt)

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