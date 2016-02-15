'''
    APPROACH #1: Handle large file and do computation on single machine
        T = ~ O(n)

    - Pass 1: write csv dictionary of (K=IP, V=count)
            HANDLING: What if the dictionary itself is too big to fit in RAM?

    - Pass 2: Maintain a min-count heap (count,IP) of size k, replace min-count head-node with larger value node, do heapify
            Doable in O(n * log k) but general case will be quiet below this, O(k) space


    APPROACH #2: Break large file and do Map-Reduce jobs (try later)
                PROBLEM: Hadoop 1.1.2 HDFS Connection Refused, Mapper not mapping properly. Reducer set to 1 for now.
'''

import os
from os import listdir
from os.path import isfile, join
import heapq

# constant
from itertools import islice

ONE_GIGABYTE = pow(2,30)
NL = ' '

# solution: http://stackoverflow.com/a/8009942
# solution: http://stackoverflow.com/a/519653
def readInChunks1(fileObj, chunkSize=ONE_GIGABYTE):
    """
        Lazy function to read a file piece by piece.
        Default chunk size: ONE GIGABYTE
    """
    while True:
        data = fileObj.read(chunkSize) # problem here?
        if not data:
            break
        yield data

def readInChunks(fileObj, chunkSize=10000):
    while True:
        data = list(islice(fileObj, chunkSize))
        if not data:
            break
        yield data

def getTopKUsers(k=5):
    cwd = os.getcwd()
    dirPath = cwd+'/../logs'
    readWriteMode = 'r+'

    rToggle = 0
    wToggle = 1

    dFullFilePath = [join(dirPath, 'data_store1.txt'), join(dirPath, 'data_store2.txt')]
    dictObjList = [open(dFullFilePath[rToggle],readWriteMode), open(dFullFilePath[wToggle],readWriteMode)]
    dictObjList[rToggle].writelines("0.0.0.0" + ',' + "0" + NL) # dummy write

    IP_Count = dict() # local dictionary for current raw chunk
    onlyfiles = [f for f in listdir(dirPath) if (isfile(join(dirPath, f)) and f.endswith(".csv"))]
    for aFile in onlyfiles[1:]:
        fullFilePath = join(dirPath, aFile)
        fo = open(fullFilePath,"r")
        for chunk in readInChunks(fo):
            # read raw chunk
            for line in chunk: #.splitlines():
                ip = line.split(',')[1]
                if (ip in IP_Count.keys()):
                    IP_Count[ip] += 1
                else:
                    IP_Count[ip] = 1

            # merge this chunk and write dictionary, TODO: how to put it first time?
            for eachIP in IP_Count.keys():
                dictObjList[rToggle].seek(0,0)
                foundInDictFile = False
                for dChunk in readInChunks(dictObjList[rToggle]):
                    for line in dChunk: #.splitlines():
                        print(rToggle, line)

                        line = line.strip()
                        if line == '':
                            continue

                        ip,cntStr = line.split(',')
                        cntStr = cntStr.strip()

                        if (eachIP == ip):
                            writeBuff = ip + ',' + str(IP_Count[ip] + int(cntStr)) + NL
                            dictObjList[wToggle].writelines(writeBuff)
                            foundInDictFile = True
                            break

                        if (ip not in IP_Count.keys()):
                            writeBuff = ip + ',' + cntStr + NL
                            dictObjList[wToggle].writelines(writeBuff)

                    if foundInDictFile == True:
                        break

                if foundInDictFile == False: # if still not found!
                    writeBuff = eachIP + ',' + str(IP_Count[eachIP]) + NL
                    # dictObjList[wToggle].writelines(writeBuff)

                # update read-write turns swapped for next chunk
                temp = rToggle
                rToggle = wToggle
                wToggle = temp
                # dictObjList[rToggle].flush()
                # dictObjList[wToggle].flush()
                dictObjList[wToggle] = open(dFullFilePath[wToggle],readWriteMode) # reset write file

            # clear local chunk dictionary
            IP_Count.clear()

    for dChunk in readInChunks(dictObjList[rToggle]):
        for line in dChunk: #.splitlines():
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