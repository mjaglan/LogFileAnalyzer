'''
    APPROACH #1: Handle large file and do computation on single machine
        T = ~ O(n)

    - Pass 1: make dictionary of (K=IP, V=count)
            Doable in O(n) time

    - Pass 2.1: make dictionary of (K=count, V=list(IP))
            Doable in O(n) time, O(k) space
    OR
    - Pass 2.2: Maintain a min-count heap (count,IP) of size k, replace min-count head-node with larger value node, do heapify
            Doable in O(n*logk) but general case will be quiet below this, O(k) space


    APPROACH #2: Break large file and do Map-Reduce jobs (try later)
'''

import csv
import os
from os import listdir
from os.path import isfile, join
import heapq

# constant
ONE_GIGABYTE = pow(2,30)


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
    IP_Count = dict()

    cwd = os.getcwd()
    dirPath = cwd+'/../logs'
    onlyfiles = [f for f in listdir(dirPath) if (isfile(join(dirPath, f)) and f.endswith(".csv"))]
    for aFile in onlyfiles[1:]:
        fullFilePath = join(dirPath, aFile)
        f = open(fullFilePath,"r")
        for chunk in readInChunks(f):
            print("\n\n")
            print chunk[2]
            # handle csv

if __name__ == '__main__':
    getTopKUsers()