import random
import os
import datetime

def one(): # dummy: time consuming!
    currTimeStamp = str(datetime.datetime.now()).replace(':','.')
    fileName = currTimeStamp + '.csv'
    cwd = os.getcwd()
    dirPath = cwd+'/../logs'
    fullFilePath = dirPath + '/' + fileName

    oneGB = pow(10,9)
    f = open(fullFilePath, "ab")
    for i in range(0,9,1):
        size = oneGB # 10 GiB
        f.write("a,b,c,d\n" * size)
        f.flush()
        os.fsync(f.fileno())
    f.close()
    os.stat(fullFilePath).st_size

def two():  # quick & useless
    currTimeStamp = str(datetime.datetime.now()).replace(':','.')
    fileName = currTimeStamp + '.csv'
    cwd = os.getcwd()
    dirPath = cwd+'/../logs'
    fullFilePath = dirPath + '/' + fileName

    f = open(fullFilePath, "wb")
    size = pow(10,10) # 10 GiB
    f.seek(size-1)
    f.write("\0")
    f.close()
    os.stat(fullFilePath).st_size

if __name__ == '__main__':
    two()
    one()
