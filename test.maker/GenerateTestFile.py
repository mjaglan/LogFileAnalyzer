import random
import os
import datetime

# random ip
import random
import socket
import struct


def generateLogFile():
    fileSizeLimit = pow(10,10) # 10 GB
    httpStatusCodes = [x for x in range(100,600,40)]

    currTimeStamp = str(datetime.datetime.now()).replace(':','.')
    fileName = currTimeStamp + '.csv'
    cwd = os.getcwd()
    dirPath = cwd+'/../logs'
    fullFilePath = dirPath + '/' + fileName

    fileObj = open(fullFilePath,'w+')
    recordCount = 0
    while(os.stat(fullFilePath).st_size < fileSizeLimit):
        randomIP_2 = str(socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff))))
        pageName_3 = 'http://domain.com/somepage/?var1=' + str(random.randint(1040,1050))
        aRecord = str(datetime.datetime.now()) + ',' + randomIP_2 + ',' + pageName_3 + ',' + str(random.choice(httpStatusCodes)) + '\n'
        fileObj.write(aRecord)
        recordCount += 1
        if (recordCount > 10000): # every 10K records
            fileObj.flush()
            os.fsync(fileObj.fileno())
    fileObj.close()

if __name__ == '__main__':
    generateLogFile()