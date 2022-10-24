from datetime import datetime, timedelta
import sys
import random
import json
import pymongo
from pymongo import InsertOne, DeleteOne, UpdateOne
import time
import threading
#from bson import json_util
import os


numOps = 500000

percentInserts = 4
percentUpdates = 95
percentDeletes = 1

# set to true to always create offsetting data (so sum of checksum is always 0)
checkSumming = True

checksumMax = 100

dropCollection = False

numSecondsFeedback = 5

numArgs = len(sys.argv) - 1

if (numArgs == 1):
    connectionString = sys.argv[1]
else:
    print("Usage: python3 cdc-correctness.py <connection-string>")
    sys.exit(1)

numTotalInserts = 0
numTotalUpdates = 0
numTotalDeletes = 0

numThreadsCompleted = 0
numInsertThreads = 1

#numExistingDocuments = 0
pkHighWaterMark = 0

databaseName = 'cdctest'
collectionName = 'coll'


def printConfig():
    print("numOps = {}".format(numOps))
    print("percentInserts = {}".format(percentInserts))
    print("percentUpdates = {}".format(percentUpdates))
    print("percentDeletes = {}".format(percentDeletes))
    print("dropCollection = {}".format(dropCollection))
    print("checksumMax = {}".format(checksumMax))
    print("")


def setup():
    global numExistingDocuments
    global pkHighWaterMark

    client = pymongo.MongoClient(connectionString)
    db = client[databaseName]
    col = db[collectionName]
    
    if dropCollection:
        print("dropping collection {}".format(collectionName))
        col.drop()
    
    colIndexes = col.index_information()

    # create indexes if needed
    
    # db.coll.createIndex({"pk":1},{"name":"pkIndex","background":false,"unique":true})
    thisIndexName = 'pkIndex'
    if thisIndexName not in colIndexes:
        print("Creating UNIQUE index {}".format(thisIndexName))
        col.create_index([('pk', pymongo.ASCENDING)], name=thisIndexName, background=False, unique=True)
    else:
        print("Index {} already exists".format(thisIndexName))
        
    # get pk high water mark
    result = col.find_one({},sort=[("pk",-1)])
    #result = col.find({},sort("pk",-1).limit(1)
    if (result is not None) and ("pk" in result):
        pkHighWaterMark = result["pk"]
    print("pkHighWaterMark = {}".format(pkHighWaterMark))
        
    client.close()


def reporter():
    print('starting reporting thread')
    
    startTime = time.time()
    lastTime = time.time()
    lastTotalInserts = 0
    lastTotalUpdates = 0
    lastTotalDeletes = 0
    nextReportTime = startTime + numSecondsFeedback
    
    while (numThreadsCompleted < numInsertThreads):
        time.sleep(numSecondsFeedback)
        nowTime = time.time()

        # total total
        elapsedSeconds = nowTime - startTime
        insertsPerSecond = numTotalInserts / elapsedSeconds
        updatesPerSecond = numTotalUpdates / elapsedSeconds
        deletesPerSecond = numTotalDeletes / elapsedSeconds

        # elapsed hours, minutes, seconds
        thisHours, rem = divmod(elapsedSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)
        
        # this interval
        intervalElapsedSeconds = nowTime - lastTime
        intervalInsertsPerSecond = (numTotalInserts - lastTotalInserts) / intervalElapsedSeconds
        intervalUpdatesPerSecond = (numTotalUpdates - lastTotalUpdates) / intervalElapsedSeconds
        intervalDeletesPerSecond = (numTotalDeletes - lastTotalDeletes) / intervalElapsedSeconds
        
        numTotalOps = numTotalInserts + numTotalUpdates + numTotalDeletes

        logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
        print("[{0}] elapsed {1} | total ops {2:16,d} | interval ins / upd / del {3:12,.2f} / {4:12,.2f} / {5:12,.2f}".format(logTimeStamp,thisHMS,numTotalOps,intervalInsertsPerSecond,intervalUpdatesPerSecond,intervalDeletesPerSecond))
        nextReportTime = nowTime + numSecondsFeedback
        
        lastTime = nowTime
        lastTotalInserts = numTotalInserts
        lastTotalUpdates = numTotalUpdates
        lastTotalDeletes = numTotalDeletes


def inserter(threadNum):
    global numThreadsCompleted, pkHighWaterMark, numTotalInserts, numTotalUpdates, numTotalDeletes

    client = pymongo.MongoClient(connectionString)
    db = client[databaseName]
    col = db[collectionName]
    
    print("starting insert thread {} - bulk2 - starting at {} - using collection {}.{}".format(threadNum,pkHighWaterMark,databaseName,collectionName))
    
    thisOps = 0
    
    totalPercent = percentInserts + percentUpdates + percentDeletes
    
    while (thisOps < numOps):
        thisOps += 1
        
        thisOpPercent = random.randint(1,totalPercent)
        
        if (thisOpPercent <= percentInserts) or (thisOps < 100):
            # insert - first 100 operations MUST be inserts
            
            pkHighWaterMark += 1
            thisChecksum = random.randint(1,checksumMax)
            
            # do the insert
            thisInsert = {}
            thisInsert['pk'] = pkHighWaterMark
            thisInsert['checksum'] = thisChecksum
            result = col.insert_one(thisInsert)
            numTotalInserts += 1
            
            if checkSumming:
                # attempt update with offset
                result = col.update_one({'pk':random.randint(1,pkHighWaterMark)},{"$inc":{"checksum":-1*thisChecksum}})
                
                if (result.modified_count == 0):
                    # if update fails, insert offset
                    pkHighWaterMark += 1
                
                    thisInsert = {}
                    thisInsert['pk'] = pkHighWaterMark
                    thisInsert['checksum'] = -1 * thisChecksum
                    result = col.insert_one(thisInsert)
                    numTotalInserts += 1
                else:
                    # update succeeded
                    numTotalUpdates += 1

        elif thisOpPercent <= (percentInserts + percentUpdates):
            # update
            thisChecksum = random.randint(1,checksumMax)
            
            # attempt the update with offset
            result = col.update_one({'pk':random.randint(1,pkHighWaterMark)},{"$inc":{"checksum":thisChecksum}})
            
            if (result.modified_count == 1):
                # if successful, attempt update with offset
                numTotalUpdates += 1
                
                if checkSumming:
                    result = col.update_one({'pk':random.randint(1,pkHighWaterMark)},{"$inc":{"checksum":-1*thisChecksum}})
                
                    # if update fails, insert offset
                    if (result.modified_count == 0):
                        # if update fails, insert offset
                        pkHighWaterMark += 1
                    
                        thisInsert = {}
                        thisInsert['pk'] = pkHighWaterMark
                        thisInsert['checksum'] = -1 * thisChecksum
                        result = col.insert_one(thisInsert)
                        numTotalInserts += 1
                    else:
                        # update succeeded
                        numTotalUpdates += 1
        
        else:
            # delete
            
            thisPk = random.randint(1,pkHighWaterMark)
            
            result = col.find_one({'pk':thisPk})
            
            if (result is not None) and ("checksum" in result):
                # attempt the delete
                thisChecksum = result['checksum']
                
                result = col.delete_one({'pk':thisPk})
                
                if (result.deleted_count == 1):
                    numTotalDeletes += 1
                    
                if checkSumming:
                    result = col.update_one({'pk':random.randint(1,pkHighWaterMark)},{"$inc":{"checksum":thisChecksum}})
                
                    # if update fails, insert offset
                    if (result.modified_count == 0):
                        # if update fails, insert offset
                        pkHighWaterMark += 1
                    
                        thisInsert = {}
                        thisInsert['pk'] = pkHighWaterMark
                        thisInsert['checksum'] = thisChecksum
                        result = col.insert_one(thisInsert)
                        numTotalInserts += 1
                    else:
                        # update succeeded
                        numTotalUpdates += 1
            
    client.close()
    
    numThreadsCompleted += 1


def main():

    print("performing ~{0:16,d} operations across {1} threads".format(numOps,numInsertThreads))

    random.seed()
    printConfig()
    setup()
    
    threadList = []
    t = threading.Thread(target=reporter)
    threadList.append(t)
    for loop in range(numInsertThreads):
        t = threading.Thread(target=inserter,args=(loop,))
        threadList.append(t)
        
    for thread in threadList:
        thread.start()
        
    for thread in threadList:
        thread.join()


if __name__ == "__main__":
    main()
