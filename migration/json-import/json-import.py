from datetime import datetime, timedelta
from queue import Queue, Full, Empty
import sys
import random
import json
import pymongo
from pymongo import InsertOne, DeleteOne, ReplaceOne, UpdateOne
import time
import threading
import os
import multiprocessing as mp
import argparse
from bson.json_util import loads


def initializeLogFile(appConfig):
    with open(appConfig['logFileName'], "w") as logFile:
        logFile.write("")


def logAndPrint(appConfig,string):
    with open(appConfig['logFileName'], "a") as logFile:
        logFile.write(string+"\n")
    print(string)
    
    
def setup(appConfig):
    if appConfig['dropCollection']:
        logAndPrint(appConfig,"  dropping the collection")
        client = pymongo.MongoClient(host=appConfig['uri'],appname='jsonimp')
        col = client[appConfig['databaseName']][appConfig['collectionName']]
        col.drop()
        client.close()


def reportCollectionInfo(appConfig):
    client = pymongo.MongoClient(host=appConfig['uri'],appname='jsonimp')
    db = client[appConfig['databaseName']]
    
    collStats = db.command("collStats", appConfig['collectionName'])
    
    compressionRatio = collStats['size'] / collStats['storageSize']
    gbDivisor = 1024*1024*1024
    
    logAndPrint(appConfig,"collection statistics | numDocs             = {0:12,d}".format(collStats['count']))
    logAndPrint(appConfig,"collection statistics | avgObjSize          = {0:12,d}".format(int(collStats['avgObjSize'])))
    logAndPrint(appConfig,"collection statistics | size (GB)           = {0:12,.4f}".format(collStats['size']/gbDivisor))
    logAndPrint(appConfig,"collection statistics | storageSize (GB)    = {0:12,.4f} ".format(collStats['storageSize']/gbDivisor))
    logAndPrint(appConfig,"collection statistics | compressionRatio    = {0:12,.4f}".format(compressionRatio))
    logAndPrint(appConfig,"collection statistics | totalIndexSize (GB) = {0:12,.4f}".format(collStats['totalIndexSize']/gbDivisor))
    
    client.close()


def reporter(appConfig,perfQ):
    numSecondsFeedback = 10
    numIntervalsTps = 5
    numWorkers = appConfig['numWorkers']

    if appConfig['debugLevel'] >= 1:
        logAndPrint(appConfig,'starting reporting thread')
    
    recentTps = []
    
    startTime = time.time()
    lastTime = time.time()
    lastTotalOps = 0
    nextReportTime = startTime + numSecondsFeedback
    intervalLatencyMs = 0
    
    numWorkersCompleted = 0
    totalOps = 0
    
    queueMessagesProcessed = 0
    
    while (numWorkersCompleted < numWorkers):
        time.sleep(numSecondsFeedback)
        nowTime = time.time()
        
        numLatencyBatches = 0
        numLatencyMs = 0

        queueMessagesProcessed = 0
        queueDrained = False
        while not queueDrained:
            try:
                qMessage = perfQ.get_nowait()
            except Empty:
                queueDrained = True
            queueMessagesProcessed += 1
            if qMessage['name'] == "batchCompleted":
                totalOps += qMessage['operations']
                numLatencyBatches += 1
                numLatencyMs += qMessage['latency']
            elif qMessage['name'] == "processCompleted":
                numWorkersCompleted += 1

        # total total
        elapsedSeconds = nowTime - startTime
        opsPerSecond = totalOps / elapsedSeconds

        # elapsed hours, minutes, seconds
        thisHours, rem = divmod(elapsedSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)
        
        # this interval
        intervalElapsedSeconds = nowTime - lastTime
        intervalOps = totalOps - lastTotalOps
        if intervalElapsedSeconds > 0:
            intervalOpsPerSecond = intervalOps / intervalElapsedSeconds
        else:
            intervalOpsPerSecond = 0
        if numLatencyBatches > 0:
            intervalLatencyMs = numLatencyMs // numLatencyBatches
        else:
            intervalLatencyMs = 0
        
        # recent intervals
        if len(recentTps) == numIntervalsTps:
            recentTps.pop(0)
        recentTps.append(intervalOpsPerSecond)
        totRecentTps = 0
        for thisTps in recentTps:
            totRecentTps += thisTps
        avgRecentTps = totRecentTps / len(recentTps)
        
        logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
        logAndPrint(appConfig,"[{}] elapsed {} | total ins/upd {:16,d} at {:12,.2f} p/s | last {} {:12,.2f} p/s | interval {:12,.2f} p/s | lat (ms) {:12}"
        .format(logTimeStamp,thisHMS,totalOps,opsPerSecond,numIntervalsTps,avgRecentTps,intervalOpsPerSecond,intervalLatencyMs))
        nextReportTime = nowTime + numSecondsFeedback
        
        lastTime = nowTime
        lastTotalOps = totalOps


def task_worker(workerNum,appConfig,perfQ):
    numOpsPerBatch = appConfig['numOpsPerBatch']
    numWorkers = appConfig['numWorkers']
    linesPerChunk = appConfig['linesPerChunk']
    opMode = appConfig['mode']
    numChunks = 1
    
    myLineStart = (workerNum*linesPerChunk)+1
    myLineEnd = (workerNum+1)*linesPerChunk
    
    if appConfig['debugLevel'] >= 1:
        logAndPrint(appConfig,"worker {} - start {} end {} chunk {}".format(workerNum,myLineStart,myLineEnd,numChunks))

    client = pymongo.MongoClient(host=appConfig['uri'],appname='jsonimp')
    db = client[appConfig['databaseName']]
    col = db[appConfig['collectionName']]
    
    if appConfig['debugLevel'] >= 1:
        logAndPrint(appConfig,"starting worker process {} - using collection {}.{}".format(workerNum,appConfig['databaseName'],appConfig['collectionName']))

    startTime = time.time()
    lastTime = time.time()

    numBatchesCompleted = 0
    numBatchOps = 0
    fileLineNum = 0
    insList = []

    with open(appConfig['fileName'], 'r') as f:
        for thisLine in f:
            fileLineNum += 1
            
            if (fileLineNum >= myLineStart) and (fileLineNum <= myLineEnd):
                # add to batch
                thisDict = loads(thisLine)
                numBatchOps += 1
                
                if opMode == 'insert':
                    insList.append(InsertOne(thisDict.copy()))
                elif opMode == 'replace':
                    insList.append(ReplaceOne({"_id":thisDict['_id']},thisDict.copy(),upsert=True))
                elif opMode == 'update':
                    insList.append(UpdateOne({"_id":thisDict['_id']},{"$set":thisDict.copy()},upsert=True))
                
                if (numBatchOps >= numOpsPerBatch):
                    batchStartTime = time.time()
                    result = col.bulk_write(insList, ordered=False)
                    batchElapsedMs = int((time.time() - batchStartTime) * 1000)
                    numBatchesCompleted += 1
                    perfQ.put({"name":"batchCompleted","operations":numBatchOps,"latency":batchElapsedMs,"timeAt":time.time()})
                    insList = []
                    numBatchOps = 0
                
            if (fileLineNum == myLineEnd):
                # increment boundaries
                myLineStart += (numWorkers*linesPerChunk)
                myLineEnd += (numWorkers*linesPerChunk)
                numChunks += 1
                if appConfig['debugLevel'] >= 1:
                    logAndPrint(appConfig,"worker {} - start {} end {} chunk {}".format(workerNum,myLineStart,myLineEnd,numChunks))
                
        if numBatchOps > 0:
            batchStartTime = time.time()
            result = col.bulk_write(insList, ordered=False)
            batchElapsedMs = int((time.time() - batchStartTime) * 1000)
            numBatchesCompleted += 1
            perfQ.put({"name":"batchCompleted","operations":numBatchOps,"latency":batchElapsedMs,"timeAt":time.time()})
    
    client.close()
    
    perfQ.put({"name":"processCompleted","processNum":workerNum,"timeAt":time.time()})


def main():
    parser = argparse.ArgumentParser(description='Bulk/Concurrent JSON file import utility.')

    parser.add_argument('--uri',
                        required=True,
                        type=str,
                        help='URI')

    parser.add_argument('--file-name',
                        required=True,
                        type=str,
                        help='Name of JSON file to load')

    parser.add_argument('--operations-per-batch',
                        required=True,
                        type=str,
                        help='Number of operations per batch')

    parser.add_argument('--workers',
                        required=True,
                        type=int,
                        help='Number of parallel workers')
                        
    parser.add_argument('--database',
                        required=True,
                        type=str,
                        help='Database name')

    parser.add_argument('--collection',
                        required=True,
                        type=str,
                        help='Collection name')

    parser.add_argument('--log-file-name',
                        required=True,
                        type=str,
                        help='Log file name')
                        
    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')

    parser.add_argument('--lines-per-chunk',
                        required=False,
                        type=int,
                        default=1000,
                        help='Number of lines each worker reserves before jumping ahead in the file to the next chunk')

    parser.add_argument('--debug-level',
                        required=False,
                        type=int,
                        default=0,
                        help='Debug output level.')

    parser.add_argument('--mode',
                        required=True,
                        type=str,
                        choices=['insert', 'replace', 'update'],
                        help='Mode - insert, replace, or update')

    parser.add_argument('--drop-collection',
                        required=False,
                        action='store_true',
                        help='Drop the collection prior to loading data')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['numOpsPerBatch'] = int(float(args.operations_per_batch))
    appConfig['numWorkers'] = int(args.workers)
    appConfig['databaseName'] = args.database
    appConfig['collectionName'] = args.collection
    appConfig['fileName'] = args.file_name
    appConfig['logFileName'] = args.log_file_name
    appConfig['linesPerChunk'] = args.lines_per_chunk
    appConfig['debugLevel'] = args.debug_level
    appConfig['mode'] = args.mode
    appConfig['dropCollection'] = args.drop_collection
    
    initializeLogFile(appConfig)

    logAndPrint(appConfig,'---------------------------------------------------------------------------------------')
    for thisKey in appConfig:
        if (thisKey == 'uri'):
            thisUri = appConfig[thisKey]
            thisParsedUri = pymongo.uri_parser.parse_uri(thisUri)
            thisUsername = thisParsedUri['username']
            thisPassword = thisParsedUri['password']
            thisUri = thisUri.replace(thisUsername,'<USERNAME>')
            thisUri = thisUri.replace(thisPassword,'<PASSWORD>')
            logAndPrint(appConfig,"  config | {} | {}".format(thisKey,thisUri))
        else:
            logAndPrint(appConfig,"  config | {} | {}".format(thisKey,appConfig[thisKey]))
    logAndPrint(appConfig,'---------------------------------------------------------------------------------------')
 
    setup(appConfig)

    mp.set_start_method('spawn')

    random.seed()
    
    q = mp.Manager().Queue()

    t = threading.Thread(target=reporter,args=(appConfig,q))
    t.start()
    
    processList = []
    for loop in range(appConfig['numWorkers']):
        p = mp.Process(target=task_worker,args=(loop,appConfig,q))
        processList.append(p)
        
    for process in processList:
        process.start()
        
    for process in processList:
        process.join()
        
    t.join()
    
    reportCollectionInfo(appConfig)
    

if __name__ == "__main__":
    main()


