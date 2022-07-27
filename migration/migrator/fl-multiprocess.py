from datetime import datetime, timedelta
import os
import sys
import time
import pymongo
from bson.timestamp import Timestamp
from bson.objectid import ObjectId
import threading
import multiprocessing as mp
import hashlib
import argparse


def logIt(threadnum, message):
    logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
    print("[{}] thread {:>3d} | {}".format(logTimeStamp,threadnum,message))


def full_load_loader(threadnum, appConfig, perfQ):
    if appConfig['verboseLogging']:
        logIt(threadnum,'thread started')

    sourceConnection = pymongo.MongoClient(appConfig["sourceUri"])
    sourceDb = sourceConnection[appConfig["sourceNs"].split('.',1)[0]]
    sourceColl = sourceDb[appConfig["sourceNs"].split('.',1)[1]]

    destConnection = pymongo.MongoClient(appConfig["targetUri"])
    destDatabase = destConnection[appConfig["targetNs"].split('.',1)[0]]
    destCollection = destDatabase[appConfig["targetNs"].split('.',1)[1]]

    startTime = time.time()
    lastFeedback = time.time()

    bulkOpList = []

    # list with replace, not insert, in case document already exists (replaying old oplog)
    bulkOpListReplace = []
    numCurrentBulkOps = 0

    numTotalBatches = 0

    myCollectionOps = 0

    if appConfig['verboseLogging']:
        logIt(threadnum,"Creating cursor")

    if (threadnum == 0):
        # thread 0 = $lte only
        cursor = sourceColl.find({'_id': {'$lte': ObjectId(appConfig['boundaries'][threadnum])}})
    elif (threadnum == appConfig['numProcessingThreads'] - 1):
        # last processor = $gt only
        cursor = sourceColl.find({'_id': {'$gt': ObjectId(appConfig['boundaries'][threadnum-1])}})
    else:
        # last processor = $gt prior, $lte next
        cursor = sourceColl.find({'_id': {'$gt': ObjectId(appConfig['boundaries'][threadnum-1]), '$lte': ObjectId(appConfig['boundaries'][threadnum])}})

    for doc in cursor:
        myCollectionOps += 1
        bulkOpList.append(pymongo.InsertOne(doc))
        # if playing old oplog, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
        #bulkOpListReplace.append(pymongo.ReplaceOne(doc['_id'],doc,upsert=True))
        numCurrentBulkOps += 1

        if (numCurrentBulkOps >= appConfig["maxInsertsPerBatch"]):
            if not appConfig['dryRun']:
            #    try:
                result = destCollection.bulk_write(bulkOpList,ordered=True)
            #    except:
            #    # replace inserts as replaces
            #        result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
            perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"processNum":threadnum})
            bulkOpList = []
            bulkOpListReplace = []
            numCurrentBulkOps = 0
            numTotalBatches += 1

    if (numCurrentBulkOps > 0):
        if not appConfig['dryRun']:
        #    try:
            result = destCollection.bulk_write(bulkOpList,ordered=True)
        #    except:
        #    # replace inserts as replaces
        #        result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
        perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"processNum":threadnum})
        bulkOpList = []
        bulkOpListReplace = []
        numCurrentBulkOps = 0
        numTotalBatches += 1

    perfQ.put({"name":"processCompleted","processNum":threadnum})


def reporter(appConfig, perfQ):
    if appConfig['verboseLogging']:
        logIt(-1,'reporting thread started')
    
    startTime = time.time()
    lastTime = time.time()
    
    lastProcessedOplogEntries = 0
    nextReportTime = startTime + appConfig["feedbackSeconds"]
    
    numWorkersCompleted = 0
    numProcessedOplogEntries = 0
    
    while (numWorkersCompleted < appConfig["numProcessingThreads"]):
        time.sleep(appConfig["feedbackSeconds"])
        nowTime = time.time()
        
        while not perfQ.empty():
            qMessage = perfQ.get_nowait()
            if qMessage['name'] == "batchCompleted":
                numProcessedOplogEntries += qMessage['operations']
            elif qMessage['name'] == "processCompleted":
                numWorkersCompleted += 1

        # total total
        elapsedSeconds = nowTime - startTime
        totalOpsPerSecond = numProcessedOplogEntries / elapsedSeconds

        # elapsed hours, minutes, seconds
        thisHours, rem = divmod(elapsedSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)
        
        # this interval
        intervalElapsedSeconds = nowTime - lastTime
        intervalOpsPerSecond = (numProcessedOplogEntries - lastProcessedOplogEntries) / intervalElapsedSeconds

        logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
        print("[{0}] elapsed {1} | total o/s {2:12,.2f} | interval o/s {3:12,.2f} | tot {4:16,d} | {5:12,d} secs behind".format(logTimeStamp,thisHMS,totalOpsPerSecond,intervalOpsPerSecond,numProcessedOplogEntries,-1))
        nextReportTime = nowTime + appConfig["feedbackSeconds"]
        
        lastTime = nowTime
        lastProcessedOplogEntries = numProcessedOplogEntries


def main():
    parser = argparse.ArgumentParser(description='Full Load migration tool.')

    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')
    
    parser.add_argument('--source-uri',
                        required=True,
                        type=str,
                        help='Source URI')

    parser.add_argument('--target-uri',
                        required=True,
                        type=str,
                        help='Target URI')

    parser.add_argument('--source-namespace',
                        required=True,
                        type=str,
                        help='Source Namespace as <database>.<collection>')
                        
    parser.add_argument('--target-namespace',
                        required=False,
                        type=str,
                        help='Target Namespace as <database>.<collection>, defaults to --source-namespace')
                        
    parser.add_argument('--feedback-seconds',
                        required=False,
                        type=int,
                        default=60,
                        help='Number of seconds between feedback output')

    parser.add_argument('--max-inserts-per-batch',
                        required=False,
                        type=int,
                        default=100,
                        help='Maximum number of inserts to include in a single batch')
                        
    parser.add_argument('--dry-run',
                        required=False,
                        action='store_true',
                        help='Read source changes only, do not apply to target')

    parser.add_argument('--verbose',
                        required=False,
                        action='store_true',
                        help='Enable verbose logging')

    parser.add_argument('--boundaries',
                        required=True,
                        type=str,
                        help='Boundaries for segmenting')


    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['sourceUri'] = args.source_uri
    appConfig['targetUri'] = args.target_uri
    appConfig['maxInsertsPerBatch'] = args.max_inserts_per_batch
    appConfig['feedbackSeconds'] = args.feedback_seconds
    appConfig['dryRun'] = args.dry_run
    appConfig['sourceNs'] = args.source_namespace
    if not args.target_namespace:
        appConfig['targetNs'] = args.source_namespace
    else:
        appConfig['targetNs'] = args.target_namespace
    appConfig['verboseLogging'] = args.verbose
    appConfig['boundaries'] = args.boundaries.split(',')
    appConfig['numProcessingThreads'] = len(appConfig['boundaries'])+1
    
    logIt(-1,"processing using {} threads".format(appConfig['numProcessingThreads']))

    mp.set_start_method('spawn')
    q = mp.Queue()

    t = threading.Thread(target=reporter,args=(appConfig,q))
    t.start()
    
    processList = []
    for loop in range(appConfig["numProcessingThreads"]):
        p = mp.Process(target=full_load_loader,args=(loop,appConfig,q))
        processList.append(p)
        
    for process in processList:
        process.start()
        
    for process in processList:
        process.join()
        
    t.join()


if __name__ == "__main__":
    main()
