from datetime import datetime, timedelta
import os
import sys
import time
import pymongo
from bson.timestamp import Timestamp
import threading
import multiprocessing as mp
import hashlib
import argparse
from collections import defaultdict


#Logger function
def logIt(threadnum, message):
    logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
    print("[{}] thread {:>3d} | {}".format(logTimeStamp,threadnum,message))

#Function to process the change stream

def change_stream_processor(threadnum, appConfig, perfQ):
    if appConfig['verboseLogging']:
        logIt(threadnum,'thread started')

    sourceConnection = pymongo.MongoClient(host=appConfig["sourceUri"],appname='mvutool')
    destConnection = pymongo.MongoClient(host=appConfig["targetUri"],appname='mvutool')
    startTime = time.time()
    lastFeedback = time.time()
    lastBatch = time.time()
    allDone = False
    threadOplogEntries = 0
    waitcount=0
    nsBulkOpDict = defaultdict(list)
    nsBulkOpDictReplace= defaultdict(list)
    # list with replace, not insert, in case document already exists (replaying old oplog)
    numCurrentBulkOps = 0
    numTotalBatches = 0
    printedFirstTs = False
    myClusterOps = 0

    # starting timestamp
    endTs = appConfig["startTs"]

    if (appConfig["startTs"] == "RESUME_TOKEN") and not appConfig["sourceDb"] :
        stream = sourceConnection.watch(resume_after={'_data': appConfig["startPosition"]}, full_document='updateLookup', pipeline=[{'$match': {'operationType': {'$in': ['insert','update','replace','delete']}}},{'$project':{'updateDescription':0}}])

    elif not appConfig["sourceColl"]:
        sourceDatabase=sourceConnection[appConfig["sourceDb"]]
        stream = sourceDatabase.watch(resume_after={'_data': appConfig["startPosition"]}, full_document='updateLookup', pipeline=[{'$match': {'operationType': {'$in': ['insert','update','replace','delete']}}},{'$project':{'updateDescription':0}}])

    else:
        sourceDatabase=sourceConnection[appConfig["sourceDb"]]
        sourceCollection = sourceDatabase[appConfig["sourceColl"]]
        stream = sourceCollection.watch(resume_after={'_data': appConfig["startPosition"]}, full_document='updateLookup', pipeline=[{'$match': {'operationType': {'$in': ['insert','update','replace','delete']}}},{'$project':{'updateDescription':0}}])

    if appConfig['verboseLogging']:
        if (appConfig["startTs"] == "RESUME_TOKEN"):
            logIt(threadnum,"Creating change stream cursor for resume token {}".format(appConfig["startPosition"]))

    while  not allDone:
        while stream.alive:
            change = stream.try_next()
            if ((time.time() - startTime) > appConfig['durationSeconds']) and (appConfig['durationSeconds'] != 0):
                allDone = True
                break
            applyLastbatch=False
            if change is  None and waitcount <=appConfig["maxSecondsBetweenBatches"]+1:
                waitcount=waitcount+1
                time.sleep(1)
                continue
            elif waitcount>(appConfig["maxSecondsBetweenBatches"]+1):
                waitcount=0
                applyLastbatch=True

            if not applyLastbatch:
                endTs = change['clusterTime']
                resumeToken = change['_id']['_data']
                thisDb = change['ns']['db']
                thisCol=change['ns']['coll']
                thisNs=thisDb+'.'+thisCol
                thisOp = change['operationType']
                if ((int(hashlib.sha512(str(change['documentKey']).encode('utf-8')).hexdigest(), 16) % appConfig["numProcessingThreads"]) == threadnum):
                    threadOplogEntries += 1
                    if (not printedFirstTs) and (thisOp in ['insert','update','replace','delete']):
                        if appConfig['verboseLogging']:
                            logIt(threadnum,'first timestamp = {} aka {}'.format(change['clusterTime'],change['clusterTime'].as_datetime()))
                        printedFirstTs = True

                    if (thisOp == 'insert'):
                        myClusterOps += 1
                        nsBulkOpDict[thisNs].append(pymongo.InsertOne(change['fullDocument']))
                        nsBulkOpDictReplace[thisNs].append(pymongo.ReplaceOne(change['documentKey'],change['fullDocument'],upsert=True))
                        numCurrentBulkOps += 1
                    elif (thisOp in ['update','replace']):
                        # update/replace
                        if (change['fullDocument'] is not None):
                            myClusterOps += 1
                            nsBulkOpDict[thisNs].append(pymongo.ReplaceOne(change['documentKey'],change['fullDocument'],upsert=True))
                            nsBulkOpDictReplace[thisNs].append(pymongo.ReplaceOne(change['documentKey'],change['fullDocument'],upsert=True))
                            numCurrentBulkOps += 1
                        else:
                            pass
                    elif (thisOp == 'delete'):
                        myClusterOps += 1
                        nsBulkOpDict[thisNs].append(pymongo.DeleteOne({'_id':change['documentKey']['_id']}))
                        nsBulkOpDictReplace[thisNs].append(pymongo.DeleteOne({'_id':change['documentKey']['_id']}))
                        numCurrentBulkOps += 1
                    elif (thisOp in ['drop','rename','dropDatabase','invalidate']):
                        # operations we do not track
                        pass
                    else:
                        print(change)
                        sys.exit(1)

            if ((numCurrentBulkOps >= appConfig["maxOperationsPerBatch"]) or (time.time() >= (lastBatch + appConfig["maxSecondsBetweenBatches"])) ) and (numCurrentBulkOps > 0):
                bulkOpList=[]
                bulkOpListReplace=[]
                if not appConfig['dryRun']:
                    for ns in nsBulkOpDict:
                        destDatabase=destConnection[(ns.split('.',1)[0])]
                        destCollection=destDatabase[(ns.split('.',1)[1])]
                        bulkOpList=nsBulkOpDict[ns]
                        try:
                            result = destCollection.bulk_write(bulkOpList,ordered=True)
                        except:
                            # replace inserts as replaces
                            bulkOpListReplace=nsBulkOpDictReplace[ns]
                            result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
                perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"endts":endTs,"processNum":threadnum,"resumeToken":resumeToken})
                nsBulkOpDict = defaultdict(list)
                nsBulkOpDictReplace= defaultdict(list)
                numCurrentBulkOps = 0
                numTotalBatches += 1
                lastBatch = time.time()

    if (numCurrentBulkOps > 0):
        bulkOpList=[]
        bulkOpListReplace=[]
        print("Inside While",numCurrentBulkOps)
        if not appConfig['dryRun']:
            for ns in nsBulkOpDict:
                destDatabase=destConnection[(ns.split('.',1)[0])]
                destCollection=destDatabase[(ns.split('.',1)[1])]
                bulkOpList=nsBulkOpDict[ns]
                try:
                    result = destCollection.bulk_write(bulkOpList,ordered=True)
                except:
                    # replace inserts as replaces
                    bulkOpListReplace=nsBulkOpDictReplace[ns]
                    result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
        nsBulkOpDict = defaultdict(list)
        nsBulkOpDictReplace= defaultdict(list)
        numCurrentBulkOps = 0
        numTotalBatches += 1

    sourceConnection.close()
    destConnection.close()
    perfQ.put({"name":"processCompleted","processNum":threadnum})

#Function to get the Change stream token
def get_resume_token(appConfig):
    sourceConnection = pymongo.MongoClient(host=appConfig["sourceUri"],appname='mvutool')

    allDone = False
    if not appConfig["sourceDb"]:
        stream = sourceConnection.watch()
        logIt(-1,'getting current change stream resume token')
    elif not appConfig["sourceColl"]:
        sourceDatabase=sourceConnection[appConfig["sourceDb"]]
        stream=sourceDatabase.watch()
        logIt(-1,'getting current change stream resume token for ' + appConfig["sourceDb"] + " database")
    else:
        sourceDatabase=sourceConnection[appConfig["sourceDb"]]
        sourceCollection = sourceDatabase[appConfig["sourceColl"]]
        stream=sourceCollection.watch()
        logIt(-1,'getting current change stream resume token for ' + appConfig["sourceDb"] + " database " + appConfig["sourceColl"] + " collection")

    while not allDone:
        for change in stream:
            resumeToken = change['_id']['_data']
            logIt(-1,'Change stream resume token is {}'.format(resumeToken))
            filename="get-resume-token-"+time.strftime("%Y%m%d-%H%M%S")+".txt"
            f = open(filename, "w")
            f.write("Change stream resume token is "+ str(resumeToken))
            f.close()
            allDone = True
            break


def reporter(appConfig, perfQ):
    if appConfig['verboseLogging']:
        logIt(-1,'reporting thread started')

    startTime = time.time()
    lastTime = time.time()

    lastProcessedOplogEntries = 0
    nextReportTime = startTime + appConfig["feedbackSeconds"]

    resumeToken = 'N/A'

    numWorkersCompleted = 0
    numProcessedOplogEntries = 0

    dtDict = {}

    while (numWorkersCompleted < appConfig["numProcessingThreads"]):
        time.sleep(appConfig["feedbackSeconds"])
        nowTime = time.time()

        while not perfQ.empty():
            qMessage = perfQ.get_nowait()
            if qMessage['name'] == "batchCompleted":
                numProcessedOplogEntries += qMessage['operations']
                thisEndDt = qMessage['endts'].as_datetime().replace(tzinfo=None)
                thisProcessNum = qMessage['processNum']
                if (thisProcessNum in dtDict) and (thisEndDt > dtDict[thisProcessNum]):
                    dtDict[thisProcessNum] = thisEndDt
                else:
                    dtDict[thisProcessNum] = thisEndDt
                #print("received endTs = {}".format(thisEndTs.as_datetime()))
                if 'resumeToken' in qMessage:
                    resumeToken = qMessage['resumeToken']
                else:
                    resumeToken = 'N/A'

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

        # how far behind current time
        dtUtcNow = datetime.utcnow()
        totSecondsBehind = 0
        numSecondsBehindEntries = 0
        for thisDt in dtDict:
            totSecondsBehind = (dtUtcNow - dtDict[thisDt].replace(tzinfo=None)).total_seconds()
            numSecondsBehindEntries += 1

        avgSecondsBehind = int(totSecondsBehind / max(numSecondsBehindEntries,1))

        logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
        print("[{0}] elapsed {1} | total o/s {2:12,.2f} | interval o/s {3:12,.2f} | tot {4:16,d} | {5:12,d} secs behind | resume token = {6}".format(logTimeStamp,thisHMS,totalOpsPerSecond,intervalOpsPerSecond,numProcessedOplogEntries,avgSecondsBehind,resumeToken))
        nextReportTime = nowTime + appConfig["feedbackSeconds"]

        lastTime = nowTime
        lastProcessedOplogEntries = numProcessedOplogEntries


def main():
    parser = argparse.ArgumentParser(description='MVU CDC Migrator Tool.')

    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')

    parser.add_argument('--source-uri',
                        required=True,
                        type=str,
                        help='Source URI')

    parser.add_argument('--target-uri',
                        required=False,
                        type=str,
                        default="no-target-uri",
                        help='Target URI you can skip if you run with get-resume-token')

    parser.add_argument('--source-database',
                        required=False,
                        type=str,
                        help='Source database name if you skip it will replicate all the databases')

    parser.add_argument('--source-collection',
                        required=False,
                        type=str,
                        help='Source collection name. Only used if --source-database is defined')

    parser.add_argument('--duration-seconds',
                        required=False,
                        type=int,
                        default=0,
                        help='Number of seconds to run before exiting, 0 = run forever')

    parser.add_argument('--feedback-seconds',
                        required=False,
                        type=int,
                        default=15,
                        help='Number of seconds between feedback output')

    parser.add_argument('--threads',
                        required=False,
                        type=int,
                        default=1,
                        help='Number of threads (parallel processing)')

    parser.add_argument('--max-seconds-between-batches',
                        required=False,
                        type=int,
                        default=5,
                        help='Maximum number of seconds to await full batch')

    parser.add_argument('--max-operations-per-batch',
                        required=False,
                        type=int,
                        default=100,
                        help='Maximum number of operations to include in a single batch')

    parser.add_argument('--dry-run',
                        required=False,
                        action='store_true',
                        help='Read source changes only, do not apply to target')

    parser.add_argument('--start-position',
                        required=True,
                        type=str,
                        help='Starting position - 0 to get change stream resume token, or change stream resume token')

    parser.add_argument('--verbose',
                        required=False,
                        action='store_true',
                        help='Enable verbose logging')

    parser.add_argument('--get-resume-token',
                        required=False,
                        action='store_true',
                        help='Display the current change stream resume token')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)


    appConfig = {}
    appConfig['sourceUri'] = args.source_uri
    appConfig['targetUri'] = args.target_uri
    appConfig['numProcessingThreads'] = args.threads
    appConfig['maxSecondsBetweenBatches'] = args.max_seconds_between_batches
    appConfig['maxOperationsPerBatch'] = args.max_operations_per_batch
    appConfig['durationSeconds'] = args.duration_seconds
    appConfig['feedbackSeconds'] = args.feedback_seconds
    appConfig['dryRun'] = args.dry_run
    appConfig['sourceDb'] = args.source_database
    appConfig['sourceColl'] = args.source_collection
    appConfig['startPosition'] = args.start_position
    appConfig['verboseLogging'] = args.verbose
    appConfig['cdcSource'] = 'changeStream'

    if args.get_resume_token:
        get_resume_token(appConfig)
        sys.exit(0)
    elif (not args.get_resume_token) and args.target_uri =='no-target-uri':
        message = "you need to supply target uri to run it"
        parser.error(message)

    logIt(-1,"processing {} using {} threads".format(appConfig['cdcSource'],appConfig['numProcessingThreads']))

    if len(appConfig["startPosition"]) == 36:
        # resume token
        appConfig["startTs"] = "RESUME_TOKEN"
        logIt(-1,"starting with resume token = {}".format(appConfig["startPosition"]))

    mp.set_start_method('spawn')
    q = mp.Manager().Queue()

    t = threading.Thread(target=reporter,args=(appConfig,q))
    t.start()

    processList = []
    for loop in range(appConfig["numProcessingThreads"]):
        p = mp.Process(target=change_stream_processor,args=(loop,appConfig,q))
        processList.append(p)

    for process in processList:
        process.start()

    for process in processList:
        process.join()

    t.join()


if __name__ == "__main__":
    main()
