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


def logIt(threadnum, message):
    logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
    print("[{}] thread {:>3d} | {}".format(logTimeStamp,threadnum,message))


def oplog_processor(threadnum, appConfig, perfQ):
    if appConfig['verboseLogging']:
        logIt(threadnum,'thread started')

    c = pymongo.MongoClient(appConfig["sourceUri"])
    oplog = c.local.oplog.rs

    destConnection = pymongo.MongoClient(appConfig["targetUri"])
    destDatabase = destConnection[appConfig["targetNs"].split('.',1)[0]]
    destCollection = destDatabase[appConfig["targetNs"].split('.',1)[1]]

    '''
    i  = insert
    u  = update
    d  = delete
    c  = command
    db = database
    n  = no-op
    '''

    startTime = time.time()
    lastFeedback = time.time()
    lastBatch = time.time()

    allDone = False
    threadOplogEntries = 0

    bulkOpList = []
    
    # list with replace, not insert, in case document already exists (replaying old oplog)
    bulkOpListReplace = []
    numCurrentBulkOps = 0
    
    numTotalBatches = 0
        
    printedFirstTs = False
    myCollectionOps = 0

    # starting timestamp
    endTs = appConfig["startTs"]

    while not allDone:
        if appConfig['verboseLogging']:
            logIt(threadnum,"Creating oplog tailing cursor for timestamp {}".format(endTs.as_datetime()))

        cursor = oplog.find({'ts': {'$gte': endTs},'ns':appConfig["sourceNs"]},cursor_type=pymongo.CursorType.TAILABLE_AWAIT,oplog_replay=True)

        while cursor.alive and not allDone:
            for doc in cursor:
                # check if time to exit
                if ((time.time() - startTime) > appConfig['durationSeconds']) and (appConfig['durationSeconds'] != 0):
                    allDone = True
                    break

                endTs = doc['ts']

                # NOTE: Python's non-deterministic hash() cannot be used as it is seeded at startup, since this code is multiprocessing we need all hash calls to be the same between processes
                #   hash(str(doc['o']['_id']))
                if (((doc['op'] in ['i','d']) and (doc['ns'] == appConfig["sourceNs"]) and ((int(hashlib.sha512(str(doc['o']['_id']).encode('utf-8')).hexdigest(), 16) % appConfig["numProcessingThreads"]) == threadnum)) or
                    ((doc['op'] in ['u']) and (doc['ns'] == appConfig["sourceNs"]) and ((int(hashlib.sha512(str(doc['o2']['_id']).encode('utf-8')).hexdigest(), 16) % appConfig["numProcessingThreads"]) == threadnum))):
                    # this is for my thread

                    threadOplogEntries += 1

                    if (not printedFirstTs) and (doc['op'] in ['i','u','d']) and (doc['ns'] == appConfig["sourceNs"]):
                        if appConfig['verboseLogging']:
                            logIt(threadnum,'first timestamp = {} aka {}'.format(doc['ts'],doc['ts'].as_datetime()))
                        printedFirstTs = True

                    if (doc['op'] == 'i'):
                        # insert
                        if (doc['ns'] == appConfig["sourceNs"]):
                            myCollectionOps += 1
                            bulkOpList.append(pymongo.InsertOne(doc['o']))
                            # if playing old oplog, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
                            bulkOpListReplace.append(pymongo.ReplaceOne({'_id':doc['o']['_id']},doc['o'],upsert=True))
                            numCurrentBulkOps += 1
                        else:
                            pass

                    elif (doc['op'] == 'u'):
                        # update
                        if (doc['ns'] == appConfig["sourceNs"]):
                            myCollectionOps += 1
                            # field "$v" is not present in MongoDB 3.4
                            doc['o'].pop('$v',None)
                            bulkOpList.append(pymongo.UpdateOne(doc['o2'],doc['o'],upsert=False))
                            # if playing old oplog, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
                            bulkOpListReplace.append(pymongo.UpdateOne(doc['o2'],doc['o'],upsert=False))
                            numCurrentBulkOps += 1
                        else:
                            pass

                    elif (doc['op'] == 'd'):
                        # delete
                        if (doc['ns'] == appConfig["sourceNs"]):
                            myCollectionOps += 1
                            bulkOpList.append(pymongo.DeleteOne(doc['o']))
                            # if playing old oplog, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
                            bulkOpListReplace.append(pymongo.DeleteOne(doc['o']))
                            numCurrentBulkOps += 1
                        else:
                            pass

                    elif (doc['op'] == 'c'):
                        # command
                        pass

                    elif (doc['op'] == 'n'):
                        # no-op
                        pass

                    else:
                        print(doc)
                        sys.exit(1)

                if ((numCurrentBulkOps >= appConfig["maxOperationsPerBatch"]) or (time.time() >= (lastBatch + appConfig["maxSecondsBetweenBatches"]))) and (numCurrentBulkOps > 0):
                    if not appConfig['dryRun']:
                        try:
                            result = destCollection.bulk_write(bulkOpList,ordered=True)
                        except:
                            # replace inserts as replaces
                            result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
                    perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"endts":endTs,"processNum":threadnum})
                    bulkOpList = []
                    bulkOpListReplace = []
                    numCurrentBulkOps = 0
                    numTotalBatches += 1
                    lastBatch = time.time()

            if ((numCurrentBulkOps >= appConfig["maxOperationsPerBatch"]) or (time.time() >= (lastBatch + appConfig["maxSecondsBetweenBatches"]))) and (numCurrentBulkOps > 0):
                if not appConfig['dryRun']:
                    try:
                        result = destCollection.bulk_write(bulkOpList,ordered=True)
                    except:
                        # replace inserts as replaces
                        result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
                perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"endts":endTs,"processNum":threadnum})
                bulkOpList = []
                bulkOpListReplace = []
                numCurrentBulkOps = 0
                numTotalBatches += 1
                lastBatch = time.time()

            # nothing arrived in the oplog for 1 second, pause before trying again
            time.sleep(1)

    if (numCurrentBulkOps > 0):
        if not appConfig['dryRun']:
            try:
                result = destCollection.bulk_write(bulkOpList,ordered=True)
            except:
                # replace inserts as replaces
                result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
        perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"endts":endTs,"processNum":threadnum})
        bulkOpList = []
        bulkOpListReplace = []
        numCurrentBulkOps = 0
        numTotalBatches += 1

    c.close()

    perfQ.put({"name":"processCompleted","processNum":threadnum})


def change_stream_processor(threadnum, appConfig, perfQ):
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
    lastBatch = time.time()

    allDone = False
    threadOplogEntries = 0

    bulkOpList = []

    # list with replace, not insert, in case document already exists (replaying old oplog)
    bulkOpListReplace = []
    numCurrentBulkOps = 0

    numTotalBatches = 0

    printedFirstTs = False
    myCollectionOps = 0

    # starting timestamp
    endTs = appConfig["startTs"]

    if (appConfig["startTs"] == "RESUME_TOKEN"):
        stream = sourceColl.watch(resume_after={'_data': appConfig["startPosition"]}, full_document='updateLookup', pipeline=[{'$match': {'operationType': {'$in': ['insert','update','replace','delete']}}},{'$project':{'updateDescription':0}}])
    else:
        stream = sourceColl.watch(start_at_operation_time=endTs, full_document='updateLookup', pipeline=[{'$match': {'operationType': {'$in': ['insert','update','replace','delete']}}},{'$project':{'updateDescription':0}}])

    if appConfig['verboseLogging']:
        logIt(threadnum,"Creating change stream cursor for timestamp {}".format(endTs.as_datetime()))

    while not allDone:
        for change in stream:
            # check if time to exit
            if ((time.time() - startTime) > appConfig['durationSeconds']) and (appConfig['durationSeconds'] != 0):
                allDone = True
                break

            endTs = change['clusterTime']
            resumeToken = change['_id']['_data']
            thisNs = change['ns']['db']+'.'+change['ns']['coll']
            thisOp = change['operationType']

            # NOTE: Python's non-deterministic hash() cannot be used as it is seeded at startup, since this code is multiprocessing we need all hash calls to be the same between processes
            #   hash(str(doc['o']['_id']))
            #if ((thisOp in ['insert','update','replace','delete']) and
            #     (thisNs == appConfig["sourceNs"]) and
            if ((int(hashlib.sha512(str(change['documentKey']).encode('utf-8')).hexdigest(), 16) % appConfig["numProcessingThreads"]) == threadnum):
                # this is for my thread

                threadOplogEntries += 1

                if (not printedFirstTs) and (thisOp in ['insert','update','replace','delete']) and (thisNs == appConfig["sourceNs"]):
                    if appConfig['verboseLogging']:
                        logIt(threadnum,'first timestamp = {} aka {}'.format(change['clusterTime'],change['clusterTime'].as_datetime()))
                    printedFirstTs = True

                if (thisOp == 'insert'):
                    # insert
                    if (thisNs == appConfig["sourceNs"]):
                        myCollectionOps += 1
                        bulkOpList.append(pymongo.InsertOne(change['fullDocument']))
                        # if playing old oplog, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
                        #bulkOpListReplace.append(pymongo.ReplaceOne({'_id':change['documentKey']},change['fullDocument'],upsert=True))
                        bulkOpListReplace.append(pymongo.ReplaceOne(change['documentKey'],change['fullDocument'],upsert=True))
                        numCurrentBulkOps += 1
                    else:
                        pass

                elif (thisOp in ['update','replace']):
                    # update/replace
                    if (change['fullDocument'] is not None):
                        if (thisNs == appConfig["sourceNs"]):
                            myCollectionOps += 1
                            #bulkOpList.append(pymongo.ReplaceOne({'_id':change['documentKey']},change['fullDocument'],upsert=True))
                            bulkOpList.append(pymongo.ReplaceOne(change['documentKey'],change['fullDocument'],upsert=True))
                            # if playing old oplog, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
                            #bulkOpListReplace.append(pymongo.ReplaceOne({'_id':change['documentKey']},change['fullDocument'],upsert=True))
                            bulkOpListReplace.append(pymongo.ReplaceOne(change['documentKey'],change['fullDocument'],upsert=True))
                            numCurrentBulkOps += 1
                        else:
                            pass

                elif (thisOp == 'delete'):
                    # delete
                    if (thisNs == appConfig["sourceNs"]):
                        myCollectionOps += 1
                        bulkOpList.append(pymongo.DeleteOne({'_id':change['documentKey']['_id']}))
                        # if playing old oplog, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
                        bulkOpListReplace.append(pymongo.DeleteOne({'_id':change['documentKey']['_id']}))
                        numCurrentBulkOps += 1
                    else:
                        pass

                elif (thisOp in ['drop','rename','dropDatabase','invalidate']):
                    # operations we do not track
                    pass

                else:
                    print(change)
                    sys.exit(1)

            if ((numCurrentBulkOps >= appConfig["maxOperationsPerBatch"]) or (time.time() >= (lastBatch + appConfig["maxSecondsBetweenBatches"]))) and (numCurrentBulkOps > 0):
                if not appConfig['dryRun']:
                    try:
                        result = destCollection.bulk_write(bulkOpList,ordered=True)
                    except:
                        # replace inserts as replaces
                        result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
                perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"endts":endTs,"processNum":threadnum,"resumeToken":resumeToken})
                bulkOpList = []
                bulkOpListReplace = []
                numCurrentBulkOps = 0
                numTotalBatches += 1
                lastBatch = time.time()

            # nothing arrived in the oplog for 1 second, pause before trying again
            #time.sleep(1)

    if (numCurrentBulkOps > 0):
        if not appConfig['dryRun']:
            try:
                result = destCollection.bulk_write(bulkOpList,ordered=True)
            except:
                # replace inserts as replaces
                result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
        perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"endts":endTs,"processNum":threadnum,"resumeToken":resumeToken})
        bulkOpList = []
        bulkOpListReplace = []
        numCurrentBulkOps = 0
        numTotalBatches += 1

    c.close()

    perfQ.put({"name":"processCompleted","processNum":threadnum})


def get_resume_token(appConfig):
    logIt(-1,'getting current change stream resume token')

    sourceConnection = pymongo.MongoClient(appConfig["sourceUri"])
    allDone = False

    stream = sourceConnection.watch()

    while not allDone:
        for change in stream:
            resumeToken = change['_id']['_data']
            logIt(-1,'Change stream resume token is {}'.format(resumeToken))
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
    parser = argparse.ArgumentParser(description='CDC replication tool.')

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
                        
    parser.add_argument('--duration-seconds',
                        required=False,
                        type=int,
                        default=0,
                        help='Number of seconds to run before exiting, 0 = run forever')

    parser.add_argument('--feedback-seconds',
                        required=False,
                        type=int,
                        default=60,
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
                        help='Starting position - 0 for all available changes, YYYY-MM-DD+HH:MM:SS in UTC, or change stream resume token')

    parser.add_argument('--verbose',
                        required=False,
                        action='store_true',
                        help='Enable verbose logging')

    parser.add_argument('--use-oplog',
                        required=False,
                        action='store_true',
                        help='Use the oplog as change data capture source')

    parser.add_argument('--use-change-stream',
                        required=False,
                        action='store_true',
                        help='Use change streams as change data capture source')

    parser.add_argument('--get-resume-token',
                        required=False,
                        action='store_true',
                        help='Display the current change stream resume token')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    if (not args.use_oplog) and (not args.use_change_stream):
        message = "Must supply either --use-oplog or --use-change-stream"
        parser.error(message)

    if (args.use_oplog) and (args.use_change_stream):
        message = "Cannot supply both --use-oplog or --use-change-stream"
        parser.error(message)

    if (args.use_change_stream) and (args.start_position == "0"):
        message = "--start-position must be supplied as YYYY-MM-DD+HH:MM:SS in UTC or resume token when executing in --use-change-stream mode"
        parser.error(message)

    appConfig = {}
    appConfig['sourceUri'] = args.source_uri
    appConfig['targetUri'] = args.target_uri
    appConfig['numProcessingThreads'] = args.threads
    appConfig['maxSecondsBetweenBatches'] = args.max_seconds_between_batches
    appConfig['maxOperationsPerBatch'] = args.max_operations_per_batch
    appConfig['durationSeconds'] = args.duration_seconds
    appConfig['feedbackSeconds'] = args.feedback_seconds
    appConfig['dryRun'] = args.dry_run
    appConfig['sourceNs'] = args.source_namespace
    if not args.target_namespace:
        appConfig['targetNs'] = args.source_namespace
    else:
        appConfig['targetNs'] = args.target_namespace
    appConfig['startPosition'] = args.start_position
    appConfig['verboseLogging'] = args.verbose

    if args.get_resume_token:
        get_resume_token(appConfig)
        sys.exit(0)
    
    if args.use_oplog:
        appConfig['cdcSource'] = 'oplog'
    else:
        appConfig['cdcSource'] = 'changeStream'

    logIt(-1,"processing {} using {} threads".format(appConfig['cdcSource'],appConfig['numProcessingThreads']))

    if len(appConfig["startPosition"]) == 36:
        # resume token
        appConfig["startTs"] = "RESUME_TOKEN"

        logIt(-1,"starting with resume token = {}".format(appConfig["startPosition"]))

    else:
        if appConfig["startPosition"] == "0":
            # start with first oplog entry
            c = pymongo.MongoClient(appConfig["sourceUri"])
            oplog = c.local.oplog.rs
            first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(1).next()
            appConfig["startTs"] = first['ts']
            c.close()
        elif appConfig["startPosition"].upper() == "NOW":
            # start with current time
            appConfig["startTs"] = Timestamp(datetime.utcnow(), 1)
        else:
            # start at an arbitrary position
            appConfig["startTs"] = Timestamp(datetime.fromisoformat(args.start_position), 1)

        logIt(-1,"starting with timestamp = {}".format(appConfig["startTs"].as_datetime()))

    mp.set_start_method('spawn')
    q = mp.Queue()

    t = threading.Thread(target=reporter,args=(appConfig,q))
    t.start()
    
    processList = []
    for loop in range(appConfig["numProcessingThreads"]):
        if (appConfig['cdcSource'] == 'oplog'):
            p = mp.Process(target=oplog_processor,args=(loop,appConfig,q))
        else:
            p = mp.Process(target=change_stream_processor,args=(loop,appConfig,q))
        processList.append(p)
        
    for process in processList:
        process.start()
        
    for process in processList:
        process.join()
        
    t.join()


if __name__ == "__main__":
    main()
