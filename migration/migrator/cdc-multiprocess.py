from datetime import datetime, timedelta, timezone
import os
import sys
import time
import pymongo
from bson.timestamp import Timestamp
import threading
import multiprocessing as mp
import hashlib
import argparse
import boto3
import warnings


def logIt(threadnum, message):
    logTimeStamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    print("[{}] thread {:>3d} | {}".format(logTimeStamp,threadnum,message))


def change_stream_processor(threadnum, appConfig, perfQ):
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    if appConfig['verboseLogging']:
        logIt(threadnum,'thread started')

    myAppname = None
    if (threadnum == 0):
        myAppname = 'migrcdc'

    sourceConnection = pymongo.MongoClient(host=appConfig["sourceUri"],appname=myAppname)
    sourceDb = sourceConnection[appConfig["sourceNs"].split('.',1)[0]]
    sourceColl = sourceDb[appConfig["sourceNs"].split('.',1)[1]]

    destConnection = pymongo.MongoClient(host=appConfig["targetUri"],appname=myAppname)
    destDatabase = destConnection[appConfig["targetNs"].split('.',1)[0]]
    destCollection = destDatabase[appConfig["targetNs"].split('.',1)[1]]

    startTime = time.time()

    allDone = False
    perfReportInterval = 1
    nextPerfReportTime = time.time() + perfReportInterval

    bulkOpList = []

    # list with replace, not insert, in case document already exists (replaying older changes)
    bulkOpListReplace = []
    numCurrentBulkOps = 0
    numReportBulkOps = 0

    # time the oldest operation in the current batch was buffered, None when the batch is empty
    batchStartTime = None

    printedFirstTs = False

    # read position: cluster time of the most recent change seen, used for "secs behind"
    lastSeenTs = None
    lastSeenToken = None

    # every processor reads the entire stream in the same order, numSeen is the position within it
    numSeen = 0

    # durable position: every change up to this point that belongs to this processor has been written to the target
    #   resuming from durableToken cannot skip any of this processor's changes
    durableToken = None
    durableSeq = -1

    # True when the stream has no more changes available and nothing is buffered
    caughtUp = False

    # bound the server-side wait for new changes so idle streams return control to flush batches and check duration
    maxAwaitTimeMs = max(100, min(1000, appConfig["maxSecondsBetweenBatches"] * 1000))

    changeStreamPipeline = [{'$match': {'operationType': {'$in': ['insert','update','replace','delete']}}},{'$project':{'updateDescription':0}}]

    if (appConfig["startTs"] == "RESUME_TOKEN"):
        stream = sourceColl.watch(resume_after={'_data': appConfig["startPosition"]}, full_document='updateLookup', max_await_time_ms=maxAwaitTimeMs, pipeline=changeStreamPipeline)
    else:
        stream = sourceColl.watch(start_at_operation_time=appConfig["startTs"], full_document='updateLookup', max_await_time_ms=maxAwaitTimeMs, pipeline=changeStreamPipeline)

    if appConfig['verboseLogging']:
        if (appConfig["startTs"] == "RESUME_TOKEN"):
            logIt(threadnum,"Creating change stream cursor for resume token {}".format(appConfig["startPosition"]))
        else:
            logIt(threadnum,"Creating change stream cursor for timestamp {}".format(appConfig["startTs"].as_datetime()))

    def flushBatch():
        nonlocal bulkOpList, bulkOpListReplace, numCurrentBulkOps, numReportBulkOps, batchStartTime, durableToken, durableSeq
        if not appConfig['dryRun']:
            try:
                result = destCollection.bulk_write(bulkOpList,ordered=True)
            except:
                # replace inserts as replaces
                result = destCollection.bulk_write(bulkOpListReplace,ordered=True)

        bulkOpList = []
        bulkOpListReplace = []
        numReportBulkOps += numCurrentBulkOps
        numCurrentBulkOps = 0
        batchStartTime = None

        # everything seen so far has now been applied
        durableToken = lastSeenToken
        durableSeq = numSeen

    def reportProgress():
        nonlocal numReportBulkOps
        perfQ.put({"name":"batchCompleted","operations":numReportBulkOps,"endts":lastSeenTs,"processNum":threadnum,
                   "resumeToken":durableToken,"resumeSeq":durableSeq,"caughtUp":caughtUp})
        numReportBulkOps = 0

    while not allDone:
        # returns the next change, or None once the server-side wait (maxAwaitTimeMs) expires with no new changes
        change = stream.try_next()

        if change is not None:
            caughtUp = False
            numSeen += 1
            lastSeenTs = change['clusterTime']
            lastSeenToken = change['_id']['_data']
            thisNs = change['ns']['db']+'.'+change['ns']['coll']
            thisOp = change['operationType']

            # NOTE: Python's non-deterministic hash() cannot be used as it is seeded at startup, since this code is multiprocessing we need all hash calls to be the same between processes
            if ((int(hashlib.sha512(str(change['documentKey']).encode('utf-8')).hexdigest(), 16) % appConfig["numProcessingThreads"]) == threadnum):
                # this is for my thread

                if (not printedFirstTs) and (thisNs == appConfig["sourceNs"]):
                    if appConfig['verboseLogging']:
                        logIt(threadnum,'first timestamp = {} aka {}'.format(change['clusterTime'],change['clusterTime'].as_datetime()))
                    printedFirstTs = True

                if (thisOp == 'insert'):
                    # insert
                    if (thisNs == appConfig["sourceNs"]):
                        bulkOpList.append(pymongo.InsertOne(change['fullDocument']))
                        # if replaying older changes, need to change inserts to be replaces (the inserts will fail due to _id uniqueness)
                        bulkOpListReplace.append(pymongo.ReplaceOne(change['documentKey'],change['fullDocument'],upsert=True))
                        numCurrentBulkOps += 1

                elif (thisOp in ['update','replace']):
                    # update/replace
                    if (change['fullDocument'] is not None) and (thisNs == appConfig["sourceNs"]):
                        bulkOpList.append(pymongo.ReplaceOne(change['documentKey'],change['fullDocument'],upsert=True))
                        bulkOpListReplace.append(pymongo.ReplaceOne(change['documentKey'],change['fullDocument'],upsert=True))
                        numCurrentBulkOps += 1

                elif (thisOp == 'delete'):
                    # delete
                    if (thisNs == appConfig["sourceNs"]):
                        bulkOpList.append(pymongo.DeleteOne({'_id':change['documentKey']['_id']}))
                        bulkOpListReplace.append(pymongo.DeleteOne({'_id':change['documentKey']['_id']}))
                        numCurrentBulkOps += 1

                elif (thisOp in ['drop','rename','dropDatabase','invalidate']):
                    # operations we do not track
                    pass

                else:
                    print(change)
                    sys.exit(1)

            if (numCurrentBulkOps == 0):
                # nothing buffered, every change seen so far is applied
                durableToken = lastSeenToken
                durableSeq = numSeen
            elif (batchStartTime is None):
                # first operation in a new batch, batch age is measured from here
                batchStartTime = time.time()

        else:
            # no changes available right now
            if (numCurrentBulkOps == 0):
                caughtUp = True
                # stream.resume_token is the post-batch resume token when the server provides one, it can advance while idle
                if (stream.resume_token is not None):
                    durableToken = stream.resume_token['_data']
                    # sorts after the last change seen, before the next one
                    durableSeq = numSeen + 0.5

        # these checks run on every pass, including when no change arrived

        if ((time.time() - startTime) > appConfig['durationSeconds']) and (appConfig['durationSeconds'] != 0):
            allDone = True

        if (numCurrentBulkOps > 0) and ((numCurrentBulkOps >= appConfig["maxOperationsPerBatch"]) or ((time.time() - batchStartTime) >= appConfig["maxSecondsBetweenBatches"])):
            flushBatch()

        if time.time() > nextPerfReportTime:
            nextPerfReportTime = time.time() + perfReportInterval
            reportProgress()

    if (numCurrentBulkOps > 0):
        flushBatch()

    stream.close()
    sourceConnection.close()
    destConnection.close()

    reportProgress()
    perfQ.put({"name":"processCompleted","processNum":threadnum})


def get_resume_token(appConfig):
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    logIt(-1,'getting current change stream resume token')

    sourceConnection = pymongo.MongoClient(host=appConfig["sourceUri"])
    sourceDb = sourceConnection[appConfig["sourceNs"].split('.',1)[0]]
    sourceColl = sourceDb[appConfig["sourceNs"].split('.',1)[1]]

    allDone = False

    stream = sourceColl.watch()

    while not allDone:
        for change in stream:
            resumeToken = change['_id']['_data']
            logIt(-1,'Change stream resume token is {}'.format(resumeToken))
            allDone = True
            break


def reporter(appConfig, perfQ):
    createCloudwatchMetrics = appConfig['createCloudwatchMetrics']
    clusterName = appConfig['clusterName']

    if appConfig['verboseLogging']:
        logIt(-1,'reporting thread started')

    if createCloudwatchMetrics:
        # only instantiate client if needed
        cloudWatchClient = boto3.client('cloudwatch')

    startTime = time.time()
    lastTime = time.time()
    
    # number of seconds between posting metrics to cloudwatch
    cloudwatchPutSeconds = 30
    lastCloudwatchPutTime = time.time()

    lastProcessedOplogEntries = 0
    nextReportTime = startTime + appConfig["feedbackSeconds"]

    resumeToken = 'N/A'

    numWorkersCompleted = 0
    numProcessedOplogEntries = 0

    # latest state reported by each processor
    dtDict = {}          # cluster time of the most recent change read
    caughtUpDict = {}    # True if the processor has no pending changes
    durableDict = {}     # (stream position, resume token) up to which all of the processor's changes are applied

    while (numWorkersCompleted < appConfig["numProcessingThreads"]):
        time.sleep(appConfig["feedbackSeconds"])
        nowTime = time.time()

        while not perfQ.empty():
            qMessage = perfQ.get_nowait()
            if qMessage['name'] == "batchCompleted":
                numProcessedOplogEntries += qMessage['operations']
                thisProcessNum = qMessage['processNum']
                if qMessage['endts'] is not None:
                    dtDict[thisProcessNum] = qMessage['endts'].as_datetime()
                caughtUpDict[thisProcessNum] = qMessage['caughtUp']
                if qMessage['resumeToken'] is not None:
                    durableDict[thisProcessNum] = (qMessage['resumeSeq'],qMessage['resumeToken'])

            elif qMessage['name'] == "processCompleted":
                numWorkersCompleted += 1

        # every processor reads the entire stream, so the only safe place to resume from is the oldest durable position across all processors
        if len(durableDict) == appConfig["numProcessingThreads"]:
            resumeToken = min(durableDict.values(), key=lambda x: x[0])[1]
        else:
            resumeToken = 'N/A'

        # total total
        elapsedSeconds = nowTime - startTime
        totalOpsPerSecond = int(numProcessedOplogEntries / elapsedSeconds)

        # elapsed hours, minutes, seconds
        thisHours, rem = divmod(elapsedSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)
        
        # this interval
        intervalElapsedSeconds = nowTime - lastTime
        intervalOpsPerSecond = int((numProcessedOplogEntries - lastProcessedOplogEntries) / intervalElapsedSeconds)
        
        # how far behind current time, processors that are caught up are 0 seconds behind
        dtUtcNow = datetime.now(timezone.utc)
        totSecondsBehind = 0
        numSecondsBehindEntries = 0
        for thisProcessNum in caughtUpDict:
            if (not caughtUpDict[thisProcessNum]) and (thisProcessNum in dtDict):
                totSecondsBehind += max((dtUtcNow - dtDict[thisProcessNum]).total_seconds(),0)
            numSecondsBehindEntries += 1

        avgSecondsBehind = int(totSecondsBehind / max(numSecondsBehindEntries,1))

        logTimeStamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        print("[{0}] elapsed {1} | total o/s {2:9,d} | interval o/s {3:9,d} | tot {4:16,d} | {5:12,d} secs behind | resume token = {6}".format(logTimeStamp,thisHMS,totalOpsPerSecond,intervalOpsPerSecond,numProcessedOplogEntries,avgSecondsBehind,resumeToken))
        nextReportTime = nowTime + appConfig["feedbackSeconds"]
        
        lastTime = nowTime
        lastProcessedOplogEntries = numProcessedOplogEntries

        # output CW metrics every cloudwatchPutSeconds seconds
        if createCloudwatchMetrics and ((time.time() - lastCloudwatchPutTime) > cloudwatchPutSeconds):
            # log to cloudwatch
            cloudWatchClient.put_metric_data(
                Namespace='CustomDocDB',
                MetricData=[{'MetricName':'MigratorCDCOperationsPerSecond','Dimensions':[{'Name':'Cluster','Value':clusterName}],'Value':intervalOpsPerSecond,'StorageResolution':60},
                            {'MetricName':'MigratorCDCNumSecondsBehind','Dimensions':[{'Name':'Cluster','Value':clusterName}],'Value':avgSecondsBehind,'StorageResolution':60}])

            lastCloudwatchPutTime = time.time()


def main():
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

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
                        help='Starting position - NOW, YYYY-MM-DD+HH:MM:SS in UTC or change stream resume token')

    parser.add_argument('--verbose',
                        required=False,
                        action='store_true',
                        help='Enable verbose logging')

    parser.add_argument('--use-change-stream',
                        required=True,
                        action='store_true',
                        help='Use change streams as change data capture source')

    parser.add_argument('--get-resume-token',
                        required=False,
                        action='store_true',
                        help='Display the current change stream resume token')

    parser.add_argument('--create-cloudwatch-metrics',required=False,action='store_true',help='Create CloudWatch metrics')
    parser.add_argument('--cluster-name',required=False,type=str,help='Name of cluster for CloudWatch metrics')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    if args.create_cloudwatch_metrics and (args.cluster_name is None):
        sys.exit("\nMust supply --cluster-name when capturing CloudWatch metrics.\n")

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
    appConfig['createCloudwatchMetrics'] = args.create_cloudwatch_metrics
    appConfig['clusterName'] = args.cluster_name

    if args.get_resume_token:
        get_resume_token(appConfig)
        sys.exit(0)
    
    appConfig['cdcSource'] = 'changeStream'

    logIt(-1,"processing {} using {} threads".format(appConfig['cdcSource'],appConfig['numProcessingThreads']))

    startPosition = appConfig["startPosition"]
    if (len(startPosition) >= 20) and all(c in '0123456789abcdefABCDEF' for c in startPosition):
        # resume token, a hex string whose length varies by server (MongoDB and DocumentDB tokens differ)
        appConfig["startTs"] = "RESUME_TOKEN"

        logIt(-1,"starting with resume token = {}".format(startPosition))

    else:
        if startPosition.upper() == "NOW":
            # start with current time
            appConfig["startTs"] = Timestamp(datetime.now(timezone.utc), 1)
        else:
            # start at an arbitrary position
            try:
                appConfig["startTs"] = Timestamp(datetime.fromisoformat(startPosition), 1)
            except ValueError:
                parser.error("--start-position must be NOW, YYYY-MM-DD+HH:MM:SS in UTC, or a change stream resume token")

        logIt(-1,"starting with timestamp = {}".format(appConfig["startTs"].as_datetime()))

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
