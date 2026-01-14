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
import boto3
import warnings
from bson import encode


def logIt(threadnum, message):
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
    print("[{}] thread {:>3d} | {}".format(logTimeStamp,threadnum,message))


def getCollectionCount(appConfig):
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    sourceDb = appConfig["sourceNs"].split('.',1)[0]
    sourceColl = appConfig["sourceNs"].split('.',1)[1]
    client = pymongo.MongoClient(appConfig['sourceUri'])
    db = client[sourceDb]
    collStats = db.command("collStats", sourceColl)
    client.close()
    return max(collStats['count'],1)


def full_load_loader(threadnum, appConfig, perfQ):
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    if appConfig['verboseLogging']:
        logIt(threadnum,'thread started')

    sourceConnection = pymongo.MongoClient(host=appConfig["sourceUri"],appname='migrfull')
    sourceDb = sourceConnection[appConfig["sourceNs"].split('.',1)[0]]
    sourceColl = sourceDb[appConfig["sourceNs"].split('.',1)[1]]

    destConnection = pymongo.MongoClient(host=appConfig["targetUri"],appname='migrfull')
    destDatabase = destConnection[appConfig["targetNs"].split('.',1)[0]]
    destCollection = destDatabase[appConfig["targetNs"].split('.',1)[1]]

    startTime = time.time()
    lastFeedback = time.time()

    bulkOpList = []

    # list with replace, not insert, in case document already exists (replaying old oplog)
    bulkOpListReplace = []
    numCurrentBulkOps = 0
    numCurrentBytes = 0

    numTotalBatches = 0

    myCollectionOps = 0

    if appConfig['verboseLogging']:
        logIt(threadnum,"Creating cursor")

    if (threadnum == 0):
        # thread 0 = $lte only
        cursor = sourceColl.find({'_id': {'$lte': appConfig['boundaries'][threadnum]}})
    elif (threadnum == appConfig['numProcessingThreads'] - 1):
        # last processor = $gt only
        cursor = sourceColl.find({'_id': {'$gt': appConfig['boundaries'][threadnum-1]}})
    else:
        # last processor = $gt prior, $lte next
        cursor = sourceColl.find({'_id': {'$gt': appConfig['boundaries'][threadnum-1], '$lte': appConfig['boundaries'][threadnum]}})

    perfQ.put({"name":"findCompleted","processNum":threadnum})

    for doc in cursor:
        myCollectionOps += 1
        numCurrentBytes += len(encode(doc))
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
            perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"processNum":threadnum,"bytes":numCurrentBytes})
            bulkOpList = []
            bulkOpListReplace = []
            numCurrentBulkOps = 0
            numCurrentBytes = 0
            numTotalBatches += 1

    if (numCurrentBulkOps > 0):
        if not appConfig['dryRun']:
        #    try:
            result = destCollection.bulk_write(bulkOpList,ordered=True)
        #    except:
        #    # replace inserts as replaces
        #        result = destCollection.bulk_write(bulkOpListReplace,ordered=True)
        perfQ.put({"name":"batchCompleted","operations":numCurrentBulkOps,"processNum":threadnum,"bytes":numCurrentBytes})
        bulkOpList = []
        bulkOpListReplace = []
        numCurrentBulkOps = 0
        numTotalBatches += 1

    perfQ.put({"name":"processCompleted","processNum":threadnum})


def reporter(appConfig, perfQ):
    createCloudwatchMetrics = appConfig['createCloudwatchMetrics']
    numDocumentsToMigrate = appConfig['numDocumentsToMigrate']
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
    
    numWorkersCompleted = 0
    numWorkersLoading = 0
    numProcessedOplogEntries = 0
    
    while (numWorkersCompleted < appConfig["numProcessingThreads"]):
        time.sleep(appConfig["feedbackSeconds"])
        nowTime = time.time()
        numThisBytes = 0
        
        while not perfQ.empty():
            qMessage = perfQ.get_nowait()
            if qMessage['name'] == "batchCompleted":
                numProcessedOplogEntries += qMessage['operations']
                numThisBytes += qMessage['bytes']
            elif qMessage['name'] == "processCompleted":
                numWorkersCompleted += 1
                numWorkersLoading -= 1
            elif qMessage['name'] == "findCompleted":
                numWorkersLoading += 1

        # total total
        elapsedSeconds = nowTime - startTime
        totalOpsPerSecond = numProcessedOplogEntries / elapsedSeconds

        # estimated time to done
        if numProcessedOplogEntries > 0:
            pctDone = max(numProcessedOplogEntries / numDocumentsToMigrate,0.001)
            remainingSeconds = max(int(elapsedSeconds / pctDone) - elapsedSeconds,0)
        else:
            remainingSeconds = 0

        thisHours, rem = divmod(remainingSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        remainHMS = "{:0>2}:{:0>2}:{:0>2}".format(int(thisHours),int(thisMinutes),int(thisSeconds))

        if (numDocumentsToMigrate == 0):
            pctDone = 100.0
        else:
            pctDone = (numProcessedOplogEntries / numDocumentsToMigrate) * 100.0

        # elapsed hours, minutes, seconds
        thisHours, rem = divmod(elapsedSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)
        
        # this interval
        intervalElapsedSeconds = nowTime - lastTime
        intervalOpsPerSecond = (numProcessedOplogEntries - lastProcessedOplogEntries) / intervalElapsedSeconds

        numThisGbPerHour = numThisBytes / intervalElapsedSeconds * 60 * 60 / 1024 / 1024 / 1024

        logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
        print("[{0}] elapsed {1} | total o/s {2:12,.2f} | interval o/s {3:12,.2f} | tot ops {4:16,d} | loading {5:5d} | pct {6:6.2f}% | done in {7} | GB/hr {8:6.2f}".format(logTimeStamp,thisHMS,totalOpsPerSecond,intervalOpsPerSecond,numProcessedOplogEntries,numWorkersLoading,pctDone,remainHMS,numThisGbPerHour))
        nextReportTime = nowTime + appConfig["feedbackSeconds"]
        
        lastTime = nowTime
        lastProcessedOplogEntries = numProcessedOplogEntries

        # output CW metrics every cloudwatchPutSeconds seconds
        if createCloudwatchMetrics and ((time.time() - lastCloudwatchPutTime) > cloudwatchPutSeconds):
            # log to cloudwatch
            cloudWatchClient.put_metric_data(
                Namespace='CustomDocDB',
                MetricData=[{'MetricName':'MigratorFLInsertsPerSecond','Dimensions':[{'Name':'Cluster','Value':clusterName}],'Value':intervalOpsPerSecond,'StorageResolution':60},
                            {'MetricName':'MigratorFLRemainingSeconds','Dimensions':[{'Name':'Cluster','Value':clusterName}],'Value':remainingSeconds,'StorageResolution':60}])

            lastCloudwatchPutTime = time.time()


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

    parser.add_argument('--boundary-datatype',
                        required=False,
                        type=str,
                        default='objectid',
                        choices=['objectid','string','int'],
                        help='Boundaries for segmenting')

    parser.add_argument('--create-cloudwatch-metrics',required=False,action='store_true',help='Create CloudWatch metrics when garbage collection is active')
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
    appConfig['maxInsertsPerBatch'] = args.max_inserts_per_batch
    appConfig['feedbackSeconds'] = args.feedback_seconds
    appConfig['dryRun'] = args.dry_run
    appConfig['sourceNs'] = args.source_namespace
    if not args.target_namespace:
        appConfig['targetNs'] = args.source_namespace
    else:
        appConfig['targetNs'] = args.target_namespace
    appConfig['verboseLogging'] = args.verbose
    appConfig['boundaryDatatype'] = args.boundary_datatype
    appConfig['createCloudwatchMetrics'] = args.create_cloudwatch_metrics
    appConfig['clusterName'] = args.cluster_name

    boundaryList = args.boundaries.split(',')
    appConfig['boundaries'] = []
    for thisBoundary in boundaryList:
        if appConfig['boundaryDatatype'] == 'objectid':
            appConfig['boundaries'].append(ObjectId(thisBoundary))
        elif appConfig['boundaryDatatype'] == 'string':
            appConfig['boundaries'].append(thisBoundary)
        else:
            appConfig['boundaries'].append(int(thisBoundary))

    appConfig['numProcessingThreads'] = len(appConfig['boundaries'])+1
    appConfig['numDocumentsToMigrate'] = getCollectionCount(appConfig)
    
    logIt(-1,"processing using {} threads".format(appConfig['numProcessingThreads']))

    mp.set_start_method('spawn')
    q = mp.Manager().Queue()

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
