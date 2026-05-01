import datetime as dt
import os
import sys
import time
import pymongo
from bson.timestamp import Timestamp
import threading
import multiprocessing as mp
import hashlib
import argparse
import warnings


def logIt(threadnum, message):
    logTimeStamp = dt.datetime.now(dt.timezone.utc).isoformat()[:-3] + 'Z'
    print("[{}] thread {:>3d} | {}".format(logTimeStamp,threadnum,message))


def change_stream_processor(threadnum, appConfig, perfQ):
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    # starting timestamp
    endTs = appConfig["startTs"]

    # full document lookup for updates
    updateLookup = None
    if appConfig['updateLookup']:
        updateLookup = 'updateLookup'

    sourceConnection = pymongo.MongoClient(host=appConfig["sourceUri"],appname='cdcperf')
    if appConfig["sourceNs"] is None:
        stream = sourceConnection.watch(start_at_operation_time=endTs, max_await_time_ms=1000, full_document=updateLookup, pipeline=[{'$match': {'operationType': {'$in': ['insert','update','replace','delete']}}},{'$project':{'updateDescription':0}}])
        logIt(threadnum,"Watching change stream for all collections at timestamp {}".format(endTs.as_datetime()))
    else:
        sourceDb = sourceConnection[appConfig["sourceNs"].split('.',1)[0]]
        if '.' in appConfig['sourceNs']:
            sourceColl = sourceDb[appConfig["sourceNs"].split('.',1)[1]]
            stream = sourceColl.watch(start_at_operation_time=endTs, max_await_time_ms=1000, full_document=updateLookup, pipeline=[{'$match': {'operationType': {'$in': ['insert','update','replace','delete']}}},{'$project':{'updateDescription':0}}])
            logIt(threadnum,"Watching change stream for collection {} at timestamp {}".format(appConfig["sourceNs"], endTs.as_datetime()))
        else:
            stream = sourceDb.watch(start_at_operation_time=endTs, max_await_time_ms=1000, full_document=updateLookup, pipeline=[{'$match': {'operationType': {'$in': ['insert','update','replace','delete']}}},{'$project':{'updateDescription':0}}])
            logIt(threadnum,"Watching change stream for database {} at timestamp {}".format(appConfig["sourceNs"], endTs.as_datetime()))

    startTime = time.time()
    lastFeedback = time.time()

    allDone = False
    perfReportInterval = 1
    nextPerfReportTime = time.time() + perfReportInterval

    numReportBulkOps = 0

    printedFirstTs = False
    myCollectionOps = 0
    resumeToken = 'N/A'

    fetchMs = 0.0
    maxFetchMs = -1.0
    totalFetchMs = 0.0
    totalFetchCount = 0

    while not allDone:
        #for change in stream:
        fetchStartTime = time.time()
        change = stream.try_next()
        fetchMs = (time.time() - fetchStartTime) * 1000

        if change is not None:
            totalFetchMs += fetchMs
            totalFetchCount += 1
            if fetchMs > maxFetchMs:
                maxFetchMs = fetchMs

            # check if time to exit
            if ((time.time() - startTime) > appConfig['durationSeconds']) and (appConfig['durationSeconds'] != 0):
                allDone = True
                break

            endTs = change['clusterTime']
            resumeToken = change['_id']['_data']
            #thisNs = change['ns']['db']+'.'+change['ns']['coll']
            thisOp = change['operationType']

            if (not printedFirstTs):
                logIt(threadnum,'first timestamp = {} aka {}'.format(change['clusterTime'],change['clusterTime'].as_datetime()))
                printedFirstTs = True

            if (thisOp == 'insert'):
                # insert
                myCollectionOps += 1

            elif (thisOp in ['update','replace']):
                # update/replace
                myCollectionOps += 1

            elif (thisOp == 'delete'):
                # delete
                myCollectionOps += 1

            elif (thisOp in ['drop','rename','dropDatabase','invalidate']):
                # operations we do not track
                pass

            else:
                print(change)
                sys.exit(1)

        if time.time() > nextPerfReportTime:
            #print("OPS|{}".format(myCollectionOps))
            nextPerfReportTime = time.time() + perfReportInterval
            avgFetchMs = -1.0
            if totalFetchCount > 0:
               avgFetchMs = totalFetchMs / totalFetchCount
            perfQ.put({"name":"batchCompleted","operations":myCollectionOps,"endts":endTs,"processNum":threadnum,"resumeToken":resumeToken,"maxFetchMs":maxFetchMs,"avgFetchMs":avgFetchMs})
            myCollectionOps = 0
            maxFetchMs = -1.0
            totalFetchMs = 0.0
            totalFetchCount = 0

    if (numCurrentBulkOps > 0):
        perfQ.put({"name":"batchCompleted","operations":myCollectionOps,"endts":endTs,"processNum":threadnum,"resumeToken":resumeToken})
        myCollectionOps = 0

    sourceConnection.close()

    perfQ.put({"name":"processCompleted","processNum":threadnum})


def reporter(appConfig, perfQ):
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
        
        numBatchEntries = 0
        maxFetchMs = -1.0
        avgFetchMs = 0.0
        numAvgFetchMs = 0
        while not perfQ.empty():
            qMessage = perfQ.get_nowait()
            #print("{}".format(qMessage))
            if qMessage['name'] == "batchCompleted":
                numBatchEntries += 1
                numProcessedOplogEntries += qMessage['operations']
                thisEndDt = qMessage['endts'].as_datetime().replace(tzinfo=None)
                thisProcessNum = qMessage['processNum']
                if (thisProcessNum in dtDict) and (thisEndDt > dtDict[thisProcessNum]):
                    dtDict[thisProcessNum] = thisEndDt
                else:
                    dtDict[thisProcessNum] = thisEndDt
                if 'resumeToken' in qMessage:
                    resumeToken = qMessage['resumeToken']
                else:
                    resumeToken = 'N/A'
                if qMessage['maxFetchMs'] > maxFetchMs:
                    maxFetchMs = qMessage['maxFetchMs']
                numAvgFetchMs += 1
                avgFetchMs += qMessage['avgFetchMs']

            elif qMessage['name'] == "processCompleted":
                numWorkersCompleted += 1

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
        
        # how far behind current time
        if numBatchEntries == 0:
            # no work this interval, we are fully caught up
            avgSecondsBehind = 0
        else:
            dtUtcNow = dt.datetime.now(dt.timezone.utc)
            totSecondsBehind = 0
            numSecondsBehindEntries = 0
            for thisDt in dtDict:
                totSecondsBehind += (dtUtcNow - dtDict[thisDt].replace(tzinfo=dt.timezone.utc)).total_seconds()
                numSecondsBehindEntries += 1

            avgSecondsBehind = int(totSecondsBehind / max(numSecondsBehindEntries,1))

        if numAvgFetchMs > 0:
            thisAvgFetchMs = avgFetchMs / numAvgFetchMs
        else:
            thisAvgFetchMs = -1.0

        logTimeStamp = dt.datetime.now(dt.timezone.utc).isoformat()[:-3] + 'Z'
        print("[{0}] elapsed {1} | total o/s {2:9,d} | interval o/s {3:9,d} | tot {4:16,d} | {5:12,d} secs behind | {6:8,.2f}  max fetch ms | {7:8,.2f} avg fetch ms".format(logTimeStamp,thisHMS,totalOpsPerSecond,intervalOpsPerSecond,numProcessedOplogEntries,avgSecondsBehind,maxFetchMs,thisAvgFetchMs))
        nextReportTime = nowTime + appConfig["feedbackSeconds"]
        
        lastTime = nowTime
        lastProcessedOplogEntries = numProcessedOplogEntries


def main():
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    parser = argparse.ArgumentParser(description='CDC retrieval throughput test.')

    parser.add_argument('--skip-python-version-check', required=False, action='store_true', help='Permit execution on Python 3.6 and prior')
    parser.add_argument('--source-uri', required=True, type=str, help='Source URI')
    parser.add_argument('--source-namespace', required=False, type=str, default=None, help='Source Namespace as <database>.<collection>, <database>, defaults to all changes.')
    parser.add_argument('--duration-seconds', required=False, type=int, default=0, help='Number of seconds to run before exiting, 0 = run forever')
    parser.add_argument('--feedback-seconds', required=False, type=int, default=5, help='Number of seconds between feedback output')
    parser.add_argument('--start-position', required=False, type=str, default='NOW', help='Starting position - "NOW" or YYYY-MM-DD+HH:MM:SS in UTC')
    parser.add_argument('--update-lookup', required=False, action='store_true', help='Perform full document lookup for updates')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['sourceUri'] = args.source_uri
    appConfig['durationSeconds'] = args.duration_seconds
    appConfig['feedbackSeconds'] = args.feedback_seconds
    appConfig['sourceNs'] = args.source_namespace
    appConfig['startPosition'] = args.start_position
    appConfig['numProcessingThreads'] = 1
    appConfig['updateLookup'] = args.update_lookup

    if appConfig["startPosition"].upper() == "NOW":
        # start with current time
        appConfig["startTs"] = Timestamp(dt.datetime.now(dt.timezone.utc), 1)
    else:
        # start at an arbitrary position
        appConfig["startTs"] = Timestamp(dt.datetime.fromisoformat(args.start_position), 1)

    mp.set_start_method('spawn')
    q = mp.Manager().Queue()

    t = threading.Thread(target=reporter,args=(appConfig,q))
    t.start()
    
    processList = []
    p = mp.Process(target=change_stream_processor,args=(0,appConfig,q))
    processList.append(p)
        
    for process in processList:
        process.start()
        
    for process in processList:
        process.join()
        
    t.join()


if __name__ == "__main__":
    main()
