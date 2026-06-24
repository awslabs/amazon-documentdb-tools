from datetime import datetime, timezone
import sys
import time
import pymongo
from pymongo.errors import BulkWriteError
import threading
import multiprocessing as mp
import hashlib
import argparse
from collections import defaultdict

def _execute_batch(destCollection, bulkOpList, bulkOpListReplace):
    """Execute a batch with ordered=True, handling duplicate key errors.

    On initial failure (e.g. InsertOne hits existing _id), falls back to 
    the replace list. On Error 11000 in the fallback, skips the op and 
    continues. Safe for single-threaded replay where the conflicting
    document is transient (will be deleted later in the same stream).

    NOTE - This skip behavior is only safe when ordering is guaranteed
    within the stream for the same compound key. Collections with unique
    indexes on non-_id fields MUST use --single-thread-collections to
    ensure all events for competing keys are processed in order.
    """
    try:
        destCollection.bulk_write(bulkOpList, ordered=True)
    except BulkWriteError:
        _execute_with_dup_skip(destCollection, bulkOpListReplace)

def _execute_with_dup_skip(destCollection, ops):
    """Run ops ordered=True. On Error 11000, skip the failed op and continue."""
    remaining = ops
    while remaining:
        try:
            destCollection.bulk_write(remaining, ordered=True)
            break
        except BulkWriteError as bwe:
            write_errors = bwe.details.get('writeErrors', [])
            non_dup_errors = [e for e in write_errors if e['code'] != 11000]
            if non_dup_errors:
                raise
            failed_index = write_errors[0]['index']
            remaining = remaining[failed_index + 1:]

def logIt(threadnum, message):
    logTimeStamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    print("[{}] thread {:>3d} | {}".format(logTimeStamp,threadnum,message))

def _open_stream(sourceConnection, appConfig, resumeToken):
    pipeline = [
        {'$match': {'operationType': {'$in': ['insert', 'update', 'replace', 'delete']}}},
        {'$project': {'updateDescription': 0}}
    ]
    kwargs = dict(resume_after={'_data': resumeToken}, full_document='updateLookup', pipeline=pipeline)
    if (appConfig["startTs"] == "RESUME_TOKEN") and not appConfig["sourceDb"]:
        return sourceConnection.watch(**kwargs)
    else:
        return sourceConnection[appConfig["sourceDb"]].watch(**kwargs)

def _new_source_connection(appConfig):
    return pymongo.MongoClient(
        host=appConfig["sourceUri"],
        appname='mvutool',
        socketTimeoutMS=30000,
        serverSelectionTimeoutMS=30000,
    )

def _get_thread_for_event(change, appConfig):
    """Determine which thread should process this event.

    Thread layout (with --threads T and D dedicated collections):
      0 .. (T-D-1)  = hash-partitioned pool
      (T-D) .. (T-1) = dedicated threads (one per collection)

    Collections in singleThreadCollectionMap route to their dedicated thread.
    All other collections use hash partitioning across the pool.
    """
    thisNs = change['ns']['db'] + '.' + change['ns']['coll']
    if thisNs in appConfig['singleThreadCollectionMap']:
        return appConfig['singleThreadCollectionMap'][thisNs]
    return int(hashlib.sha512(str(change['documentKey']).encode('utf-8')).hexdigest(), 16) % appConfig["numProcessingThreads"]

def _flush_batch(destConnection, nsBulkOpDict, nsBulkOpDictReplace, numCurrentBulkOps, appConfig, perfQ, threadnum, endTs, resumeToken):
    """Flush the current batch to the target and report progress."""
    if not appConfig['dryRun']:
        for ns in nsBulkOpDict:
            destDatabase = destConnection[(ns.split('.', 1)[0])]
            destCollection = destDatabase[(ns.split('.', 1)[1])]
            _execute_batch(destCollection, nsBulkOpDict[ns], nsBulkOpDictReplace[ns])
    if perfQ is not None:
        perfQ.put({"name": "batchCompleted", "operations": numCurrentBulkOps, "endts": endTs, "processNum": threadnum, "resumeToken": resumeToken})

def change_stream_processor(threadnum, appConfig, perfQ):
    if appConfig['verboseLogging']:
        logIt(threadnum, 'thread started')

    sourceConnection = _new_source_connection(appConfig)
    destConnection = pymongo.MongoClient(
        host=appConfig["targetUri"],
        appname='mvutool',
        socketTimeoutMS=30000,
        serverSelectionTimeoutMS=30000,
    )
    startTime = time.time()
    lastBatch = time.time()
    allDone = False
    waitcount = 0
    nsBulkOpDict = defaultdict(list)
    nsBulkOpDictReplace = defaultdict(list)
    numCurrentBulkOps = 0
    printedFirstTs = False
    endTs = appConfig["startTs"]

    currentResumeToken = appConfig["startPosition"]
    lastReportedResumeToken = currentResumeToken

    if appConfig['verboseLogging'] and appConfig["startTs"] == "RESUME_TOKEN":
        logIt(threadnum, "Creating change stream cursor for resume token {}".format(currentResumeToken))

    stream = _open_stream(sourceConnection, appConfig, currentResumeToken)

    while not allDone:
        try:
            if not stream.alive:
                logIt(threadnum, "cursor dead, reconnecting from {}".format(currentResumeToken))
                try:
                    stream.close()
                except Exception:
                    pass
                stream = _open_stream(sourceConnection, appConfig, currentResumeToken)
                continue

            change = stream.try_next()

            if stream.resume_token is not None:
                currentResumeToken = stream.resume_token['_data']

            if ((time.time() - startTime) > appConfig['durationSeconds']) and (appConfig['durationSeconds'] != 0):
                allDone = True
                break

            if change is None:
                waitcount += 1
                if waitcount <= appConfig["maxSecondsBetweenBatches"]:
                    time.sleep(1)
                    continue
                else:
                    waitcount = 0
                    if numCurrentBulkOps > 0:
                        if appConfig['verboseLogging']:
                            logIt(threadnum, f'Timeout reached, processing batch of {numCurrentBulkOps} operations')
                        _flush_batch(destConnection, nsBulkOpDict, nsBulkOpDictReplace, numCurrentBulkOps, appConfig, perfQ, threadnum, endTs, lastReportedResumeToken)
                        nsBulkOpDict = defaultdict(list)
                        nsBulkOpDictReplace = defaultdict(list)
                        numCurrentBulkOps = 0
                        lastBatch = time.time()
                    continue

            waitcount = 0

            endTs = change['clusterTime']
            resumeToken = change['_id']['_data']
            lastReportedResumeToken = resumeToken
            thisDb = change['ns']['db']
            thisCol = change['ns']['coll']
            thisNs = thisDb + '.' + thisCol
            thisOp = change['operationType']

            if _get_thread_for_event(change, appConfig) == threadnum:
                if (not printedFirstTs) and (thisOp in ['insert', 'update', 'replace', 'delete']):
                    if appConfig['verboseLogging']:
                        logIt(threadnum, 'first timestamp = {} aka {}'.format(change['clusterTime'], change['clusterTime'].as_datetime()))
                    printedFirstTs = True

                if thisOp == 'insert':
                    nsBulkOpDict[thisNs].append(pymongo.InsertOne(change['fullDocument']))
                    nsBulkOpDictReplace[thisNs].append(pymongo.ReplaceOne(change['documentKey'], change['fullDocument'], upsert=True))
                    numCurrentBulkOps += 1
                elif thisOp in ['update', 'replace']:
                    if change['fullDocument'] is not None:
                        nsBulkOpDict[thisNs].append(pymongo.ReplaceOne(change['documentKey'], change['fullDocument'], upsert=True))
                        nsBulkOpDictReplace[thisNs].append(pymongo.ReplaceOne(change['documentKey'], change['fullDocument'], upsert=True))
                        numCurrentBulkOps += 1
                elif thisOp == 'delete':
                    nsBulkOpDict[thisNs].append(pymongo.DeleteOne({'_id': change['documentKey']['_id']}))
                    nsBulkOpDictReplace[thisNs].append(pymongo.DeleteOne({'_id': change['documentKey']['_id']}))
                    numCurrentBulkOps += 1
                elif thisOp in ['drop', 'rename', 'dropDatabase', 'invalidate']:
                    pass
                else:
                    print(change)
                    sys.exit(1)

            if ((numCurrentBulkOps >= appConfig["maxOperationsPerBatch"]) or (time.time() >= (lastBatch + appConfig["maxSecondsBetweenBatches"]))) and (numCurrentBulkOps > 0):
                _flush_batch(destConnection, nsBulkOpDict, nsBulkOpDictReplace, numCurrentBulkOps, appConfig, perfQ, threadnum, endTs, resumeToken)
                nsBulkOpDict = defaultdict(list)
                nsBulkOpDictReplace = defaultdict(list)
                numCurrentBulkOps = 0
                lastBatch = time.time()

        except pymongo.errors.PyMongoError as e:
            logIt(threadnum, "change stream error: {}, reconnecting in 5s from {}".format(e, currentResumeToken))
            time.sleep(5)
            try:
                stream.close()
            except Exception:
                pass
            try:
                sourceConnection.close()
            except Exception:
                pass
            sourceConnection = _new_source_connection(appConfig)
            stream = _open_stream(sourceConnection, appConfig, currentResumeToken)

    if numCurrentBulkOps > 0:
        _flush_batch(destConnection, nsBulkOpDict, nsBulkOpDictReplace, numCurrentBulkOps, appConfig, None, threadnum, endTs, None)

    sourceConnection.close()
    destConnection.close()
    perfQ.put({"name": "processCompleted", "processNum": threadnum})

def get_resume_token(appConfig):
    sourceConnection = pymongo.MongoClient(host=appConfig["sourceUri"],appname='mvutool')
    try:
        if not appConfig["sourceDb"]:
            stream = sourceConnection.watch()
            logIt(-1,'getting current change stream resume token')
        else:
            sourceDatabase=sourceConnection[appConfig["sourceDb"]]
            stream=sourceDatabase.watch()
            logIt(-1,'getting current change stream resume token for ' + appConfig["sourceDb"] + " database")

        logIt(-1,'waiting for a write operation on the source to capture a token...')
        waitStart = time.time()
        while True:
            change = stream.try_next()
            if change is not None:
                resumeToken = change['_id']['_data']
                logIt(-1,'resume token: {}'.format(resumeToken))
                break
            elapsed = int(time.time() - waitStart)
            if elapsed > 0 and elapsed % 30 == 0:
                logIt(-1,'still waiting ({} seconds). A write (insert/update/delete) must occur on the source to generate a token.'.format(elapsed))
            time.sleep(1)
    finally:
        sourceConnection.close()

def reporter(appConfig, perfQ):
    if appConfig['verboseLogging']:
        logIt(-1,'reporting thread started')

    startTime = time.time()
    lastTime = time.time()

    lastProcessedOplogEntries = 0

    resumeToken = 'N/A'

    numWorkersCompleted = 0
    numProcessedOplogEntries = 0

    dtDict = {}

    while (numWorkersCompleted < appConfig["totalWorkers"]):
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
                if qMessage.get('resumeToken') and qMessage['resumeToken'] != 'N/A':
                    resumeToken = qMessage['resumeToken']

            elif qMessage['name'] == "processCompleted":
                numWorkersCompleted += 1

        elapsedSeconds = nowTime - startTime
        totalOpsPerSecond = numProcessedOplogEntries / elapsedSeconds

        thisHours, rem = divmod(elapsedSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)

        intervalElapsedSeconds = nowTime - lastTime
        intervalOpsPerSecond = (numProcessedOplogEntries - lastProcessedOplogEntries) / intervalElapsedSeconds

        dtUtcNow = datetime.now(timezone.utc).replace(tzinfo=None)
        secsBehind = 0
        for thisDt in dtDict:
            secondsBehind = (dtUtcNow - dtDict[thisDt].replace(tzinfo=None)).total_seconds()
            secsBehind = max(secsBehind, secondsBehind)
        secsBehind = int(secsBehind)

        logTimeStamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        print("[{0}] elapsed {1} | total o/s {2:12,.2f} | interval o/s {3:12,.2f} | tot {4:16,d} | {5:12,d} secs behind | resume token = {6}".format(logTimeStamp,thisHMS,totalOpsPerSecond,intervalOpsPerSecond,numProcessedOplogEntries,secsBehind,resumeToken))

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

    parser.add_argument('--single-thread-collections',
                        required=False,
                        type=str,
                        default='',
                        help='Comma-separated list of db.collection namespaces that require '
                             'single-threaded processing. Each collection gets its own dedicated '
                             'thread, separate from the hash-partitioned pool. Required for '
                             'collections with unique indexes on non-_id fields when running '
                             'with multiple threads. '
                             'Example: mydb.device_tokens,mydb.user_sessions')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['sourceUri'] = args.source_uri
    appConfig['targetUri'] = args.target_uri
    appConfig['maxSecondsBetweenBatches'] = args.max_seconds_between_batches
    appConfig['maxOperationsPerBatch'] = args.max_operations_per_batch
    appConfig['durationSeconds'] = args.duration_seconds
    appConfig['feedbackSeconds'] = args.feedback_seconds
    appConfig['dryRun'] = args.dry_run
    appConfig['sourceDb'] = args.source_database
    appConfig['startPosition'] = args.start_position
    appConfig['verboseLogging'] = args.verbose
    appConfig['cdcSource'] = 'changeStream'
    singleThreadList = [ns.strip() for ns in args.single_thread_collections.split(',') if ns.strip()]
    numDedicated = len(singleThreadList)
    numHashThreads = args.threads - numDedicated
    if numHashThreads < 1:
        parser.error(
            "--threads must be greater than the number of --single-thread-collections. "
            "You specified {} threads and {} single-thread collections, leaving no threads "
            "for the hash-partitioned pool.".format(args.threads, numDedicated)
        )
    appConfig['numProcessingThreads'] = numHashThreads
    appConfig['singleThreadCollectionMap'] = {
        ns: numHashThreads + i for i, ns in enumerate(singleThreadList)
    }
    appConfig['totalWorkers'] = args.threads

    if args.get_resume_token:
        get_resume_token(appConfig)
        sys.exit(0)
    elif (not args.get_resume_token) and args.target_uri =='no-target-uri':
        message = "you need to supply target uri to run it"
        parser.error(message)

    logIt(-1,"processing {} using {} total threads ({} hash-partitioned, {} dedicated)".format(
        appConfig['cdcSource'], appConfig['totalWorkers'],
        appConfig['numProcessingThreads'], len(appConfig['singleThreadCollectionMap'])))

    if appConfig['singleThreadCollectionMap']:
        for ns, tid in sorted(appConfig['singleThreadCollectionMap'].items(), key=lambda x: x[1]):
            logIt(-1,"  thread {} -> {}".format(tid, ns))

    if appConfig["startPosition"] == "0":
        parser.error(
            "--start-position 0 is only valid with --get-resume-token. "
            "To start replication, provide a 36-character resume token."
        )
    elif len(appConfig["startPosition"]) == 36:
        appConfig["startTs"] = "RESUME_TOKEN"
        logIt(-1,"starting with resume token = {}".format(appConfig["startPosition"]))
    else:
        parser.error(
            "Invalid --start-position value. Expected a 36-character resume token, "
            "got {} characters. Use --get-resume-token to obtain a valid token.".format(
                len(appConfig["startPosition"]))
        )

    mp.set_start_method('spawn')
    q = mp.Manager().Queue()

    t = threading.Thread(target=reporter,args=(appConfig,q))
    t.start()

    processList = []
    for loop in range(appConfig["totalWorkers"]):
        p = mp.Process(target=change_stream_processor,args=(loop,appConfig,q))
        processList.append(p)

    for process in processList:
        process.start()

    for process in processList:
        process.join()

    t.join()

if __name__ == "__main__":
    main()
