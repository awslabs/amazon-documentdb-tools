import argparse
import os
import sys
import time
import csv
import pymongo
from bson.timestamp import Timestamp
from datetime import datetime, timedelta, timezone


def printLog(thisMessage,thisFile):
    print(thisMessage)
    thisFile.write("{}\n".format(thisMessage))


def parseOplog(appConfig):
    logTimeStamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    logFileName = "{}-{}-mongo-oplog-review.log".format(appConfig['serverAlias'],logTimeStamp)
    fp = open(logFileName, 'w')

    printLog('connecting to MongoDB aliased as {}'.format(appConfig['serverAlias']),fp)
    client = pymongo.MongoClient(host=appConfig['uri'],appname='mdboplrv')
    oplog = client.local.oplog.rs

    secondsBehind = 999999

    # oldest and newest oplog entries
    first = oplog.find({},{'ts':1}).sort('$natural', pymongo.ASCENDING).limit(1).next()
    last = oplog.find({},{'ts':1}).sort('$natural', pymongo.DESCENDING).limit(1).next()

    endTs = None
    if appConfig['startTime'] is None and appConfig['windowSeconds'] is None:
        # start with first oplog entry, read until stopped (original behavior)
        startTs = first['ts']
    else:
        # read a fixed time window [startTs, endTs]
        if appConfig['startTime'] is not None:
            startSec = int(appConfig['startTime'].timestamp())
            if appConfig['windowSeconds'] is not None:
                endSec = startSec + appConfig['windowSeconds']
                endTs = Timestamp(endSec, 4294967295) if endSec < last['ts'].time else last['ts']
            else:
                endTs = last['ts']
        else:
            # window ends at the newest oplog entry
            endTs = last['ts']
            startSec = endTs.time - appConfig['windowSeconds']
        if (endTs.time < first['ts'].time) or (startSec > last['ts'].time):
            printLog("ERROR: the requested window is outside the oplog, which covers {} to {}.".format(first['ts'].as_datetime(),last['ts'].as_datetime()),fp)
            sys.exit(1)
        if startSec < first['ts'].time:
            printLog("WARNING: the oplog starts at {}, after the requested start; the window starts at the oldest entry.".format(first['ts'].as_datetime()),fp)
            startTs = first['ts']
        else:
            startTs = Timestamp(startSec, 0)

    currentTs = startTs

    printLog("starting with opLog timestamp = {}".format(currentTs.as_datetime()),fp)
    if endTs is not None:
        printLog("stopping at opLog timestamp  = {}".format(endTs.as_datetime()),fp)

    numTotalOplogEntries = 0
    numMigrateEntries = 0
    numTxnEntries = 0
    opDict = {}
    multiNsTxnDict = {}
    opKeys = {'i':'ins','u':'upd','d':'del','c':'com','n':'nop'}

    def countOp(thisOp, thisNs):
        # one oplog operation = one changed document for i/u/d
        if thisOp == 'n':
            thisNs = '**NO-OP**'
        if thisOp not in opKeys:
            return False
        if thisNs not in opDict:
            opDict[thisNs] = {'ins':0,'upd':0,'del':0,'com':0,'nop':0}
        opDict[thisNs][opKeys[thisOp]] += 1
        return True

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
    elapsedSeconds = 0
    allDone = False

    #sourceNs = "<database>.<collection>"

    while not allDone:
        if appConfig['includeAllDatabases']:
            thisProjection = {'op':1,'ns':1,'ts':1,'fromMigrate':1,'o.applyOps.op':1,'o.applyOps.ns':1,'o.applyOps.fromMigrate':1}
            if endTs is None:
                cursor = oplog.find({'ts': {'$gte': currentTs}},thisProjection,cursor_type=pymongo.CursorType.TAILABLE_AWAIT,oplog_replay=True,batch_size=appConfig['batchSize'])
            else:
                cursor = oplog.find({'ts': {'$gte': currentTs, '$lte': endTs}},thisProjection,oplog_replay=True,batch_size=appConfig['batchSize'])
        else:
            #cursor = oplog.find({'ts': {'$gte': currentTs},'ns':sourceNs},cursor_type=pymongo.CursorType.TAILABLE_AWAIT,oplog_replay=True)
            printLog('Namespace specific parsing is not yet supported.',fp)
            sys.exit(1)
            
        while cursor.alive and not allDone:
            for doc in cursor:
                currentTs = doc['ts']
                
                numTotalOplogEntries += 1
                if ((numTotalOplogEntries % appConfig['numOperationsFeedback']) == 0) or ((lastFeedback + appConfig['numSecondsFeedback']) < time.time()):
                    lastFeedback = time.time()
                    elapsedSeconds = time.time() - startTime
                    secondsBehind = int((datetime.now(timezone.utc) - currentTs.as_datetime().replace(tzinfo=timezone.utc)).total_seconds())
                    if (elapsedSeconds != 0):
                        printLog("  tot oplog entries read {:16,d} @ {:12,.0f} per second | {:12,d} seconds behind".format(numTotalOplogEntries,numTotalOplogEntries//elapsedSeconds,secondsBehind),fp)
                    else:
                        printLog("  tot oplog entries read {:16,d} @ {:12,.0f} per second | {:12,d} seconds behind".format(0,0.0,secondsBehind),fp)

                    # check if time to stop
                    elapsedSeconds = time.time() - startTime
                    if ((elapsedSeconds >= appConfig['collectSeconds']) or (appConfig['stopWhenOplogCurrent'] and (endTs is None) and (secondsBehind < 60))):
                        allDone = True
                        break

                if doc.get('fromMigrate', False) and not appConfig['includeMigrations']:
                    # chunk migration (balancer / moveChunk), not an application write
                    numMigrateEntries += 1
                    continue

                if (doc['op'] == 'c') and isinstance(doc.get('o',{}).get('applyOps'), list):
                    # transaction (or applyOps command): count each inner operation on its own namespace
                    numTxnEntries += 1
                    txnNamespaces = set()
                    for innerOp in doc['o']['applyOps']:
                        if innerOp.get('fromMigrate', False) and not appConfig['includeMigrations']:
                            numMigrateEntries += 1
                            continue
                        if not countOp(innerOp.get('op'), innerOp.get('ns')):
                            printLog(innerOp,fp)
                            sys.exit(1)
                        if innerOp.get('op') in ('i','u','d'):
                            txnNamespaces.add(innerOp.get('ns'))
                    if len(txnNamespaces) > 1:
                        thisKey = ' | '.join(sorted(txnNamespaces))
                        multiNsTxnDict[thisKey] = multiNsTxnDict.get(thisKey,0) + 1
                    continue

                if not countOp(doc['op'], doc.get('ns')):
                    printLog(doc,fp)
                    sys.exit(1)

            # check if time to stop
            elapsedSeconds = time.time() - startTime
            if (endTs is not None) or (elapsedSeconds >= appConfig['collectSeconds']) or (appConfig['stopWhenOplogCurrent'] and (endTs is None) and (secondsBehind < 60)):
                allDone = True
                break

        if endTs is not None:
            # bounded window: one pass of the cursor reads all of it
            allDone = True

    # print overall ops, ips/ups/dps

    if endTs is not None:
        if numTotalOplogEntries > 0 and (elapsedSeconds >= appConfig['collectSeconds']) and (currentTs.time < endTs.time):
            printLog("WARNING: --collect-seconds stopped the read before the end of the window; rates cover {} to {} only.".format(startTs.as_datetime(),currentTs.as_datetime()),fp)
            endTs = currentTs
        oplogSeconds = max((endTs.as_datetime()-startTs.as_datetime()).total_seconds(),1)
    else:
        oplogSeconds = max((currentTs.as_datetime()-startTs.as_datetime()).total_seconds(),1)
    oplogMinutes = oplogSeconds/60
    oplogHours = oplogMinutes/60
    oplogDays = oplogHours/24

    if appConfig['unitOfMeasure'] == 'sec':
        calcDivisor = oplogSeconds
    elif appConfig['unitOfMeasure'] == 'min':
        calcDivisor = oplogMinutes
    elif appConfig['unitOfMeasure'] == 'hr':
        calcDivisor = oplogHours
    else:
        calcDivisor = oplogDays

    # determine width needed for namespace
    nsWidth = 10
    for thisOpKey in opDict.keys():
        if len(thisOpKey) > nsWidth:
            nsWidth = len(thisOpKey)

    printLog("",fp)
    printLog("-----------------------------------------------------------------------------------------",fp)
    printLog("",fp)
   
    printLog("opLog elapsed seconds = {}".format(oplogSeconds),fp)
    printLog("transaction (applyOps) entries unwound = {:,d}".format(numTxnEntries),fp)
    if appConfig['includeMigrations']:
        printLog("chunk migration (fromMigrate) entries are INCLUDED in the counts",fp)
    else:
        printLog("chunk migration (fromMigrate) operations skipped = {:,d}".format(numMigrateEntries),fp)

    # Write to CSV if requested
    if appConfig['outputToCsv']:
        try:
            with open(appConfig['fileName'], 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                # Write header
                csvwriter.writerow(['Namespace', 
                                   'Tot Inserts', 'Per '+appConfig['unitOfMeasure'],
                                   'Tot Updates', 'Per '+appConfig['unitOfMeasure'],
                                   'Tot Deletes', 'Per '+appConfig['unitOfMeasure'],
                                   'Tot Commands', 'Per '+appConfig['unitOfMeasure'],
                                   'Tot No-Ops', 'Per '+appConfig['unitOfMeasure']])
                
                # Write data rows
                for thisOpKey in sorted(opDict.keys()):
                    csvwriter.writerow([thisOpKey,
                                       opDict[thisOpKey]['ins'], opDict[thisOpKey]['ins']//calcDivisor,
                                       opDict[thisOpKey]['upd'], opDict[thisOpKey]['upd']//calcDivisor,
                                       opDict[thisOpKey]['del'], opDict[thisOpKey]['del']//calcDivisor,
                                       opDict[thisOpKey]['com'], opDict[thisOpKey]['com']//calcDivisor,
                                       opDict[thisOpKey]['nop'], opDict[thisOpKey]['nop']//calcDivisor])
            printLog(f"CSV output written to {appConfig['fileName']}", fp)
        except Exception as e:
            printLog(f"Error writing to CSV file: {str(e)}", fp)

    # print collection ops, ips/ups/dps
    printLog("{:<{dbWidth}s} | {:<{intWidth}s} | {:<{floatWidth}s} | {:<{intWidth}s} | {:<{floatWidth}s} | {:<{intWidth}s} | {:<{floatWidth}s} | {:<{intWidth}s} | {:<{floatWidth}s} | {:<{intWidth}s} | {:<{floatWidth}s}".format('Namespace',
            'Tot Inserts','Per '+appConfig['unitOfMeasure'],
            'Tot Updates','Per '+appConfig['unitOfMeasure'],
            'Tot Deletes','Per '+appConfig['unitOfMeasure'],
            'Tot Commands','Per '+appConfig['unitOfMeasure'],
            'Tot No-Ops','Per '+appConfig['unitOfMeasure'],
            dbWidth=nsWidth,
            intWidth=15,
            floatWidth=10
            ),fp)
            
    for thisOpKey in sorted(opDict.keys()):
        printLog("{:<{dbWidth}s} | {:<{intWidth},d} | {:<{floatWidth},.0f} | {:<{intWidth},d} | {:<{floatWidth},.0f} | {:<{intWidth},d} | {:<{floatWidth},.0f} | {:<{intWidth},d} | {:<{floatWidth},.0f} | {:<{intWidth},d} | {:<{floatWidth},.0f}".format(thisOpKey,
            opDict[thisOpKey]['ins'],opDict[thisOpKey]['ins']//calcDivisor,
            opDict[thisOpKey]['upd'],opDict[thisOpKey]['upd']//calcDivisor,
            opDict[thisOpKey]['del'],opDict[thisOpKey]['del']//calcDivisor,
            opDict[thisOpKey]['com'],opDict[thisOpKey]['com']//calcDivisor,
            opDict[thisOpKey]['nop'],opDict[thisOpKey]['nop']//calcDivisor,
            dbWidth=nsWidth,
            intWidth=15,
            floatWidth=10
            ),fp)

    printLog("",fp)

    if multiNsTxnDict:
        printLog("transactions that change more than one namespace on this replica set (oplog entries; a large transaction can span several entries)",fp)
        for thisKey in sorted(multiNsTxnDict, key=multiNsTxnDict.get, reverse=True):
            printLog("  {:>10,d} | {}".format(multiNsTxnDict[thisKey],thisKey),fp)
        printLog("",fp)

    client.close()
    fp.close()

def main():
    # roadmap
    
    # v1
    #   * mvp
    
    # v2
    #   * add option to run for specified number of seconds
    #   * start from last timestamp when creating oplog cursor
    #   * output number of "seconds behind" when running
    #   * add feature to stop when the oplog is current (default False)

    # v3
    #   - add logIt [proper timestamp, duration, etc]
    #   - scope to limited number of databases
    #   - parameterize more options
    #   - add option to run from/to particular timestamp

    parser = argparse.ArgumentParser(description='Calculate collection level acvitivity using the oplog.')
        
    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')
    
    parser.add_argument('--uri',
                        required=True,
                        type=str,
                        help='MongoDB Connection URI')

    parser.add_argument('--server-alias',
                        required=True,
                        type=str,
                        help='Alias for server, used to name output file')
                        
    parser.add_argument('--unit-of-measure',
                        required=False,
                        default='day',
                        help='Unit of measure for reporting [sec | min | hr | day]')

    parser.add_argument('--collect-seconds',
                        required=False,
                        type=int,
                        default=10800,
                        help='Number of seconds to parse opLog before stopping.')

    parser.add_argument('--batch-size',
                        required=False,
                        type=int,
                        default=1000,
                        help='Number of oplog entries to retrieve per batch [default 1000].')

    parser.add_argument('--stop-when-oplog-current',
                        required=False,
                        action='store_true',
                        help='Stop processing and output results when fully caught up on the oplog')
                        
    parser.add_argument('--output-to-csv',
                        required=False,
                        action='store_true',
                        help='Output results to a CSV file')

    parser.add_argument('--file-name',
                        required=False,
                        type=str,
                        help='Name of the CSV file to write (default: <server-alias>_oplog_stats.csv)')

    parser.add_argument('--start-time',
                        required=False,
                        type=str,
                        help='Start of the window to read, UTC ISO 8601 (e.g. 2026-11-19T14:00:00Z). Default: the oldest oplog entry.')

    parser.add_argument('--window-seconds',
                        required=False,
                        type=int,
                        help='Length of the window to read in seconds. With --start-time the window starts there; without it the window ends at the newest oplog entry.')

    parser.add_argument('--include-migrations',
                        required=False,
                        action='store_true',
                        help='Count chunk migration entries (fromMigrate: true) as inserts/deletes (default: skip them)')

    args = parser.parse_args()
    
    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    if args.unit_of_measure not in ['sec','min','hr','day']:
        message = "--unit-of-measure must be one of ['sec','min','hr','day'] for second, minute, hour, day."
        parser.error(message)
        
    startTime = None
    if args.start_time is not None:
        try:
            startTime = datetime.fromisoformat(args.start_time.replace('Z','+00:00'))
        except ValueError:
            parser.error("--start-time must be UTC ISO 8601, for example 2026-11-19T14:00:00Z")
        if startTime.tzinfo is None:
            startTime = startTime.replace(tzinfo=timezone.utc)

    if args.window_seconds is not None and args.window_seconds < 1:
        parser.error("--window-seconds must be >= 1")

    appConfig = {}
    appConfig['startTime'] = startTime
    appConfig['windowSeconds'] = args.window_seconds
    appConfig['includeMigrations'] = args.include_migrations
    appConfig['uri'] = args.uri
    appConfig['serverAlias'] = args.server_alias
    appConfig['collectSeconds'] = args.collect_seconds
    appConfig['unitOfMeasure'] = args.unit_of_measure
    appConfig['stopWhenOplogCurrent'] = args.stop_when_oplog_current
    appConfig['outputToCsv'] = args.output_to_csv
    appConfig['fileName'] = args.file_name if args.file_name else f"{args.server_alias}_oplog_stats.csv"
    
    # consume all of the oplog rather than scoping to particular namespaces
    appConfig['includeAllDatabases'] = True

    appConfig['numOperationsFeedback'] = 200000
    appConfig['numSecondsFeedback'] = 5
    
    #appConfig['maxOplogEntries'] = 6100000
    #appConfig['maxSecondsBetweenBatches'] = 1
    
    appConfig['batchSize'] = args.batch_size

    # handled via URI
    #mongoWireCompression="none"
    #mongoWireCompression="zlib"
    #mongoWireCompression="snappy"

    parseOplog(appConfig)


if __name__ == "__main__":
    main()
