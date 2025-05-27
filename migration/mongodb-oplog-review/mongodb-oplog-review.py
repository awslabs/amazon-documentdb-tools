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

    if appConfig['startFromOplogStart']:
        # start with first oplog entry
        first = oplog.find().sort('$natural', pymongo.ASCENDING).limit(1).next()
        startTs = first['ts']
        #printLog(first,fp)
    else:
        # start at an arbitrary position
        startTs = Timestamp(1641240727, 5)
        # get proper startTs from oplog
        printLog('starting with an arbitrary timestamp is not yet supported.',fp)
        sys.exit(1)
        # start with right now
        #startTs = Timestamp(int(time.time()), 1)

    currentTs = startTs

    printLog("starting with opLog timestamp = {}".format(currentTs.as_datetime()),fp)

    numTotalOplogEntries = 0
    opDict = {}

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
    allDone = False

    #sourceNs = "<database>.<collection>"

    while not allDone:
        if appConfig['includeAllDatabases']:
            cursor = oplog.find({'ts': {'$gte': currentTs}},{'op':1,'ns':1,'ts':1},cursor_type=pymongo.CursorType.TAILABLE_AWAIT,oplog_replay=True,batch_size=appConfig['batchSize'])
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
                    if ((elapsedSeconds >= appConfig['collectSeconds']) or (appConfig['stopWhenOplogCurrent'] and (secondsBehind < 60))):
                        allDone = True
                        break

                if (doc['op'] == 'i'):
                    # insert
                    thisOp = doc['ns']
                    if thisOp in opDict:
                        opDict[thisOp]['ins'] += 1
                    else:
                        opDict[thisOp] = {'ins':1,'upd':0,'del':0,'com':0,'nop':0}
                        
                elif (doc['op'] == 'u'):
                    # update
                    thisOp = doc['ns']
                    if thisOp in opDict:
                        opDict[thisOp]['upd'] += 1
                    else:
                        opDict[thisOp] = {'ins':0,'upd':1,'del':0,'com':0,'nop':0}
                        
                elif (doc['op'] == 'd'):
                    # delete
                    thisOp = doc['ns']
                    if thisOp in opDict:
                        opDict[thisOp]['del'] += 1
                    else:
                        opDict[thisOp] = {'ins':0,'upd':0,'del':1,'com':0,'nop':0}
                        
                elif (doc['op'] == 'c'):
                    # command
                    thisOp = doc['ns']
                    if thisOp in opDict:
                        opDict[thisOp]['com'] += 1
                    else:
                        opDict[thisOp] = {'ins':0,'upd':0,'del':0,'com':1,'nop':0}
                        
                elif (doc['op'] == 'n'):
                    # no-op
                    thisOp = '**NO-OP**'
                    if thisOp in opDict:
                        opDict[thisOp]['nop'] += 1
                    else:
                        opDict[thisOp] = {'ins':0,'upd':0,'del':0,'com':0,'nop':1}
                        
                else:
                    printLog(doc,fp)
                    sys.exit(1)
                
            # check if time to stop
            elapsedSeconds = time.time() - startTime
            if ((elapsedSeconds >= appConfig['collectSeconds']) or (appConfig['stopWhenOplogCurrent'] and (secondsBehind < 60))):
                allDone = True
                break

    # print overall ops, ips/ups/dps

    oplogSeconds = (currentTs.as_datetime()-startTs.as_datetime()).total_seconds()
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

    args = parser.parse_args()
    
    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    if args.unit_of_measure not in ['sec','min','hr','day']:
        message = "--unit-of-measure must be one of ['sec','min','hr','day'] for second, minute, hour, day."
        parser.error(message)
        
    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['serverAlias'] = args.server_alias
    appConfig['collectSeconds'] = args.collect_seconds
    appConfig['unitOfMeasure'] = args.unit_of_measure
    appConfig['stopWhenOplogCurrent'] = args.stop_when_oplog_current
    appConfig['outputToCsv'] = args.output_to_csv
    appConfig['fileName'] = args.file_name if args.file_name else f"{args.server_alias}_oplog_stats.csv"
    
    # start from the beginning of the oplog rather than an aribtrary timestamp
    appConfig['startFromOplogStart'] = True

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
