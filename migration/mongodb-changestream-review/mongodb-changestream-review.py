import argparse
import csv
import os
import sys
import time
import pymongo
from bson.timestamp import Timestamp
from datetime import datetime, timedelta, timezone
import warnings


def printLog(thisMessage,thisFile):
    print(thisMessage)
    thisFile.write("{}\n".format(thisMessage))


def tokenSeconds(resumeToken):
    # MongoDB resume token: '_data' is a hex keystring; a first byte 0x82 marks
    # a Timestamp, and the next 4 bytes are its seconds (big-endian).
    # Returns None for any other format (for example Amazon DocumentDB).
    try:
        data = resumeToken['_data']
        if isinstance(data, str) and len(data) >= 10 and data[0:2].lower() == '82':
            return int(data[2:10], 16)
    except Exception:
        pass
    return None


def parseChangestream(appConfig):
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")    
    
    startTs = appConfig['startTs']
    
    logTimeStamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    logFileName = "{}-{}-mongo-changestream-review.log".format(appConfig['serverAlias'],logTimeStamp)
    fp = open(logFileName, 'w')

    printLog('connecting to MongoDB aliased as {}'.format(appConfig['serverAlias']),fp)
    client = pymongo.MongoClient(host=appConfig['uri'],appname='mdbcstrv',unicode_decode_error_handler='ignore')

    secondsBehind = 999999

    printLog("starting with timestamp = {}".format(startTs.as_datetime()),fp)

    # optional fixed window [startTs, endSec]
    endSec = None
    if appConfig['windowSeconds'] is not None:
        endSec = startTs.time + appConfig['windowSeconds']
        printLog("stopping at timestamp  = {}".format(Timestamp(endSec, 0).as_datetime()),fp)
        if endSec > time.time():
            printLog("NOTE: the window ends in the future; the tool reads until then (or until --collect-seconds)",fp)

    numTotalChangestreamEntries = 0
    opDict = {}

    # cluster time of the last qualifying change; stays None if the
    # changestream yields zero qualifying changes in the collected window
    currentTs = None

    startTime = time.time()
    lastFeedback = time.time()
    allDone = False
    windowDone = False
    
    noChangesPauseSeconds = 5.0

    '''
    i  = insert
    u  = update
    d  = delete
    c  = command
    db = database
    n  = no-op
    '''

    with client.watch(start_at_operation_time=startTs, full_document=None, pipeline=[{'$match': {'operationType': {'$in': ['insert','update','replace','delete']}}},{'$project':{'updateDescription':0,'fullDocument':0}}]) as stream:
        while stream.alive and not allDone:
            change = stream.try_next()
            resumeToken = stream.resume_token

            # check if time to stop - elapsed time
            elapsedSeconds = (time.time() - startTime)
            if (elapsedSeconds >= appConfig['collectSeconds']):
                print("reached requested elapsed {} seconds, stopping".format(appConfig['collectSeconds']))
                allDone = True
                break
            
            if change is None:
                # no changes available - might be time to stop
                if endSec is not None:
                    tokenSec = tokenSeconds(resumeToken) if resumeToken is not None else None
                    if (tokenSec is not None) and (tokenSec > endSec):
                        # the stream is past the end of the window
                        windowDone = True
                        allDone = True
                        break
                    if tokenSec is not None:
                        # stream position is known and still inside the window - keep reading
                        time.sleep(noChangesPauseSeconds)
                        continue
                if appConfig['stopWhenChangestreamCurrent']:
                    print("change stream is current, stopping")
                    allDone = True
                    break
                #print("  no changes, pausing for {} second(s)".format(noChangesPauseSeconds))
                time.sleep(noChangesPauseSeconds)
                continue
            else:
                #print("change doc is | {}".format(change))
                pass

            # check if time to stop - past the end of the window
            if (endSec is not None) and (change['clusterTime'].time > endSec):
                windowDone = True
                allDone = True
                break

            # check if time to stop - current enough
            if (endSec is None) and (appConfig['stopWhenChangestreamCurrent'] and (secondsBehind < 60)):
                print("change stream is current, stopping")
                allDone = True
                break
            
            currentTs = change['clusterTime']
            resumeToken = change['_id']['_data']
            thisNs = change['ns']['db']+'.'+change['ns']['coll']
            thisOpType = change['operationType']
                
            numTotalChangestreamEntries += 1
            if ((numTotalChangestreamEntries % appConfig['numOperationsFeedback']) == 0) or ((lastFeedback + appConfig['numSecondsFeedback']) < time.time()):
                lastFeedback = time.time()
                elapsedSeconds = time.time() - startTime
                secondsBehind = int((datetime.now(timezone.utc) - currentTs.as_datetime().replace(tzinfo=timezone.utc)).total_seconds())
                if (elapsedSeconds != 0):
                    printLog("  tot changestream entries read {:16,d} @ {:12,.0f} per second | {:12,d} seconds behind".format(numTotalChangestreamEntries,numTotalChangestreamEntries//elapsedSeconds,secondsBehind),fp)
                else:
                    printLog("  tot changestream entries read {:16,d} @ {:12,.0f} per second | {:12,d} seconds behind".format(0,0.0,secondsBehind),fp)

            if (thisOpType == 'insert'):
                # insert
                if thisNs in opDict:
                    opDict[thisNs]['ins'] += 1
                else:
                    opDict[thisNs] = {'ins':1,'upd':0,'del':0}
                    
            elif (thisOpType in ['update','replace']):
                # update
                if thisNs in opDict:
                    opDict[thisNs]['upd'] += 1
                else:
                    opDict[thisNs] = {'ins':0,'upd':1,'del':0}
                    
            elif (thisOpType == 'delete'):
                # delete
                if thisNs in opDict:
                    opDict[thisNs]['del'] += 1
                else:
                    opDict[thisNs] = {'ins':0,'upd':0,'del':1}
                    
            else:
                printLog(change,fp)
                sys.exit(1)
                
    # print overall ops, ips/ups/dps

    # currentTs is None when the changestream yielded zero qualifying changes in
    # the collected window - report and return rather than crashing on the
    # elapsed-time calc / rate division below
    if currentTs is None:
        printLog("",fp)
        printLog("-----------------------------------------------------------------------------------------",fp)
        printLog("",fp)
        printLog("no changes found in the collected window",fp)
        printLog("",fp)
        client.close()
        fp.close()
        return

    if (endSec is not None) and windowDone:
        oplogSeconds = float(appConfig['windowSeconds'])
    else:
        if endSec is not None:
            printLog("WARNING: the read stopped before the end of the window; rates cover {} to {} only.".format(startTs.as_datetime(),currentTs.as_datetime()),fp)
        # floor of 1 second: avoids a division by zero when all changes share one second
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
   
    printLog("changestream elapsed seconds = {}".format(oplogSeconds),fp)

    if appConfig['outputToCsv']:
        try:
            with open(appConfig['fileName'], 'w', newline='') as csvfile:
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(['Namespace',
                                   'Tot Inserts', 'Per '+appConfig['unitOfMeasure'],
                                   'Tot Updates', 'Per '+appConfig['unitOfMeasure'],
                                   'Tot Deletes', 'Per '+appConfig['unitOfMeasure']])
                for thisOpKey in sorted(opDict.keys()):
                    csvwriter.writerow([thisOpKey,
                                       opDict[thisOpKey]['ins'], opDict[thisOpKey]['ins']//calcDivisor,
                                       opDict[thisOpKey]['upd'], opDict[thisOpKey]['upd']//calcDivisor,
                                       opDict[thisOpKey]['del'], opDict[thisOpKey]['del']//calcDivisor])
            printLog("CSV output written to {}".format(appConfig['fileName']),fp)
        except Exception as e:
            printLog("Error writing to CSV file: {}".format(str(e)),fp)

    # print collection ops, ips/ups/dps
    printLog("{:<{dbWidth}s} | {:<{intWidth}s} | {:<{floatWidth}s} | {:<{intWidth}s} | {:<{floatWidth}s} | {:<{intWidth}s} | {:<{floatWidth}s}".format('Namespace',
            'Tot Inserts','Per '+appConfig['unitOfMeasure'],
            'Tot Updates','Per '+appConfig['unitOfMeasure'],
            'Tot Deletes','Per '+appConfig['unitOfMeasure'],
            dbWidth=nsWidth,
            intWidth=15,
            floatWidth=10
            ),fp)
            
    for thisOpKey in sorted(opDict.keys()):
        printLog("{:<{dbWidth}s} | {:<{intWidth},d} | {:<{floatWidth},.0f} | {:<{intWidth},d} | {:<{floatWidth},.0f} | {:<{intWidth},d} | {:<{floatWidth},.0f}".format(thisOpKey,
            opDict[thisOpKey]['ins'],opDict[thisOpKey]['ins']//calcDivisor,
            opDict[thisOpKey]['upd'],opDict[thisOpKey]['upd']//calcDivisor,
            opDict[thisOpKey]['del'],opDict[thisOpKey]['del']//calcDivisor,
            dbWidth=nsWidth,
            intWidth=15,
            floatWidth=10
            ),fp)

    printLog("",fp)
            
    client.close()
    fp.close()


def main():
    parser = argparse.ArgumentParser(description='Calculate collection level acvitivity using a changestream.')
        
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
                        choices=['sec','min','hr','day'],
                        help='Unit of measure for reporting [sec | min | hr | day]')

    parser.add_argument('--collect-seconds',
                        required=False,
                        type=int,
                        default=10800,
                        help='Number of seconds to parse changestream before stopping.')

    parser.add_argument('--stop-when-changestream-current',
                        required=False,
                        action='store_true',
                        help='Stop processing and output results when fully caught up on the changestream')
                        
    parser.add_argument('--start-position',
                        required=True,
                        type=str,
                        help='Starting position in UTC: YYYY-MM-DD+HH:MM:SS or ISO 8601 (e.g. 2026-11-19T14:00:00Z)')

    parser.add_argument('--window-seconds',
                        required=False,
                        type=int,
                        help='Length of the window to read in seconds, from --start-position. Rates use the window length.')

    parser.add_argument('--output-to-csv',
                        required=False,
                        action='store_true',
                        help='Output results to a CSV file')

    parser.add_argument('--file-name',
                        required=False,
                        type=str,
                        help='Name of the CSV file to write (default: <server-alias>_changestream_stats.csv)')

    parser.add_argument('--num-operations-feedback',
                        required=False,
                        type=int,
                        default=200000,
                        help='Maximum number of operations per feedback')

    parser.add_argument('--num-seconds-feedback',
                        required=False,
                        type=int,
                        default=5,
                        help='Maximum number of seconds per feedback')

    args = parser.parse_args()
    
    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['serverAlias'] = args.server_alias
    appConfig['collectSeconds'] = args.collect_seconds
    appConfig['unitOfMeasure'] = args.unit_of_measure
    appConfig['stopWhenChangestreamCurrent'] = args.stop_when_changestream_current
    startText = args.start_position.strip()
    if len(startText) > 10 and startText[10] in ('+',' '):
        # original format YYYY-MM-DD+HH:MM:SS
        startText = startText[:10] + 'T' + startText[11:]
    try:
        startDt = datetime.fromisoformat(startText.replace('Z','+00:00'))
    except ValueError:
        parser.error("--start-position must be UTC, YYYY-MM-DD+HH:MM:SS or ISO 8601 (e.g. 2026-11-19T14:00:00Z)")
    if startDt.tzinfo is None:
        startDt = startDt.replace(tzinfo=timezone.utc)

    if args.window_seconds is not None and args.window_seconds < 1:
        parser.error("--window-seconds must be >= 1")

    appConfig["startTs"] = Timestamp(startDt, 1)
    appConfig['windowSeconds'] = args.window_seconds
    appConfig['outputToCsv'] = args.output_to_csv
    appConfig['fileName'] = args.file_name if args.file_name else "{}_changestream_stats.csv".format(args.server_alias)
    appConfig['numOperationsFeedback'] = int(args.num_operations_feedback)
    appConfig['numSecondsFeedback'] = int(args.num_seconds_feedback)
    
    # consume all of the changestream rather than scoping to particular namespaces
    appConfig['includeAllDatabases'] = True
    
    parseChangestream(appConfig)


if __name__ == "__main__":
    main()
