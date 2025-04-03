import argparse
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

    numTotalChangestreamEntries = 0
    opDict = {}
    
    startTime = time.time()
    lastFeedback = time.time()
    allDone = False
    
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

            # check if time to stop - current enough
            if (appConfig['stopWhenChangestreamCurrent'] and (secondsBehind < 60)):
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
   
    printLog("changestream elapsed seconds = {}".format(oplogSeconds),fp)

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
                        help='Starting position - YYYY-MM-DD+HH:MM:SS in UTC')

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
    appConfig["startTs"] = Timestamp(datetime.fromisoformat(args.start_position), 1)
    appConfig['numOperationsFeedback'] = int(args.num_operations_feedback)
    appConfig['numSecondsFeedback'] = int(args.num_seconds_feedback)
    
    # consume all of the changestream rather than scoping to particular namespaces
    appConfig['includeAllDatabases'] = True
    
    parseChangestream(appConfig)


if __name__ == "__main__":
    main()
