from datetime import datetime, timedelta
import sys
import json
import pymongo
import time
import os
import argparse


def initializeLogFile(appConfig):
    with open(appConfig['logFileName'], "w") as logFile:
        logFile.write("")


def logAndPrint(appConfig,string):
    with open(appConfig['logFileName'], "a") as logFile:
        logFile.write(string+"\n")
    print(string)


def reportCollectionInfo(appConfig):
    collections = {}
    
    if appConfig['showPerSecond']:
        opsDivisor = appConfig['updateFrequencySeconds']
        opsString = "/s"
    else:
        opsDivisor = 1
        opsString = ""

    mustCrud = appConfig['mustCrud']
    
    logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
    logAndPrint(appConfig,"{} | {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s} {:>10s}".format(logTimeStamp,'collection','ins'+opsString,'upd'+opsString,'del'+opsString,'colBlkHit','colBlkRead','colRatio','idxBlkHit','idxBlkRead','idxRatio'))
    
    client = pymongo.MongoClient(host=appConfig['uri'],appname='ddbtop')
    db = client[appConfig['databaseName']]
    
    while True:
        collCursor = db.list_collections()
        
        for thisColl in collCursor:
            thisCollName = thisColl['name']
            
            collStats = db.command("collStats", thisCollName)
            
            if thisCollName not in collections:
                # add it
                collections[thisCollName] = {'opCounter': {'numDocsIns': 0, 'numDocsUpd': 0, 'numDocsDel': 0 },
                                             'cacheStats': {'collBlksHit': 0, 'collBlksRead': 0, 'collHitRatio': 0.0, 'idxBlksHit': 0, 'idxBlksRead': 0, 'idxHitRatio': 0.0}
                                            }

            # output the differences
            diffOI = int((collStats['opCounter']['numDocsIns'] - collections[thisCollName]['opCounter']['numDocsIns'])/opsDivisor)
            diffOU = int((collStats['opCounter']['numDocsUpd'] - collections[thisCollName]['opCounter']['numDocsUpd'])/opsDivisor)
            diffOD = int((collStats['opCounter']['numDocsDel'] - collections[thisCollName]['opCounter']['numDocsDel'])/opsDivisor)

            diffCCH = int(collStats['cacheStats']['collBlksHit'] - collections[thisCollName]['cacheStats']['collBlksHit'])
            diffCCR = int(collStats['cacheStats']['collBlksRead'] - collections[thisCollName]['cacheStats']['collBlksRead'])
            diffCCHR = collStats['cacheStats']['collHitRatio'] - collections[thisCollName]['cacheStats']['collHitRatio']
            diffICH = int(collStats['cacheStats']['idxBlksHit'] - collections[thisCollName]['cacheStats']['idxBlksHit'])
            diffICR = int(collStats['cacheStats']['idxBlksRead'] - collections[thisCollName]['cacheStats']['idxBlksRead'])
            diffICHR = collStats['cacheStats']['idxHitRatio'] - collections[thisCollName]['cacheStats']['idxHitRatio']

            displayLine = False

            if (mustCrud and (diffOI != 0 or diffOU != 0 or diffOD != 0)):
                displayLine = True

            elif ((not mustCrud) and
                  (diffOI != 0 or diffOU != 0 or diffOD != 0 or
                   diffCCH != 0 or diffCCR != 0 or diffCCHR != 0 or 
                   diffICH != 0 or diffICR != 0 or diffICHR != 0)):
                displayLine = True

            if displayLine:
                logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'
                logAndPrint(appConfig,"{} | {:>10s} {:10,d} {:10,d} {:10,d} {:10,d} {:10,d} {:10.4f} {:10,d} {:10,d} {:10.4f}".format(logTimeStamp,thisCollName,diffOI,diffOU,diffOD,diffCCH,diffCCR,diffCCHR,diffICH,diffICR,diffICHR))

            collections[thisCollName]['opCounter']['numDocsIns'] = collStats['opCounter']['numDocsIns']
            collections[thisCollName]['opCounter']['numDocsUpd'] = collStats['opCounter']['numDocsUpd']
            collections[thisCollName]['opCounter']['numDocsDel'] = collStats['opCounter']['numDocsDel']

            collections[thisCollName]['cacheStats']['collBlksHit'] = collStats['cacheStats']['collBlksHit']
            collections[thisCollName]['cacheStats']['collBlksRead'] = collStats['cacheStats']['collBlksRead']
            collections[thisCollName]['cacheStats']['collHitRatio'] = collStats['cacheStats']['collHitRatio']
            collections[thisCollName]['cacheStats']['idxBlksHit'] = collStats['cacheStats']['idxBlksHit']
            collections[thisCollName]['cacheStats']['idxBlksRead'] = collStats['cacheStats']['idxBlksRead']
            collections[thisCollName]['cacheStats']['idxHitRatio'] = collStats['cacheStats']['idxHitRatio']

        time.sleep(appConfig['updateFrequencySeconds'])

    client.close()


def main():
    parser = argparse.ArgumentParser(description='DocumentDB Top')

    parser.add_argument('--uri',
                        required=True,
                        type=str,
                        help='URI')

    parser.add_argument('--database',
                        required=True,
                        type=str,
                        help='Database name')

    parser.add_argument('--update-frequency-seconds',
                        required=False,
                        type=int,
                        default=60,
                        help='Number of seconds before update')

    parser.add_argument('--must-crud',
                        required=False,
                        action='store_true',
                        help='Only display when insert/update/delete occurred')

    parser.add_argument('--log-file-name',
                        required=True,
                        type=str,
                        help='Log file name')
                        
    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')

    parser.add_argument('--show-per-second',
                        required=False,
                        action='store_true',
                        help='Show operations as "per second"')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['updateFrequencySeconds'] = int(args.update_frequency_seconds)
    appConfig['databaseName'] = args.database
    appConfig['logFileName'] = args.log_file_name
    appConfig['showPerSecond'] = args.show_per_second
    appConfig['mustCrud'] = args.must_crud
    
    logAndPrint(appConfig,'---------------------------------------------------------------------------------------')
    for thisKey in appConfig:
        logAndPrint(appConfig,"  config | {} | {}".format(thisKey,appConfig[thisKey]))
    logAndPrint(appConfig,'---------------------------------------------------------------------------------------')

    reportCollectionInfo(appConfig)
    

if __name__ == "__main__":
    main()


