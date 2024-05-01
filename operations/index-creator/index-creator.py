from datetime import datetime
import sys
import pymongo
import time
import threading
import os
import argparse


allDone = False


def deleteLog(appConfig):
    if os.path.exists(appConfig['logFileName']):
        os.remove(appConfig['logFileName'])


def printLog(thisMessage,appConfig):
    with open(appConfig['logFileName'], 'a') as fp:
        fp.write("{}\n".format(thisMessage))
    print("{}".format(thisMessage))


def reportCollectionInfo(appConfig):
    client = pymongo.MongoClient(appConfig['uri'])
    db = client[appConfig['databaseName']]
    
    collStats = db.command("collStats", appConfig['collectionName'])
    
    compressionRatio = collStats['size'] / collStats['storageSize']
    gbDivisor = 1024*1024*1024
    
    printLog("collection statistics | numDocs             = {0:12,d}".format(collStats['count']),appConfig)
    printLog("collection statistics | avgObjSize          = {0:12,d}".format(int(collStats['avgObjSize'])),appConfig)
    printLog("collection statistics | size (GB)           = {0:12,.4f}".format(collStats['size']/gbDivisor),appConfig)
    printLog("collection statistics | storageSize (GB)    = {0:12,.4f} ".format(collStats['storageSize']/gbDivisor),appConfig)
    printLog("collection statistics | compressionRatio    = {0:12,.4f}".format(compressionRatio),appConfig)
    printLog("collection statistics | totalIndexSize (GB) = {0:12,.4f}".format(collStats['totalIndexSize']/gbDivisor),appConfig)
    
    client.close()


def reporter(appConfig):
    global allDone
    
    uri = appConfig['uri']
    numSecondsFeedback = appConfig['updateFrequencySeconds']
    ns = "{}.{}".format(appConfig['databaseName'],appConfig['collectionName'])
    
    startTime = time.time()
    lastTime = time.time()
    nextReportTime = startTime + numSecondsFeedback
    
    client = pymongo.MongoClient(uri)
    
    currentStage = 0
    progressDone = 0
    progressTotal = 0

    
    while not allDone:
        time.sleep(numSecondsFeedback)
        nowTime = time.time()

        # run the query to get the current status
        indexCreateStatus = ''
        with client.admin.aggregate([{"$currentOp": {}},{"$match":{"ns":ns}}]) as cursor:
            for operation in cursor:
                if 'createIndexes' in operation['command']:
                    #print(operation['command'])
                    indexCreateStatus = operation.get('msg','')
                    if 'progress' in operation:
                        progressDone = operation['progress'].get('done',0)
                        progressTotal = operation['progress'].get('total',0)
                    else:
                        progressDone = 0
                        progressTotal = 0
                    
        if ('stage 3' in indexCreateStatus) and (currentStage < 3):
            currentStage = 3
            stageStartTime = time.time()
        elif ('stage 4' in indexCreateStatus) and (currentStage < 4):
            currentStage = 4
            stageStartTime = time.time()
        elif ('stage 5' in indexCreateStatus) and (currentStage < 5):
            currentStage = 5
            stageStartTime = time.time()
        elif ('stage 6' in indexCreateStatus) and (currentStage < 6):
            currentStage = 6
            stageStartTime = time.time()
        elif ('stage 8' in indexCreateStatus) and (currentStage < 8):
            currentStage = 8
            stageStartTime = time.time()

        if progressDone != 0 and progressTotal != 0 and currentStage in [3,4,5,6,8]:
            remainingSeconds = max(int(((nowTime - stageStartTime) * (1 / (progressDone / progressTotal))) - (nowTime - stageStartTime)),0)
        else:
            remainingSeconds = 0
        
        elapsedSeconds = nowTime - startTime
        thisHours, rem = divmod(elapsedSeconds, 3600)
        thisMinutes, thisSeconds = divmod(rem, 60)
        thisHMS = "{:0>2}:{:0>2}:{:05.2f}".format(int(thisHours),int(thisMinutes),thisSeconds)
       
        logTimeStamp = datetime.utcnow().isoformat()[:-3] + 'Z'

        if remainingSeconds == 0:
            printLog("[{}] elapsed {} | {}".format(logTimeStamp,thisHMS,indexCreateStatus),appConfig)
        else:
            printLog("[{}] elapsed {} | {} | stage {} remaining seconds ~{}".format(logTimeStamp,thisHMS,indexCreateStatus,currentStage,remainingSeconds),appConfig)
            
    client.close()


def task_worker(appConfig):
    global allDone

    uri = appConfig['uri']
    databaseName = appConfig['databaseName']
    collectionName = appConfig['collectionName']
    indexName = appConfig['indexName']
    background = appConfig['background']
    unique = appConfig['unique']
    indexKeys = appConfig['indexKeys']
    workers = appConfig['workers']
    dropIndex = appConfig['dropIndex']
    
    indexList = []
    for thisKey in indexKeys.split(','):
        if ':' in thisKey:
            thisKeyField, thisKeyDirection = thisKey.split(':')
            if thisKeyDirection == '-1':
                thisKeyDirectionPymongo = pymongo.DESCENDING
            else:
                thisKeyDirectionPymongo = pymongo.ASCENDING
            indexList.append((thisKeyField,thisKeyDirectionPymongo))
        else:
            thisKeyField = thisKey
            thisKeyDirectionPymongo = pymongo.ASCENDING
            indexList.append((thisKeyField,thisKeyDirectionPymongo))

    client = pymongo.MongoClient(host=uri,appname='indxcrtr')
    db = client[databaseName]
    col = db[collectionName]

    if dropIndex and (indexName in col.index_information()):
        printLog("Dropping index {} on {}.{}".format(indexName,databaseName,collectionName),appConfig)
        col.drop_index(indexName)
    
    # output what we are doing before we do it
    printLog("Creating index {} on {}.{}, unique={}, background={}, workers={}, keys={}".format(indexName,databaseName,collectionName,unique,background,workers,indexList),appConfig)
    
    col.create_index(indexList, name=indexName, background=background, unique=unique, workers=workers)
    
    client.close()
    
    allDone = True


def main():
    parser = argparse.ArgumentParser(description='Index Creator')

    parser.add_argument('--uri',required=True,type=str,help='URI (connection string)')
    parser.add_argument('--workers',required=True,type=int,help='Number of index creation workers')
    parser.add_argument('--database',required=True,type=str,help='Database')
    parser.add_argument('--collection',required=True,type=str,help='Collection')
    parser.add_argument('--update-frequency-seconds',required=False,type=int,default=15,help='Number of seconds between updates')
    parser.add_argument('--index-name',required=True,type=str,help='Index name')
    parser.add_argument('--index-keys',required=True,type=str,help='Index key(s) - comma separated - optional :1 or :-1 for direction')
    parser.add_argument('--log-file-name',required=False,type=str,default='index-creator.log',help='Name of log file')
    parser.add_argument('--background',required=False,action='store_true',help='Create index in the background')
    parser.add_argument('--foreground',required=False,action='store_true',help='Create index in the foreground')
    parser.add_argument('--unique',required=False,action='store_true',help='Create unique index')
    parser.add_argument('--drop-index',required=False,action='store_true',help='Drop the index (if it exists)')
    args = parser.parse_args()
    
    # fail if not --background or --foreground
    if not (args.background or args.foreground):
        print("Must supply either --background or --foreground")
        sys.exit(1)
    
    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['workers'] = int(args.workers)
    appConfig['databaseName'] = args.database
    appConfig['collectionName'] = args.collection
    appConfig['updateFrequencySeconds'] = int(args.update_frequency_seconds)
    appConfig['indexName'] = args.index_name
    appConfig['indexKeys'] = args.index_keys
    appConfig['logFileName'] = args.log_file_name
    appConfig['unique'] = args.unique
    appConfig['background'] = args.background
    appConfig['dropIndex'] = args.drop_index
    
    deleteLog(appConfig)
    
    reportCollectionInfo(appConfig)

    tWorker = threading.Thread(target=task_worker,args=(appConfig,))
    tWorker.start()
    
    tReporter = threading.Thread(target=reporter,args=(appConfig,))
    tReporter.start()
    
    tReporter.join()
    
    reportCollectionInfo(appConfig)
    
    printLog("Created {} with results".format(appConfig['logFileName']),appConfig)


if __name__ == "__main__":
    main()
