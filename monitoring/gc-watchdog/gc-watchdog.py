import datetime
import sys
import json
import pymongo
import time
import os
import argparse
import boto3


def initializeLogFile(appConfig):
    with open(appConfig['logFileName'], mode="w", buffering=1) as logFile:
        logFile.write("")


def logAndPrint(appConfig,string):
    with open(appConfig['logFileName'], mode="a", buffering=1) as logFile:
        logFile.write(string+"\n")
    print(string)


def watchGc(appConfig):
    verboseOutput = appConfig['verbose']
    checkFrequencySeconds = appConfig['checkFrequencySeconds']
    createCloudwatchMetrics = appConfig['createCloudwatchMetrics']
    clusterName = appConfig['clusterName']
    client = pymongo.MongoClient(host=appConfig['uri'],appname='gcwatch')
    watchStartTime = time.time()
    
    # number of seconds between posting metrics to cloudwatch
    cloudwatchPutSeconds = 60
    lastCloudwatchPutTime = time.time()
    activeGcCount = 0
    activeGcTotalSeconds = 0
    activeGcMaxSeconds = 0
    
    if createCloudwatchMetrics:
        # only instantiate client if needed
        cloudWatchClient = boto3.client('cloudwatch')
    
    gcDict = {}

    while True:
        logTimeStamp = datetime.datetime.now(datetime.timezone.utc).isoformat()[:-3] + 'Z'
        
        # mark all as not seen
        for thisNs in gcDict.keys():
            gcDict[thisNs]['active'] = False

        foundGcActivity = False
        elapsedSeconds = (time.time() - watchStartTime)
        with client.admin.aggregate([{"$currentOp": {"allUsers": True, "idleConnections": True}},{"$match": {"desc": "GARBAGE_COLLECTION"}}]) as cursor:
            #if verboseOutput:
            #    logAndPrint(appConfig,"{} | executionTime (seconds) | {:.2f}".format(logTimeStamp,elapsedSeconds))

            thisActiveGcCount = 0
            thisActiveGcTotalSeconds = 0
            thisActiveGcMaxSeconds = 0
            
            for operation in cursor:
                if 'garbageCollection' in operation:
                    cloudwatchDataExists = True
                    foundGcActivity = True
                    thisNs = "{}.{}".format(operation['garbageCollection'].get('databaseName','UNKNOWN-DATABASE'),operation['garbageCollection'].get('collectionName','UNKNOWN-COLLECTION'))

                    # get current values
                    thisActiveGcCount += 1
                    thisActiveGcTotalSeconds += int(operation.get('secs_running',0))
                    if int(operation.get('secs_running',0)) > thisActiveGcMaxSeconds:
                        thisActiveGcMaxSeconds = int(operation.get('secs_running',0))

                    # store if larger than existing
                    activeGcCount = max(activeGcCount,thisActiveGcCount)
                    activeGcTotalSeconds = max(activeGcTotalSeconds,thisActiveGcTotalSeconds)
                    activeGcMaxSeconds = max(activeGcMaxSeconds,thisActiveGcMaxSeconds)
                    
                    if thisNs in gcDict:
                        # already tracking as garbage collecting - check if it finish and started again
                        if operation.get('secs_running',-1) < gcDict[thisNs]['secsRunning']:
                            # finished and started again, output result
                            logAndPrint(appConfig,"{} | GC COMPLETED | {} | {:.2f} | seconds".format(logTimeStamp, thisNs, time.time() - gcDict[thisNs]['startTime']))
                            
                            # reset values
                            gcDict[thisNs]['active'] = True
                            gcDict[thisNs]['startTime'] = time.time()
                            gcDict[thisNs]['secsRunning'] = operation.get('secs_running',999999)
                            
                        else:
                            gcDict[thisNs]['active'] = True
                        
                    else:
                        # first time seen as garbage collecting, add to tracking dictionary and mark as active
                        gcDict[thisNs] = {}
                        gcDict[thisNs]['active'] = True
                        gcDict[thisNs]['startTime'] = time.time()
                        gcDict[thisNs]['secsRunning'] = operation.get('secs_running',999999)
                        logAndPrint(appConfig,"{} | GC STARTED   | {}".format(logTimeStamp, thisNs))
                
                if verboseOutput:
                    logAndPrint(appConfig,"{} | executionTime (seconds) | {:.2f} | {}".format(logTimeStamp,elapsedSeconds,operation))

        # output CW metrics every cloudwatchPutSeconds seconds
        if createCloudwatchMetrics and ((time.time() - lastCloudwatchPutTime) > cloudwatchPutSeconds):
            #logAndPrint(appConfig,"{} | CloudWatch count / maxSecs / totSecs = {} / {} / {}".format(logTimeStamp, activeGcCount, activeGcMaxSeconds, activeGcTotalSeconds))
            
            # log to cloudwatch
            cloudWatchClient.put_metric_data(
                Namespace='CustomDocDB',
                MetricData=[{'MetricName':'GCCount','Dimensions':[{'Name':'Cluster','Value':clusterName}],'Value':activeGcCount,'StorageResolution':60},
                            {'MetricName':'GcMaxSeconds','Dimensions':[{'Name':'Cluster','Value':clusterName}],'Value':activeGcMaxSeconds,'StorageResolution':60},
                            {'MetricName':'GCTotalSeconds','Dimensions':[{'Name':'Cluster','Value':clusterName}],'Value':activeGcTotalSeconds,'StorageResolution':60}])
            
            lastCloudwatchPutTime = time.time()
            activeGcCount = 0
            activeGcTotalSeconds = 0
            activeGcMaxSeconds = 0
            
        if not foundGcActivity:
            if verboseOutput:
                logAndPrint(appConfig,"{} | executionTime (seconds) | {:.2f} | NO GC Activity".format(logTimeStamp,elapsedSeconds))

        for thisNs in list(gcDict.keys()):
            if gcDict[thisNs]['active'] == False:
                # GC completed, output result and remove
                logAndPrint(appConfig,"{} | GC COMPLETED | {} | {:.2f} | seconds".format(logTimeStamp, thisNs, time.time() - gcDict[thisNs]['startTime']))
                gcDict.pop(thisNs)
               
        time.sleep(checkFrequencySeconds)

    client.close()


def main():
    parser = argparse.ArgumentParser(description='DocumentDB GC Watchdog')

    parser.add_argument('--uri',required=True,type=str,help='URI')
    parser.add_argument('--check-frequency-seconds',required=False,type=int,default=5,help='Number of seconds between checks')
    parser.add_argument('--skip-python-version-check',required=False,action='store_true',help='Permit execution on Python 3.6 and prior')
    parser.add_argument('--log-file-name',required=True,type=str,help='Name of log file')
    parser.add_argument('--verbose',required=False,action='store_true',help='Verbose output')
    parser.add_argument('--create-cloudwatch-metrics',required=False,action='store_true',help='Create CloudWatch metrics when garbage collection is active')
    parser.add_argument('--cluster-name',required=False,type=str,help='Name of cluster for CloudWatch metrics')

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    if args.create_cloudwatch_metrics and (args.cluster_name is None):
        sys.exit("\nMust supply --cluster-name when capturing CloudWatch metrics.\n")

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['checkFrequencySeconds'] = int(args.check_frequency_seconds)
    appConfig['logFileName'] = args.log_file_name
    appConfig['verbose'] = args.verbose
    appConfig['createCloudwatchMetrics'] = args.create_cloudwatch_metrics
    appConfig['clusterName'] = args.cluster_name
    
    initializeLogFile(appConfig)
    
    watchGc(appConfig)
    

if __name__ == "__main__":
    main()


