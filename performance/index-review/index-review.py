import argparse
from datetime import datetime, timedelta
import sys
import json
import pymongo
import time
import os
from collections import OrderedDict


def getData(appConfig):
    serverOpCounters = {}
    serverMetricsDocument = {}
    collectionStats = OrderedDict()
    serverUptime = 0
    serverHost = ''
    serverLocalTime = ''

    print('connecting to server')
    client = pymongo.MongoClient(appConfig['connectionString'])

    serverOpCounters = client.admin.command("serverStatus")['opcounters']
    serverMetricsDocument = client.admin.command("serverStatus")['metrics']['document']
    serverUptime = client.admin.command("serverStatus")['uptime']
    serverHost = client.admin.command("serverStatus")['host']
    serverLocalTime = client.admin.command("serverStatus")['localTime']
    collectionStats = getCollectionStats(client)

    client.close()

    # log what we found
    finalDict = OrderedDict()
    finalDict['serverAlias'] = appConfig['serverAlias']
    finalDict['start'] = {}
    finalDict['start']['opcounters'] = serverOpCounters
    finalDict['start']['docmetrics'] = serverMetricsDocument
    finalDict['start']['uptime'] = serverUptime
    finalDict['start']['host'] = serverHost
    finalDict['start']['localtime'] = serverLocalTime
    finalDict['start']['collstats'] = collectionStats

    # log output to file
    logTimeStamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    logFileName = "{}-{}-index-review.json".format(appConfig['serverAlias'],logTimeStamp)
    with open(logFileName, 'w') as fp:
        json.dump(finalDict, fp, indent=4, default=str)
        
    return logFileName


def getCollectionStats(client):
    returnDict = OrderedDict()
    
    # get databases - filter out admin, config, local, and system
    dbDict = client.admin.command("listDatabases",nameOnly=True,filter={"name":{"$nin":['admin','config','local','system']}})['databases']
    for thisDb in dbDict:
        #print(thisDb)
        collCursor = client[thisDb['name']].list_collections()
        for thisColl in collCursor:
            #print(thisColl)
            if thisColl['type'] == 'view':
                # exclude views
                pass
            elif thisColl['name'] in ['system.profile']:
                # exclude certain collections
                pass
            else:
                #collStats = client[thisDb['name']].command("collstats",thisColl['name'])['wiredTiger']['cursor']
                print("{}.{}".format(thisDb['name'],thisColl['name']))
                collStats = client[thisDb['name']].command("collStats",thisColl['name'])
                if thisDb['name'] not in returnDict:
                    returnDict[thisDb['name']] = {}
                returnDict[thisDb['name']][thisColl['name']] = collStats.copy()
                
                # get index info
                indexInfo = list(client[thisDb['name']][thisColl['name']].aggregate([{"$indexStats":{}}]))
                
                # put keys into a proper list to maintain order
                for thisIndex in indexInfo:
                    keyAsList = []
                    keyAsString = ""
                    for thisKey in thisIndex['key']:
                        keyAsList.append([thisKey,thisIndex['key'][thisKey]])
                        keyAsString += "{}||{}||".format(thisKey,thisIndex['key'][thisKey])
                    thisIndex['keyAsList'] = keyAsList.copy()
                    thisIndex['keyAsString'] = keyAsString
                    
                returnDict[thisDb['name']][thisColl['name']]['indexInfo'] = indexInfo.copy()
    
    return returnDict
    
    
def evalIndexes(appConfig):
    print("loading first file {}".format(appConfig['files'][0]))
    with open(appConfig['files'][0], 'r') as index_file:
        idxDict = json.load(index_file, object_pairs_hook=OrderedDict)

    # load additional files
    addlIdxDictList = []
    addlIdxCount = 0
    for filePtr in range(1,len(appConfig['files'])):
        print("  loading additional file {}".format(appConfig['files'][filePtr]))
        with open(appConfig['files'][filePtr], 'r') as index_file:
            addlIdxDictList.append(json.load(index_file, object_pairs_hook=OrderedDict))
            addlIdxCount += 1

    outFile1 = open(appConfig['serverAlias']+'-collections.csv','wt')
    outFile1.write("{},{},{},{},{},{},{},{}\n".format('database','collection','doc-count','average-doc-size','size-GB','storageSize-GB','num-indexes','indexSize-GB'))

    outFile2 = open(appConfig['serverAlias']+'-indexes.csv','wt')
    outFile2.write("{},{},{},{},{},{},{},{},{},{},{},{}\n".format('database','collection','doc-count','average-doc-size','size-GB','storageSize-GB','num-indexes','indexSize-GB','index-name','index-accesses-total','index-accesses-secondary','redundant','covered-by'))

    # for each database
    for thisDb in idxDict["start"]["collstats"]:
        print("  database {}".format(thisDb))

        # for each collection
        for thisColl in idxDict["start"]["collstats"][thisDb]:
            printedCollection = False
            thisCollInfo = idxDict["start"]["collstats"][thisDb][thisColl]
            bToGb = 1024*1024*1024

            outFile1.write("{},{},{},{},{:8.2f},{:8.2f},{},{:8.2f}\n".format(thisDb,thisColl,thisCollInfo['count'],thisCollInfo['avgObjSize'],thisCollInfo['size']/bToGb,thisCollInfo['storageSize']/bToGb,thisCollInfo['nindexes'],thisCollInfo['totalIndexSize']/bToGb))
            
            # for each index
            for thisIdx in idxDict["start"]["collstats"][thisDb][thisColl]["indexInfo"]:
                if thisIdx["name"] in ["_id","_id_"]:
                    continue
                    
                # check extra servers for non-usage
                numXtraOps = 0
                if addlIdxCount > 0:
                    for n in range(0,addlIdxCount):
                        for xtraIdx in addlIdxDictList[n]["start"]["collstats"][thisDb][thisColl]["indexInfo"]:
                            if xtraIdx["name"] == thisIdx["name"]:
                                numXtraOps += xtraIdx["accesses"]["ops"]

                # check index for non-usage (all servers)
                if (thisIdx["accesses"]["ops"]+numXtraOps == 0):
                    if not printedCollection:
                        printedCollection = True
                        print("    collection {}".format(thisColl))
                    print("        index {} | has never been used".format(thisIdx["name"]))

                # check index for redundancy
                redundantList = checkIfRedundant(thisIdx["name"],thisIdx["keyAsString"],idxDict["start"]["collstats"][thisDb][thisColl]["indexInfo"])
                isRedundant = "No"
                if len(redundantList) > 0:
                    if not printedCollection:
                        printedCollection = True
                        print("    collection {}".format(thisColl))
                    print("        index {} | is redundant and covered by the following indexes : {}".format(thisIdx["name"],redundantList))
                    isRedundant = "Yes"

                # output details
                #with open('output.log', 'a') as fpDet:
                #    fpDet.write("{:40s} {:40s} {:40s} {:12d} {:12d}\n".format(thisDb,thisColl,thisIdx["name"],thisIdx["accesses"]["ops"],numXtraOps))

                outFile2.write("{},{},{},{},{:8.2f},{:8.2f},{},{:8.2f},{},{},{},{},{}\n".format(thisDb,thisColl,thisCollInfo['count'],thisCollInfo['avgObjSize'],
                  thisCollInfo['size']/bToGb,thisCollInfo['storageSize']/bToGb,thisCollInfo['nindexes'],thisCollInfo['indexSizes'][thisIdx["name"]]/bToGb,thisIdx["name"],
                  thisIdx["accesses"]["ops"]+numXtraOps,numXtraOps,isRedundant,redundantList))

    outFile1.close()
    outFile2.close()


def checkIfRedundant(idxName,idxKeyAsString,indexList):
    returnList = []
    for thisIdx in indexList:
        if thisIdx["name"] in ["_id","_id_"]:
            continue
        if thisIdx["name"] == idxName:
            continue
        if thisIdx["keyAsString"].startswith(idxKeyAsString):
            returnList.append(thisIdx["name"])
    return returnList


def checkReplicaSet(appConfig):
    print('connecting to server')
    client = pymongo.MongoClient(appConfig['connectionString'])

    rsStatus = client.admin.command("replSetGetStatus")
    print("  rs.status() = {}".format(rsStatus))

    client.close()


def main():
    # v0
    #    * single server
    #    * add python 3.7 check
    #    * save full set of data collected to filesystem
    #    * find unused and redundant indexes
    #    * add proper argument system
    
    # v1
    #    * allow override of minimum Python version
    #    * create CSV files - one for collections, one for indexes
    #    filter by database and/or collection by name
    #    filter by database and/or collection by regular expression
    #    report server uptime with suggestions
    #    clean up JSON - remove "start"
    #    check for same index twice
    #    ensure compatibility with MongoDB 3.2+
    
    # v2
    #    multi-server (via command line arg)
    #    compare host in JSON, look for duplicates
    #    unit testing
    
    # v3
    #    replicaSet discovery
    
    # v4
    #    sharding support?
    
    # v5
    #    diff across multiple runs, find unused
    
    parser = argparse.ArgumentParser(description='Check for redundant and unused indexes.')
        
    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')
    
    parser.add_argument('--uri',
                        required=False,
                        type=str,
                        help='MongoDB Connection URI')

    parser.add_argument('--server-alias',
                        required=True,
                        type=str,
                        help='Alias for server, used to name output file')

    parser.add_argument('--files',
                        required=False,
                        type=str,
                        help='Comma separated list of existing output files to review')

    args = parser.parse_args()
    
    # check for minimum Python version
    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    if args.uri is None and args.files is None:
        parser.error("must provide either --uri or --files")

    if args.uri is not None and args.files is not None:
        parser.error("cannot provide both --uri and --files")

    appConfig = {}
    appConfig['connectionString'] = args.uri
    appConfig['serverAlias'] = args.server_alias
    
    #checkReplicaSet(appConfig)

    if (args.uri is not None):
        # pulling from a server
        outfName = getData(appConfig)
        appConfig['files'] = [outfName]

    else:
        # comparing using 1 or more output files from prior runs
        appConfig['files'] = args.files.split(',')

    evalIndexes(appConfig)


if __name__ == "__main__":
    main()
