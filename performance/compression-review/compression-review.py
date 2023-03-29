import argparse
from datetime import datetime, timedelta
import sys
import json
import pymongo
import time
import os
import lz4.frame


def getData(appConfig):
    print('connecting to server')
    client = pymongo.MongoClient(appConfig['uri'])
    sampleSize = appConfig['sampleSize']

    # log output to file
    logTimeStamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
    logFileName = "{}-{}-compression-review.csv".format(appConfig['serverAlias'],logTimeStamp)
    logFileHandle = open(logFileName, "w")

    logFileHandle.write("{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format('dbName','collName','numDocs','avgDocSize','sizeGB','storageGB','compRatio','minSample','maxSample','avgSample','minLz4','maxLz4','avgLz4','lz4Ratio'))

    # get databases - filter out admin, config, local, and system
    dbDict = client.admin.command("listDatabases",nameOnly=True,filter={"name":{"$nin":['admin','config','local','system']}})['databases']
    for thisDb in dbDict:
        thisDbName = thisDb['name']
        collCursor = client[thisDbName].list_collections()
        for thisColl in collCursor:
            thisCollName = thisColl['name']
            if thisColl['type'] == 'view':
                # exclude views
                pass
            elif thisCollName in ['system.profile']:
                # exclude certain collections
                pass
            else:
                # get the collection stats
                print("analyzing collection {}.{}".format(thisDbName,thisCollName))
                collStats = client[thisDbName].command("collStats",thisCollName)

                collectionCompressionRatio = collStats['size'] / collStats['storageSize']
                gbDivisor = 1024*1024*1024
                collectionCount = collStats['count']
                collectionAvgObjSize = int(collStats['avgObjSize']) 
                collectionSizeGB = collStats['size']/gbDivisor
                collectionStorageSizeGB = collStats['storageSize']/gbDivisor

                sampleDocs = client[thisDbName][thisCollName].aggregate([{"$sample":{"size":sampleSize}}])
                minDocBytes = 999999999
                maxDocBytes = 0
                totDocs = 0
                totDocBytes = 0
                minLz4Bytes = 999999999
                maxLz4Bytes = 0
                totLz4Bytes = 0
                for thisDoc in sampleDocs:
                    totDocs += 1
                    docAsString = str(thisDoc)
                    docBytes = len(docAsString)
                    totDocBytes += docBytes
                    if (docBytes < minDocBytes):
                        minDocBytes = docBytes
                    if (docBytes > maxDocBytes):
                        maxDocBytes = docBytes

                    # compress it
                    compressed = lz4.frame.compress(docAsString.encode())
                    lz4Bytes = len(compressed)
                    totLz4Bytes += lz4Bytes
                    if (lz4Bytes < minLz4Bytes):
                        minLz4Bytes = lz4Bytes
                    if (lz4Bytes > maxLz4Bytes):
                        maxLz4Bytes = lz4Bytes


                if (totDocs == 0):
                    avgDocBytes = 0
                    minDocBytes = 0
                    maxDocBytes = 0
                    avgLz4Bytes = 0
                    minLz4Bytes = 0
                    maxLz4Bytes = 0
                    lz4Ratio = 0.0
                else:
                    avgDocBytes = int(totDocBytes / totDocs)
                    avgLz4Bytes = int(totLz4Bytes / totDocs)
                    lz4Ratio = collectionAvgObjSize / avgLz4Bytes

                logFileHandle.write("{},{},{:d},{:d},{:.4f},{:.4f},{:.4f},{:d},{:d},{:d},{:d},{:d},{:d},{:.4f}\n".format(thisDb['name'],thisColl['name'],collectionCount,
                    collectionAvgObjSize,collectionSizeGB,collectionStorageSizeGB,collectionCompressionRatio,minDocBytes,maxDocBytes,avgDocBytes,minLz4Bytes,maxLz4Bytes,avgLz4Bytes,lz4Ratio))


    logFileHandle.close() 
    client.close()
    
    
def main():
    parser = argparse.ArgumentParser(description='Check compressibility of collections')
        
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

    parser.add_argument('--sample-size',
                        required=False,
                        type=int,
                        default=1000,
                        help='Number of documents to sample in each collection, default 1000')

    args = parser.parse_args()
    
    # check for minimum Python version
    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['serverAlias'] = args.server_alias
    appConfig['sampleSize'] = int(args.sample_size)
    
    getData(appConfig)


if __name__ == "__main__":
    main()
