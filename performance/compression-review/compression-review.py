import argparse
import datetime as dt
import sys
import json
import pymongo
import time
import lz4.block
import bz2
import lzma
import zstandard as zstd
import zlib


def createDictionary(appConfig, databaseName, collectionName, client):
    dictionarySampleSize = appConfig['dictionarySampleSize']
    dictionarySize = appConfig['dictionarySize']

    col = client[databaseName][collectionName]

    print("creating dictionary for {}.{} of {:d} bytes using {:d} samples".format(databaseName,collectionName,dictionarySize,dictionarySampleSize))
    dictTrainingDocs = []
    dictSampleDocs = col.aggregate([{"$sample":{"size":dictionarySampleSize}}])
    for thisDoc in dictSampleDocs:
        docAsString = json.dumps(thisDoc,default=str)
        docAsBytes = str.encode(docAsString)
        dictTrainingDocs.append(docAsBytes)
    dict_data = zstd.train_dictionary(dictionarySize,dictTrainingDocs)

    return dict_data


def getData(appConfig):
    print('connecting to server')
    client = pymongo.MongoClient(host=appConfig['uri'],appname='comprevw')

    compressor = appConfig['compressor']
    sampleSize = appConfig['sampleSize']

    # log output to file
    logTimeStamp = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d%H%M%S')
    logFileName = "{}-{}-compression-review.csv".format(appConfig['serverAlias'],logTimeStamp)
    logFileHandle = open(logFileName, "w")

    # output miscellaneos parameters to csv
    logFileHandle.write("{},{},{},{}\n".format('compressor','docsSampled','dictDocsSampled','dictBytes'))
    logFileHandle.write("{},{:d},{:d},{:d}\n".format(compressor,sampleSize,appConfig['dictionarySampleSize'],appConfig['dictionarySize']))
    logFileHandle.write("\n")

    # output header to csv
    logFileHandle.write("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n".format('dbName','collName','numDocs','avgDocSize','sizeGB','storageGB','compRatio','compEnabled','minSample','maxSample','avgSample','minComp','maxComp','avgComp','compRatio','exceptions','compTime(ms)'))

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

                if collStats['count'] == 0:
                    # exclude collections with no documents
                    continue

                collectionCompressionRatio = collStats['size'] / collStats['storageSize']
                gbDivisor = 1024*1024*1024
                collectionCount = collStats['count']
                collectionAvgObjSize = int(collStats.get('avgObjSize',0))
                collectionSizeGB = collStats['size']/gbDivisor
                collectionStorageSizeGB = collStats['storageSize']/gbDivisor
                # check if compression is enabled
                compressionInfo = collStats.get('compression',{'enable':False,'threshold':-1})
                compressionEnabled = compressionInfo.get('enable',False)
                compressionThreshold = compressionInfo.get('threshold',0)
                if compressionEnabled:
                    #compressionEnabledString = 'Y'
                    compCsvString = "{}/{}".format('Y',compressionThreshold)
                else:
                    #compressionEnabledString = 'N'
                    compCsvString = ""


                numExceptions = 0
                minDocBytes = 999999999
                maxDocBytes = 0
                totDocs = 0
                totDocBytes = 0
                minLz4Bytes = 999999999
                maxLz4Bytes = 0
                totLz4Bytes = 0
                totTimeMs = 0
                totTimeNs = 0

                # build the dictionary if needed (and there are enough documents)
                if compressor in ['lz4-fast-dict','lz4-high-dict','zstd-1-dict','zstd-3-dict','zstd-5-dict']:
                    if collectionCount >= 100:
                        zstdDict = createDictionary(appConfig, thisDbName, thisCollName, client)

                # instantiate the compressor for zstandard (it doesn't support 1-shot compress)
                if compressor == 'zstd-1' or (compressor == 'zstd-1-dict' and collectionCount < 100):
                    zstdCompressor = zstd.ZstdCompressor(level=1,dict_data=None)
                elif compressor == 'zstd-3' or (compressor == 'zstd-3-dict' and collectionCount < 100):
                    zstdCompressor = zstd.ZstdCompressor(level=3,dict_data=None)
                elif compressor == 'zstd-5' or (compressor == 'zstd-5-dict' and collectionCount < 100):
                    zstdCompressor = zstd.ZstdCompressor(level=5,dict_data=None)
                elif compressor == 'zstd-1-dict':
                    zstdCompressor = zstd.ZstdCompressor(level=1,dict_data=zstdDict)
                elif compressor == 'zstd-3-dict':
                    zstdCompressor = zstd.ZstdCompressor(level=3,dict_data=zstdDict)
                elif compressor == 'zstd-5-dict':
                    zstdCompressor = zstd.ZstdCompressor(level=5,dict_data=zstdDict)

                try:
                    sampleDocs = client[thisDbName][thisCollName].aggregate([{"$sample":{"size":sampleSize}}])
                    for thisDoc in sampleDocs:
                        totDocs += 1
                        docAsString = json.dumps(thisDoc,default=str)
                        docBytes = len(docAsString)
                        totDocBytes += docBytes
                        if (docBytes < minDocBytes):
                            minDocBytes = docBytes
                        if (docBytes > maxDocBytes):
                            maxDocBytes = docBytes

                        startTimeMs = time.time()
                        startTimeNs = time.time_ns()

                        # compress it
                        if compressor == 'lz4-fast' or (compressor == 'lz4-fast-dict' and collectionCount < 100):
                            compressed = lz4.block.compress(docAsString.encode(),mode='fast',acceleration=1)
                        elif compressor == 'lz4-high' or (compressor == 'lz4-high-dict' and collectionCount < 100):
                            compressed = lz4.block.compress(docAsString.encode(),mode='high_compression',compression=1)
                        elif compressor == 'lz4-fast-dict':
                            compressed = lz4.block.compress(docAsString.encode(),mode='fast',acceleration=1,dict=zstdDict.as_bytes())
                        elif compressor == 'lz4-high-dict':
                            compressed = lz4.block.compress(docAsString.encode(),mode='high_compression',compression=1,dict=zstdDict.as_bytes())
                        elif compressor in ['zstd-1','zstd-3','zstd-5','zstd-1-dict','zstd-3-dict','zstd-5-dict']:
                            compressed = zstdCompressor.compress(docAsString.encode())
                        elif compressor == 'bz2-1':
                            compressed = bz2.compress(docAsString.encode(),compresslevel=1)
                        elif compressor == 'lzma-0':
                            compressed = lzma.compress(docAsString.encode(),format=lzma.FORMAT_XZ,preset=0)
                        elif compressor == 'zlib-1':
                            compressed = zlib.compress(docAsString.encode(),level=1)
                        else:
                            print('Unknown compressor | {}'.format('compressor'))
                            sys.exit(1)

                        totTimeMs += time.time() - startTimeMs
                        totTimeNs += time.time_ns() - startTimeNs

                        lz4Bytes = len(compressed)
                        totLz4Bytes += lz4Bytes
                        if (lz4Bytes < minLz4Bytes):
                            minLz4Bytes = lz4Bytes
                        if (lz4Bytes > maxLz4Bytes):
                            maxLz4Bytes = lz4Bytes

                except:
                    numExceptions += 1

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

                logFileHandle.write("{},{},{:d},{:d},{:.4f},{:.4f},{:.4f},{},{:d},{:d},{:d},{:d},{:d},{:d},{:.4f},{:d},{:.4f}\n".format(thisDb['name'],thisColl['name'],collectionCount,
                    collectionAvgObjSize,collectionSizeGB,collectionStorageSizeGB,collectionCompressionRatio,compCsvString,minDocBytes,maxDocBytes,avgDocBytes,minLz4Bytes,maxLz4Bytes,avgLz4Bytes,lz4Ratio,numExceptions,totTimeNs/1000000))


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

    parser.add_argument('--compressor',
                        required=False,
                        choices=['lz4-fast','lz4-high','lz4-fast-dict','lz4-high-dict','zstd-1','zstd-3','zstd-5','zstd-1-dict','zstd-3-dict','zstd-5-dict','bz2-1','lzma-0','zlib-1'],
                        type=str,
                        default='lz4-fast',
                        help='Compressor')
    
    parser.add_argument('--dictionary-sample-size',
                        required=False,
                        type=int,
                        default=100,
                        help='Number of documents to sample for dictionary creation')
    
    parser.add_argument('--dictionary-size',
                        required=False,
                        type=int,
                        default=4096,
                        help='Size of dictionary (bytes)')

    args = parser.parse_args()
    
    # check for minimum Python version
    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['serverAlias'] = args.server_alias
    appConfig['sampleSize'] = int(args.sample_size)
    appConfig['compressor'] = args.compressor
    appConfig['dictionarySampleSize'] = int(args.dictionary_sample_size)
    appConfig['dictionarySize'] = int(args.dictionary_size)
    
    getData(appConfig)


if __name__ == "__main__":
    main()
