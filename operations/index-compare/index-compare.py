import argparse
import sys
#import json
import pymongo
import os
import warnings


def ensureDirect(uri,appname):
    # make sure we are directly connecting to the server requested, not via replicaSet

    connInfo = {}
    parsedUri = pymongo.uri_parser.parse_uri(uri)

    for thisKey in sorted(parsedUri['options'].keys()):
        if thisKey.lower() not in ['replicaset','readpreference']:
            connInfo[thisKey] = parsedUri['options'][thisKey]

    # make sure we are using directConnection=true
    connInfo['directconnection'] = True

    connInfo['username'] = parsedUri['username']
    connInfo['password'] = parsedUri['password']
    connInfo['host'] = parsedUri['nodelist'][0][0]
    connInfo['port'] = parsedUri['nodelist'][0][1]
    connInfo['appname'] = appname

    if parsedUri.get('database') is not None:
        connInfo['authSource'] = parsedUri['database']

    return connInfo


def getData(appConfig,connectUri,whichServer):
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")
    
    print('connecting to {} server'.format(whichServer))
    client = pymongo.MongoClient(**ensureDirect(connectUri,'indxcomp'))

    return getCollectionStats(appConfig,client)


def getCollectionStats(appConfig,client):
    verbose = appConfig['verbose']
    returnDict = {}
    
    # get databases - filter out admin, config, local, and system
    dbDict = client.admin.command("listDatabases",nameOnly=True,filter={"name":{"$nin":['admin','config','local','system']}})['databases']
    for thisDb in dbDict:
        collCursor = client[thisDb['name']].list_collections()
        for thisColl in collCursor:
            if thisColl.get('type','NOT-FOUND') == 'view':
                # exclude views
                pass
            elif thisColl['name'] in ['system.profile']:
                # exclude certain collections
                pass
            else:
                if verbose:
                    print("  retrieving indexes for {}.{}".format(thisDb['name'],thisColl['name']))
                if thisDb['name'] not in returnDict:
                    returnDict[thisDb['name']] = {}
                
                indexInfo = client[thisDb['name']][thisColl['name']].index_information()

                returnDict[thisDb['name']][thisColl['name']] = indexInfo.copy()
    
    return returnDict


def compareSpecificIndex(appConfig,index1,index2,keyIndex,keyDatabase,keyCollection):
    verbose = appConfig['verbose']
    excludedAttributesList = ['ns','v','textIndexVersion','language_override']
    
    if verbose:
        print("          {} | {}".format(index1,index2))
    # check for keys in index1 but not index2
    for key in sorted(index1.keys()):
        if key not in excludedAttributesList:
            if verbose:
                print("          checking source attribute {}".format(key))
            if key not in index2:
                print("attribute {} on index {} on {}.{} does not exist in target".format(key,keyIndex,keyDatabase,keyCollection))
            else:
                # check if the values differ
                if index1[key] == index2[key]:
                    pass
                else:
                    print("attribute {} on index {} on {}.{} has differing values of source == {} and target == {}".format(key,keyIndex,keyDatabase,keyCollection,index1[key],index2[key]))
        else:
            if verbose:
                print("attribute {} on index {} on {}.{} skipped (excluded from check)".format(key,keyIndex,keyDatabase,keyCollection))

    # check for keys in index2 but not index1
    for key in sorted(index2.keys()):
        if key not in excludedAttributesList:
            if verbose:
                print("          checking target attribute {}".format(key))
            if key not in index1:
                print("attribute {} on index {} on {}.{} does not exist in source".format(key,keyIndex,keyDatabase,keyCollection))
            
    
    
def compareIndexes(appConfig,sourceDict,targetDict):
    verbose = appConfig['verbose']
    
    # compare source to target
    print("")
    print("comparing - source to target")
    for keyDatabase in sorted(sourceDict.keys()):    
        if verbose:
            print("  checking source database {}".format(keyDatabase))
        for keyCollection in sorted(sourceDict[keyDatabase].keys()):
            if verbose:
                print("    checking collection {}".format(keyCollection))
            if keyDatabase in targetDict and keyCollection in targetDict[keyDatabase]:
                for keyIndex in sorted(sourceDict[keyDatabase][keyCollection]):
                    if verbose:
                        print("      checking index {}".format(keyIndex))
                    if keyIndex in targetDict[keyDatabase][keyCollection]:
                        # index exists - compare
                        compareSpecificIndex(appConfig,sourceDict[keyDatabase][keyCollection][keyIndex],targetDict[keyDatabase][keyCollection][keyIndex],keyIndex,keyDatabase,keyCollection)
                    else:
                        print("index {} on {}.{} does not exist in target".format(keyIndex,keyDatabase,keyCollection))
            else:
                print("collection {}.{} does not exist in target".format(keyDatabase,keyCollection))
                
    # compare target to source
    print("")
    print("comparing - target to source")
    for keyDatabase in sorted(targetDict.keys()):    
        if verbose:
            print("  checking target database {}".format(keyDatabase))
        for keyCollection in sorted(targetDict[keyDatabase].keys()):
            if verbose:
                print("    checking collection {}".format(keyCollection))
            if keyDatabase in sourceDict and keyCollection in sourceDict[keyDatabase]:
                for keyIndex in sorted(targetDict[keyDatabase][keyCollection]):
                    if verbose:
                        print("      checking index {}".format(keyIndex))
                    if keyIndex in sourceDict[keyDatabase][keyCollection]:
                        # index exists - skip
                        #compareSpecificIndex(appConfig,sourceDict[keyDatabase][keyCollection][keyIndex],targetDict[keyDatabase][keyCollection][keyIndex],keyIndex,keyDatabase,keyCollection)
                        pass
                    else:
                        print("index {} on {}.{} does not exist in source".format(keyIndex,keyDatabase,keyCollection))
            else:
                print("collection {}.{} does not exist in source".format(keyDatabase,keyCollection))

    print("")
    

def main():
    parser = argparse.ArgumentParser(description='Compare indexes between two DocumentDB or MongoDB servers')
        
    parser.add_argument('--skip-python-version-check',required=False,action='store_true',help='Permit execution on Python 3.6 and prior')
    parser.add_argument('--source-uri',required=True,type=str,help='Connection URI for source')
    parser.add_argument('--target-uri',required=True,type=str,help='Connection URI for target')
    parser.add_argument('--verbose',required=False,action='store_true',help='Verbose output')

    args = parser.parse_args()
    
    # check for minimum Python version
    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    appConfig = {}
    appConfig['sourceUri'] = args.source_uri
    appConfig['targetUri'] = args.target_uri
    appConfig['verbose'] = args.verbose

    sourceDict = getData(appConfig,appConfig['sourceUri'],'source')
    targetDict = getData(appConfig,appConfig['targetUri'],'target')
    
    compareIndexes(appConfig,sourceDict,targetDict)
    

if __name__ == "__main__":
    main()
