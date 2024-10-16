import datetime
import sys
import random
import json
import pymongo
import time
import threading
import os
import multiprocessing as mp
import argparse
import string
import math


def deleteLog(appConfig):
    if os.path.exists(appConfig['logFileName']):
        os.remove(appConfig['logFileName'])

def printLog(thisMessage,appConfig):
    print("{}".format(thisMessage))
    with open(appConfig['logFileName'], 'a') as fp:
        fp.write("{}\n".format(thisMessage))

def setup(appConfig):
    if sys.version_info < (3,7):
        sys.exit('Sorry, Python < 3.7 is not supported')

    databaseName = appConfig['databaseName']
    collectionName = appConfig['collectionName']

    client = pymongo.MongoClient(appConfig['uri'])
    
    # database and collection for compression

    db = client[databaseName]
    adminDb = client['admin']
    col = db[collectionName]
    
    # database and collection for tracking

    tracker_db=client['tracker_db']
    trackerCollectionName = databaseName+'_'+collectionName+'_tracker_col'
    tracker_col=tracker_db[trackerCollectionName]
    
    list_of_collections = tracker_db.list_collection_names()  # Return a list of collections in 'tracker_db'
    print("list_of_collections {}".format(list_of_collections))
    
    if trackerCollectionName in list_of_collections :
    
        # tracker db already has entry for collection

        result = tracker_col.find({}).sort({ "_id" : -1}).limit(1)   
               
        for lastEntry in result :
            numExistingDocuments = lastEntry["numExistingDocuments"]
            maxObjectIdToTouch = lastEntry["maxObjectIdToTouch"]
            lastScannedObjectId = lastEntry["lastScannedObjectId"]
            numDocumentsUpdated = lastEntry["numDocumentsUpdated"]
            print("Found existing record: {}".format(str(lastEntry))) 

    else :      
    
        # create first entry in tracker db  for collection
        result = col.find({},{ "_id" :1}).sort({ "_id" :-1}).limit(1)
        
        for id in result :
            print("result {}".format(result))
            maxObjectIdToTouch = id["_id"]
            
        lastScannedObjectId = 0
        numDocumentsUpdated = 0
        numExistingDocuments = col.estimated_document_count()
            
        first_entry = {
            "collection_name": appConfig['collectionName'],
            "lastScannedObjectId" : lastScannedObjectId,
            "ts": datetime.datetime.now(tz=datetime.timezone.utc),
            "maxObjectIdToTouch" : maxObjectIdToTouch,
            "numExistingDocuments" : numExistingDocuments,
            "numDocumentsUpdated" : numDocumentsUpdated
            # scan fields in future, for now we use  _id
            }   
        tracker_col.insert_one(first_entry)

        printLog("create first entry in tracker db  for collection {}".format(first_entry),appConfig)
            
    client.close()

    returnData = {}
    returnData["numExistingDocuments"] = numExistingDocuments
    returnData["maxObjectIdToTouch"] = maxObjectIdToTouch
    returnData["lastScannedObjectId"] = lastScannedObjectId
    returnData["numDocumentsUpdated"] = numDocumentsUpdated
    
    return returnData

def task_worker(threadNum,perfQ,appConfig):
    maxObjectIdToTouch = appConfig['maxObjectIdToTouch']
    lastScannedObjectId = appConfig['lastScannedObjectId']
    numInsertProcesses = appConfig['numInsertProcesses']

    numExistingDocuments = appConfig["numExistingDocuments"]
    maxObjectIdToTouch = appConfig["maxObjectIdToTouch"]
    lastScannedObjectId = appConfig["lastScannedObjectId"]
    numDocumentsUpdated = appConfig["numDocumentsUpdated"]

    client = pymongo.MongoClient(appConfig['uri'])
    
    myDatabaseName = appConfig['databaseName']
    db = client[myDatabaseName]
    myCollectionName = appConfig['collectionName']
    col = db[myCollectionName]
    tracker_db=client['tracker_db']
    trackerCollectionName = myDatabaseName+'_'+myCollectionName+'_tracker_col'
    tracker_col=tracker_db[trackerCollectionName]
    
    allDone = False
    tempLastScannedObjectId = lastScannedObjectId
    
    while not allDone:

        #start and go through all the docs using _id 
        
        if lastScannedObjectId != 0 :
            batch =  col.find({"_id" : { "$gt" : lastScannedObjectId   }},{ "_id" :1}).sort({"_id" :1}).limit(appConfig['batchSize'])
        else :
            batch =  col.find({},{ "_id" :1}).sort({ "_id" :1}).limit(appConfig['batchSize'])
            
        batch_count = 0
        updateList = []

        for id in batch : 
            if id["_id"]<=maxObjectIdToTouch:
                # print("found id {} lesser than maxObjectIdToTouch {}.".format(str(id["_id"]),str(maxObjectIdToTouch)))
                updateList.append(pymongo.UpdateOne({ "_id" : id["_id"] } , { "$set": { appConfig['updateField']: 1 } } ))
                tempLastScannedObjectId = id["_id"]
                batch_count = batch_count + 1
            else:
                allDone = True
                print("found id {} higher than maxObjectIdToTouch {}. all done .stopping)".format(str(id["_id"]),str(maxObjectIdToTouch)))
                break
            
        if  batch_count > 0 :
            result = col.bulk_write(updateList)
            numDocumentsUpdated = numDocumentsUpdated + batch_count
            
            tracker_entry = {
                "collection_name": appConfig['collectionName'],
                "lastScannedObjectId" : tempLastScannedObjectId,
                "date": datetime.datetime.now(tz=datetime.timezone.utc),
                "maxObjectIdToTouch" : maxObjectIdToTouch,
                "numExistingDocuments" : numExistingDocuments,
                "numDocumentsUpdated" : numDocumentsUpdated
                # scan fields in future, for now we use  _id
                }   
            tracker_col.insert_one(tracker_entry)
            
            printLog( " Last updates applied : {}".format(str(tracker_entry)),appConfig)
            
            lastScannedObjectId = tempLastScannedObjectId
        
            printLog("sleeping for {} seconds".format(appConfig['waitPeriod']),appConfig)
            time.sleep(appConfig['waitPeriod'])
        else :
            print("No updates in batch")
            allDone = True
            break
                
    client.close()

def main():
    parser = argparse.ArgumentParser(description='Data Generator')
    parser.add_argument('--uri',required=True,type=str,help='URI (connection string)')
    parser.add_argument('--database',required=True,type=str,help='Database')
    parser.add_argument('--collection',required=True,type=str,help='Collection')
    parser.add_argument('--file-name',required=False,type=str,default='compressor',help='Starting name of the created log files')
    parser.add_argument('--update-field',required=False,type=str,default='6nh63',help='Field used for updating an existing document. This should not conflict with any fieldname you are already using ')
    parser.add_argument('--wait-period',required=False,type=int,default=60,help='Number of seconds to wait between each batch')
    parser.add_argument('--batch-size',required=False,type=int,default=5000,help='Number of documents to update in a single batch')

    args = parser.parse_args()
    
    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['numInsertProcesses'] = 1 #int(args.processes)
    appConfig['databaseName'] = args.database
    appConfig['collectionName'] = args.collection
    appConfig['updateField'] = args.update_field
    appConfig['batchSize'] = int(args.batch_size)
    appConfig['waitPeriod'] = int(args.wait_period)
    appConfig['logFileName'] = "{}.log".format(args.file_name)

    setUpdata = setup(appConfig)
    
    appConfig['numExistingDocuments'] = setUpdata["numExistingDocuments"]  
    appConfig['maxObjectIdToTouch'] = setUpdata["maxObjectIdToTouch"]  
    appConfig['lastScannedObjectId'] = setUpdata["lastScannedObjectId"]     
    appConfig['numDocumentsUpdated'] = setUpdata["numDocumentsUpdated"]   

    deleteLog(appConfig)
    
    printLog('---------------------------------------------------------------------------------------',appConfig)
    for thisKey in sorted(appConfig):
        if (thisKey == 'uri'):
            thisUri = appConfig[thisKey]
            thisParsedUri = pymongo.uri_parser.parse_uri(thisUri)
            thisUsername = thisParsedUri['username']
            thisPassword = thisParsedUri['password']
            thisUri = thisUri.replace(thisUsername,'<USERNAME>')
            thisUri = thisUri.replace(thisPassword,'<PASSWORD>')
            printLog("  config | {} | {}".format(thisKey,thisUri),appConfig)
        else:
            printLog("  config | {} | {}".format(thisKey,appConfig[thisKey]),appConfig)
    printLog('---------------------------------------------------------------------------------------',appConfig)
    
    mp.set_start_method('spawn')
    q = mp.Manager().Queue()

    processList = []
    for loop in range(appConfig['numInsertProcesses']):
        p = mp.Process(target=task_worker,args=(loop,q,appConfig))
        processList.append(p)
    for process in processList:
        process.start()
        
    for process in processList:
        process.join()
        
    printLog("Created {}  with results".format(appConfig['logFileName']),appConfig)

if __name__ == "__main__":
    main()