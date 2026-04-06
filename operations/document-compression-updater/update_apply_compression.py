import datetime
import sys
import json
import pymongo
import time
import os
import multiprocessing as mp
import argparse
import math

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds between retry attempts

def deleteLog(appConfig):
    if os.path.exists(appConfig['logFileName']):
        os.remove(appConfig['logFileName'])

def printLog(thisMessage, appConfig):
    print("{}".format(thisMessage))
    with open(appConfig['logFileName'], 'a') as fp:
        fp.write("{}\n".format(thisMessage))

def get_mongo_client(uri, appConfig=None):
    """Create a MongoClient, retrying up to MAX_RETRIES times on failure."""
    def log(msg):
        if appConfig:
            printLog(msg, appConfig)
        else:
            print(msg)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client = pymongo.MongoClient(host=uri, appname='compupd', serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            return client
        except pymongo.errors.PyMongoError as e:
            if attempt < MAX_RETRIES:
                log("Connection attempt {} failed: {}. Retrying in {} seconds...".format(attempt, e, RETRY_DELAY))
                time.sleep(RETRY_DELAY)
            else:
                raise

def validate_connection(appConfig):
    """Validate that the URI is reachable and that the target database/collection exist."""
    try:
        client = get_mongo_client(appConfig['uri'])
    except pymongo.errors.PyMongoError as e:
        sys.exit("Error: Unable to connect to DocumentDB: {}".format(e))

    try:
        db_names = client.list_database_names()
        if appConfig['databaseName'] not in db_names:
            sys.exit("Error: Database '{}' does not exist.".format(appConfig['databaseName']))

        col_names = client[appConfig['databaseName']].list_collection_names()
        if appConfig['collectionName'] not in col_names:
            sys.exit("Error: Collection '{}' does not exist in database '{}'.".format(
                appConfig['collectionName'], appConfig['databaseName']))
    finally:
        client.close()

def setup(appConfig):
    if sys.version_info < (3, 7):
        sys.exit('Sorry, Python < 3.7 is not supported')

    databaseName = appConfig['databaseName']
    collectionName = appConfig['collectionName']

    try:
        client = get_mongo_client(appConfig['uri'])
    except pymongo.errors.PyMongoError as e:
        sys.exit("Error connecting during setup: {}".format(e))

    db = client[databaseName]
    col = db[collectionName]

    tracker_db = client['tracker_db']
    trackerCollectionName = databaseName + '_' + collectionName + '_tracker_col'
    tracker_col = tracker_db[trackerCollectionName]

    list_of_collections = tracker_db.list_collection_names()
    printLog("list_of_collections {}".format(list_of_collections), appConfig)

    try:
        if trackerCollectionName in list_of_collections:
            result = tracker_col.find({}).sort([("_id", -1)]).limit(1)
            for lastEntry in result:
                numExistingDocuments = lastEntry["numExistingDocuments"]
                maxObjectIdToTouch = lastEntry["maxObjectIdToTouch"]
                lastScannedObjectId = lastEntry["lastScannedObjectId"]
                numDocumentsUpdated = lastEntry["numDocumentsUpdated"]
                printLog("Found existing record: {}".format(str(lastEntry)), appConfig)

        else:
            result = col.find({}, {"_id": 1}).sort([("_id", -1)]).limit(1)

            maxObjectIdToTouch = None
            for doc in result:
                maxObjectIdToTouch = doc["_id"]

            if maxObjectIdToTouch is None:
                sys.exit("Error: Collection '{}' is empty, nothing to compress.".format(collectionName))

            lastScannedObjectId = 0
            numDocumentsUpdated = 0
            numExistingDocuments = col.estimated_document_count()

            first_entry = {
                "collection_name": appConfig['collectionName'],
                "lastScannedObjectId": lastScannedObjectId,
                "ts": datetime.datetime.now(tz=datetime.timezone.utc),
                "maxObjectIdToTouch": maxObjectIdToTouch,
                "numExistingDocuments": numExistingDocuments,
                "numDocumentsUpdated": numDocumentsUpdated
            }
            tracker_col.insert_one(first_entry)
            printLog("create first entry in tracker db for collection {}".format(first_entry), appConfig)

    except pymongo.errors.PyMongoError as e:
        sys.exit("Error during setup: {}".format(e))
    finally:
        client.close()

    return {
        "numExistingDocuments": numExistingDocuments,
        "maxObjectIdToTouch": maxObjectIdToTouch,
        "lastScannedObjectId": lastScannedObjectId,
        "numDocumentsUpdated": numDocumentsUpdated
    }

def task_worker(threadNum, appConfig):
    numExistingDocuments = appConfig["numExistingDocuments"]
    maxObjectIdToTouch = appConfig["maxObjectIdToTouch"]
    lastScannedObjectId = appConfig["lastScannedObjectId"]
    numDocumentsUpdated = appConfig["numDocumentsUpdated"]

    myDatabaseName = appConfig['databaseName']
    myCollectionName = appConfig['collectionName']
    trackerCollectionName = myDatabaseName + '_' + myCollectionName + '_tracker_col'

    try:
        client = get_mongo_client(appConfig['uri'])
    except pymongo.errors.PyMongoError as e:
        printLog("Fatal: could not connect in worker: {}".format(e), appConfig)
        return

    db = client[myDatabaseName]
    col = db[myCollectionName]
    tracker_db = client['tracker_db']
    tracker_col = tracker_db[trackerCollectionName]

    allDone = False
    completedSuccessfully = False
    tempLastScannedObjectId = lastScannedObjectId
    overall_start_time = time.time()

    while not allDone:
        try:
            batch_start_time = time.time()

            if lastScannedObjectId != 0:
                batch = col.find({"_id": {"$gt": lastScannedObjectId}}, {"_id": 1}).sort([("_id", 1)]).limit(appConfig['batchSize'])
            else:
                batch = col.find({}, {"_id": 1}).sort([("_id", 1)]).limit(appConfig['batchSize'])

            batch_count = 0
            updateList = []

            for doc in batch:
                if doc["_id"] <= maxObjectIdToTouch:
                    updateList.append(pymongo.UpdateOne({"_id": doc["_id"]}, {"$set": {appConfig['updateField']: 1}}))
                    tempLastScannedObjectId = doc["_id"]
                    batch_count += 1
                else:
                    allDone = True
                    completedSuccessfully = True
                    printLog("found id {} higher than maxObjectIdToTouch {}. all done. stopping".format(
                        str(doc["_id"]), str(maxObjectIdToTouch)), appConfig)
                    break

            if batch_count > 0:
                col.bulk_write(updateList)
                numDocumentsUpdated += batch_count

                if not appConfig['skipCleanup']:
                    cleanupList = [pymongo.UpdateOne({"_id": op._filter["_id"]}, {"$unset": {appConfig['updateField']: ""}}) for op in updateList]
                    col.bulk_write(cleanupList)
                    printLog("cleanup: removed dummy field from {:,} docs".format(batch_count), appConfig)

                batch_elapsed = time.time() - batch_start_time
                overall_elapsed = time.time() - overall_start_time
                progress_pct = (numDocumentsUpdated / numExistingDocuments * 100) if numExistingDocuments > 0 else 0
                docs_remaining = numExistingDocuments - numDocumentsUpdated
                rate = numDocumentsUpdated / overall_elapsed if overall_elapsed > 0 else 0
                eta_seconds = int(docs_remaining / rate) if rate > 0 else 0
                eta_str = str(datetime.timedelta(seconds=eta_seconds))

                tracker_entry = {
                    "collection_name": appConfig['collectionName'],
                    "lastScannedObjectId": tempLastScannedObjectId,
                    "date": datetime.datetime.now(tz=datetime.timezone.utc),
                    "maxObjectIdToTouch": maxObjectIdToTouch,
                    "numExistingDocuments": numExistingDocuments,
                    "numDocumentsUpdated": numDocumentsUpdated,
                    "cleanupComplete": not appConfig['skipCleanup']
                }
                tracker_col.insert_one(tracker_entry)

                bar_width = 20
                filled = int(bar_width * progress_pct / 100)
                bar = chr(9608) * filled + chr(9617) * (bar_width - filled)
                printLog(
                    "[{}] {:.1f}% | {:,}/{:,} | batch: {:,} docs in {:.1f}s | rate: {:.0f} docs/s | ETA: {}".format(
                        bar, progress_pct,
                        numDocumentsUpdated, numExistingDocuments,
                        batch_count, batch_elapsed,
                        rate, eta_str),
                    appConfig)

                lastScannedObjectId = tempLastScannedObjectId

                printLog("sleeping for {} seconds".format(appConfig['waitPeriod']), appConfig)
                time.sleep(appConfig['waitPeriod'])
            else:
                printLog("No updates in batch", appConfig)
                allDone = True
                completedSuccessfully = True
                break

        except pymongo.errors.PyMongoError as e:
            printLog("MongoDB error: {}. Retrying in {} seconds...".format(e, RETRY_DELAY), appConfig)
            time.sleep(RETRY_DELAY)
            try:
                client.close()
            except Exception:
                pass
            try:
                client = get_mongo_client(appConfig['uri'], appConfig)
                db = client[myDatabaseName]
                col = db[myCollectionName]
                tracker_db = client['tracker_db']
                tracker_col = tracker_db[trackerCollectionName]
            except pymongo.errors.PyMongoError as reconnect_err:
                printLog("Fatal: could not reconnect after error: {}".format(reconnect_err), appConfig)
                break

    if completedSuccessfully:
        overall_elapsed = time.time() - overall_start_time
        printLog("completed | totalDocumentsUpdated: {:,} | elapsed: {} | cleanupComplete: {}".format(
            numDocumentsUpdated,
            str(datetime.timedelta(seconds=int(overall_elapsed))),
            not appConfig['skipCleanup']),
            appConfig)
        try:
            tracker_col.drop()
            printLog("tracker collection '{}' dropped".format(trackerCollectionName), appConfig)
        except pymongo.errors.PyMongoError as e:
            printLog("Warning: could not drop tracker collection: {}".format(e), appConfig)

    client.close()

def main():
    parser = argparse.ArgumentParser(description='Update and Apply Compression')
    parser.add_argument('--uri', required=True, type=str, help='URI (connection string)')
    parser.add_argument('--database', required=True, type=str, help='Database')
    parser.add_argument('--collection', required=True, type=str, help='Collection')
    parser.add_argument('--file-name', required=False, type=str, default='compressor', help='Starting name of the created log files')
    parser.add_argument('--update-field', required=False, type=str, default='6nh63', help='Field used for updating an existing document. This should not conflict with any fieldname you are already using')
    parser.add_argument('--wait-period', required=False, type=int, default=60, help='Number of seconds to wait between each batch')
    parser.add_argument('--batch-size', required=False, type=int, default=5000, help='Number of documents to update in a single batch')
    parser.add_argument('--append-log', required=False, action='store_true', default=False, help='Append to existing log file instead of overwriting it on startup')
    parser.add_argument('--skip-cleanup', required=False, action='store_true', default=False, help='Skip removing the dummy field after each batch (leaves update field permanently on documents)')

    args = parser.parse_args()

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['numInsertProcesses'] = 1
    appConfig['databaseName'] = args.database
    appConfig['collectionName'] = args.collection
    appConfig['updateField'] = args.update_field
    appConfig['batchSize'] = int(args.batch_size)
    appConfig['waitPeriod'] = int(args.wait_period)
    appConfig['logFileName'] = "{}.log".format(args.file_name)
    appConfig['appendLog'] = args.append_log
    appConfig['skipCleanup'] = args.skip_cleanup

    validate_connection(appConfig)

    setUpdata = setup(appConfig)

    appConfig['numExistingDocuments'] = setUpdata["numExistingDocuments"]
    appConfig['maxObjectIdToTouch'] = setUpdata["maxObjectIdToTouch"]
    appConfig['lastScannedObjectId'] = setUpdata["lastScannedObjectId"]
    appConfig['numDocumentsUpdated'] = setUpdata["numDocumentsUpdated"]

    if not appConfig['appendLog']:
        deleteLog(appConfig)

    printLog('---------------------------------------------------------------------------------------', appConfig)
    for thisKey in sorted(appConfig):
        if thisKey == 'uri':
            thisUri = appConfig[thisKey]
            thisParsedUri = pymongo.uri_parser.parse_uri(thisUri)
            thisUsername = thisParsedUri['username']
            thisPassword = thisParsedUri['password']
            thisUri = thisUri.replace(thisUsername, '<USERNAME>')
            thisUri = thisUri.replace(thisPassword, '<PASSWORD>')
            printLog("  config | {} | {}".format(thisKey, thisUri), appConfig)
        else:
            printLog("  config | {} | {}".format(thisKey, appConfig[thisKey]), appConfig)
    printLog('---------------------------------------------------------------------------------------', appConfig)

    mp.set_start_method('spawn')

    processList = []
    for loop in range(appConfig['numInsertProcesses']):
        p = mp.Process(target=task_worker, args=(loop, appConfig))
        processList.append(p)
    for process in processList:
        process.start()

    for process in processList:
        process.join()

    printLog("Created {} with results".format(appConfig['logFileName']), appConfig)

if __name__ == "__main__":
    main()
