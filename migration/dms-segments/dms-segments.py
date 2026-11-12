from datetime import datetime, timedelta, timezone
import sys
import json
import pymongo
from bson import ObjectId
import time
import os
import argparse
import math
import warnings


supportedIdTypes=['int','string','objectId']


def via_time(appConfig):
    # calculate boundaries by uniformly splitting the time range between the first
    # and last _id values (based on the ObjectId embedded timestamp); faster than the
    # count-based methods but does not guarantee even document distribution per segment
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    boundaryList = []

    numSegments = appConfig['numSegments']

    client = pymongo.MongoClient(host=appConfig['uri'],appname='segmentr')
    db = client[appConfig['database']]
    col = db[appConfig['collection']]

    # get the min and max _id values
    minDoc = col.find_one(filter=None,projection={"_id":True},sort=[("_id",pymongo.ASCENDING)])
    maxDoc = col.find_one(filter=None,projection={"_id":True},sort=[("_id",pymongo.DESCENDING)])

    if minDoc is None or maxDoc is None:
        print("collection {}.{} is empty, nothing to do".format(appConfig['database'],appConfig['collection']))
        client.close()
        return

    minId = minDoc["_id"]
    maxId = maxDoc["_id"]

    # time-based segmentation relies on the embedded timestamp of ObjectId values
    if not isinstance(minId, ObjectId) or not isinstance(maxId, ObjectId):
        print("time-based segmentation requires _id values of type ObjectId")
        print("found types '{}' (min) and '{}' (max) in {}.{}, stopping".format(type(minId).__name__, type(maxId).__name__, appConfig['database'], appConfig['collection']))
        client.close()
        return

    # ObjectId.generation_time returns a timezone-aware datetime, convert to epoch seconds
    minTs = minId.generation_time.timestamp()
    maxTs = maxId.generation_time.timestamp()

    step = (maxTs - minTs) / numSegments

    print("")
    print("Min: {} -> {}".format(minId, minId.generation_time))
    print("Max: {} -> {}".format(maxId, maxId.generation_time))
    print("finding _id values for {} time-uniform segments".format(numSegments))
    print("")

    queryStartTime = time.time()

    # DMS treats each boundary as an inclusive upper limit and adds a final open-ended segment,
    # so numSegments segments need numSegments - 1 boundaries (time-uniform splits, zero-filled ObjectIds)
    # a zero-filled ObjectId sorts before every real ObjectId created in the same second, so round each split
    # up to the next whole second, that way a boundary always falls after the min _id when the range is at least a second
    # boundaries that are not greater than the previous boundary (or the min _id), or not less than the max _id,
    # would create empty or duplicate segments, this happens when the collection spans only a few seconds, so skip them
    lastId = minId
    for n in range(1, numSegments):
        boundaryTs = minTs + (step * n)
        boundarySecs = math.ceil(boundaryTs)
        hexSecs = format(boundarySecs, '08x')
        boundaryId = ObjectId(hexSecs + '0000000000000000')
        if boundaryId <= lastId or boundaryId >= maxId:
            continue
        boundaryList.append(str(boundaryId))
        lastId = boundaryId
        print("  boundary {:3d} - {} ({})".format(len(boundaryList), boundaryId, datetime.fromtimestamp(boundarySecs, tz=timezone.utc)))

    print("")

    if len(boundaryList) == 0:
        print("the _id values in {}.{} were all created within the same second, time-based segmentation is not possible, use the default or --single-cursor mode instead".format(appConfig['database'], appConfig['collection']))
        print("")
        client.close()
        return

    if len(boundaryList) < numSegments - 1:
        print("WARNING: the _id values in {}.{} span only {} seconds, so only {} segments could be calculated instead of the {} requested".format(
            appConfig['database'], appConfig['collection'], int(maxTs - minTs), len(boundaryList) + 1, numSegments))
        print("")

    boundaryListAsString = "{}".format(",".join('"{}"'.format(i) for i in boundaryList))
    print("boundaries as list | {}".format(boundaryListAsString))

    boundaryListAsStringForDms = "[{}]".format("],[".join('"{}"'.format(i) for i in boundaryList))
    print("")
    print("boundaries as list for DMS | {}".format(boundaryListAsStringForDms))

    print("")

    queryElapsedSecs = int(time.time() - queryStartTime)
    print('query required {} seconds'.format(queryElapsedSecs))

    print("")

    client.close()


def segments_fit_collection(appConfig, numDocuments):
    # count-based modes need at least one document per segment
    if numDocuments < appConfig['numSegments']:
        print("collection {}.{} contains {} documents, which is fewer than the {} segments requested, stopping".format(appConfig['database'],appConfig['collection'],numDocuments,appConfig['numSegments']))
        return False
    return True


def via_skips(appConfig):
    # get boundaries by performing large server-side skips
    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    boundaryList = []

    numBoundaries = appConfig['numSegments'] - 1

    client = pymongo.MongoClient(host=appConfig['uri'],appname='segmentr')
    db = client[appConfig['database']]
    col = db[appConfig['collection']]

    collStats = db.command("collStats",appConfig['collection'])
    numDocuments = collStats['count']
    if not segments_fit_collection(appConfig, numDocuments):
        client.close()
        return
    feedbackDocuments = int(numDocuments/appConfig['numSegments'])

    print("")
    print("collection {}.{} contains {} documents".format(appConfig['database'],appConfig['collection'],numDocuments))
    print("finding _id values for {} chunks, approximately {} documents in each".format(appConfig['numSegments'],feedbackDocuments))

    queryStartTime = time.time()

    # each boundary is the last _id of its segment, so boundary n is the (n * feedbackDocuments)th document
    # the first boundary skips (feedbackDocuments - 1) documents from the start of the collection, each later
    # boundary skips (feedbackDocuments - 1) documents after the previous boundary
    currentId = None
    numDocsTotal = 0

    for x in range(numBoundaries):
        idFilter = None if currentId is None else {"_id":{"$gt":currentId["_id"]}}
        currentId = col.find_one(filter=idFilter,projection={"_id":True},sort=[("_id",pymongo.ASCENDING)],skip=feedbackDocuments-1)
        if currentId is None:
            # collection has fewer documents than collStats reported, likely due to concurrent deletes
            print("")
            print("reached the end of {}.{} after {} boundaries, the collection has fewer documents than collStats reported, stopping".format(appConfig['database'],appConfig['collection'],x))
            client.close()
            return
        numDocsTotal += feedbackDocuments
        pctDone = numDocsTotal/(numDocuments - feedbackDocuments)*100
        elapsedSecs = int(time.time() - queryStartTime)
        estimatedSecsToDone = int(((100/pctDone)*elapsedSecs)-elapsedSecs)
        print("  boundary {:3d} - {} {} | done in approximately {} seconds".format(x+1,type(currentId["_id"]),currentId["_id"],estimatedSecsToDone))
        boundaryList.append(currentId["_id"])

    boundaryListAsString = "{}".format(",".join('"{}"'.format(i) for i in boundaryList))
    print("")
    print("boundaries as list | {}".format(boundaryListAsString))

    boundaryListAsStringForDms = "[{}]".format("],[".join('"{}"'.format(i) for i in boundaryList))
    print("")
    print("boundaries as list for DMS | {}".format(boundaryListAsStringForDms))

    print("")

    queryElapsedSecs = int(time.time() - queryStartTime)
    print('query required {} seconds'.format(queryElapsedSecs))

    print("")
        
    client.close()


def via_cursor(appConfig):
    # get by walking the _id index

    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    numBoundaries = appConfig['numSegments'] - 1
    boundaryList = []

    client = pymongo.MongoClient(host=appConfig['uri'],appname='segmentr')
    db = client[appConfig['database']]
    col = db[appConfig['collection']]

    collStats = db.command("collStats",appConfig['collection'])
    numDocuments = collStats['count']
    if not segments_fit_collection(appConfig, numDocuments):
        client.close()
        return
    feedbackDocuments = int(numDocuments/appConfig['numSegments'])
    # report progress about every 1 percent, at least every document for small collections
    progressDocuments = max(1, int((numDocuments - feedbackDocuments)*0.01))

    print("")
    print("collection {}.{} contains {} documents".format(appConfig['database'],appConfig['collection'],numDocuments))
    print("finding _id values for {} chunks, approximately {} documents in each".format(appConfig['numSegments'],feedbackDocuments))

    queryStartTime = time.time()
    
    cursor = col.find(filter=None,projection={"_id":True},sort=[("_id",pymongo.ASCENDING)])
    print("..cursor created")
    numDocsTotal = 0
    numDocsBoundary = 0
    thisBoundary = 0
    for thisDoc in cursor:
        numDocsTotal += 1
        numDocsBoundary += 1

        if (numDocsBoundary >= feedbackDocuments):
            numDocsBoundary = 0
            thisBoundary += 1
            print("  boundary {:3d} - objectid {}".format(thisBoundary,thisDoc["_id"]))
            boundaryList.append(thisDoc["_id"])
            if (thisBoundary >= numBoundaries):
                break

        if (numDocsTotal % progressDocuments == 0):
            pctDone = numDocsTotal/(numDocuments - feedbackDocuments)*100
            elapsedSecs = int(time.time() - queryStartTime)
            estimatedSecsToDone = int(((100/pctDone)*elapsedSecs)-elapsedSecs)
            print("  documents processed = {:12,d} - {:5,.1f} percent - {:10,d} seconds duration - done in approx {:10,d} seconds".format(numDocsTotal,pctDone,elapsedSecs,estimatedSecsToDone))

    print("")

    if len(boundaryList) < numBoundaries:
        # collection has fewer documents than collStats reported, likely due to concurrent deletes
        print("reached the end of {}.{} after {} boundaries, the collection has fewer documents than collStats reported, stopping".format(appConfig['database'],appConfig['collection'],len(boundaryList)))
        print("")
        client.close()
        return

    # output full boundary list
    boundaryNum = 0
    print("Boundary list")
    for thisBoundary in boundaryList:
        boundaryNum += 1
        print("  boundary {:3d} - objectid {}".format(boundaryNum,thisBoundary))

    print("")

    boundaryListAsString = "{}".format(",".join('"{}"'.format(i) for i in boundaryList))
    print("boundaries as list | {}".format(boundaryListAsString))

    boundaryListAsStringForDms = "[{}]".format("],[".join('"{}"'.format(i) for i in boundaryList))
    print("")
    print("boundaries as list for DMS | {}".format(boundaryListAsStringForDms))

    print("")

    queryElapsedSecs = int(time.time() - queryStartTime)
    print('query required {} seconds'.format(queryElapsedSecs))

    print("")

    client.close()


def check_for_mixed_types(appConfig):
    # grab the first document and last document as ordered by _id, check for unsupported or differing data types
    returnValue = True

    warnings.filterwarnings("ignore","You appear to be connected to a DocumentDB cluster.")

    global supportedIdTypes

    client = pymongo.MongoClient(host=appConfig['uri'])
    db = client[appConfig['database']]
    col = db[appConfig['collection']]

    firstDocs = list(col.aggregate([{"$sort":{"_id":pymongo.ASCENDING}},{"$project":{"_id":False,"idType":{"$type":"$_id"}}},{"$limit":1}]))
    lastDocs = list(col.aggregate([{"$sort":{"_id":pymongo.DESCENDING}},{"$project":{"_id":False,"idType":{"$type":"$_id"}}},{"$limit":1}]))

    if len(firstDocs) == 0 or len(lastDocs) == 0:
        print("collection {}.{} is empty, nothing to do".format(appConfig['database'],appConfig['collection']))
        client.close()
        return False

    idTypeFirst = firstDocs[0]['idType']
    idTypeLast = lastDocs[0]['idType']

    if idTypeFirst not in supportedIdTypes:
        # unsupported data type
        print("Unsupported data type of '{}' for first _id value in {}.{} - only {} types are supported, stopping".format(idTypeFirst,appConfig['database'],appConfig['collection'],supportedIdTypes))
        returnValue = False

    if idTypeLast not in supportedIdTypes:
        # unsupported data type
        print("Unsupported data type of '{}' for last _id value in {}.{} - only {} types are supported, stopping".format(idTypeLast,appConfig['database'],appConfig['collection'],supportedIdTypes))
        returnValue = False

    if idTypeFirst != idTypeLast:
        # mixed data types
        print("Mixed data types of '{}' and '{}' for first and last  _id values in {}.{}, stopping".format(idTypeFirst,idTypeLast,appConfig['database'],appConfig['collection']))
        returnValue = False

    client.close()

    return returnValue


def main():
    parser = argparse.ArgumentParser(description='DMS Segment Analysis Tool.')

    parser.add_argument('--uri',
                        required=True,
                        type=str,
                        help='URI')

    parser.add_argument('--database',
                        required=True,
                        type=str,
                        help='Database')

    parser.add_argument('--collection',
                        required=True,
                        type=str,
                        help='Collection')

    parser.add_argument('--num-segments',
                        required=True,
                        type=int,
                        help='Number of segments, must be 2 or more')

    parser.add_argument('--single-cursor',
                        required=False,
                        action='store_true',
                        help='Scan the full _id index using a cursor')

    parser.add_argument('--time-based-segments',
                        required=False,
                        action='store_true',
                        help='Calculate boundaries by uniformly splitting the ObjectId timestamp range (faster, requires ObjectId _id values, does not guarantee even document distribution)')

    args = parser.parse_args()

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['database'] = args.database
    appConfig['collection'] = args.collection
    appConfig['numSegments'] = args.num_segments

    if appConfig['numSegments'] < 2:
        parser.error('--num-segments must be 2 or more, a single segment does not need boundaries')

    if args.time_based_segments:
        via_time(appConfig)

    elif check_for_mixed_types(appConfig):
        if args.single_cursor:
            via_cursor(appConfig)

        else:
            via_skips(appConfig)


if __name__ == "__main__":
    main()
