from datetime import datetime, timedelta
import sys
import json
import pymongo
import time
import os
import argparse


def via_skips(appConfig):
    # get boundaries by performing large server-side skips

    boundaryList = []

    numBoundaries = appConfig['numSegments'] - 1

    client = pymongo.MongoClient(appConfig['uri'])
    db = client[appConfig['database']]
    col = db[appConfig['collection']]

    collStats = db.command("collStats",appConfig['collection'])
    numDocuments = collStats['count']
    feedbackDocuments = int(numDocuments/appConfig['numSegments'])
    progressDocuments = int((numDocuments - feedbackDocuments)*0.01)

    print("")
    print("collection {}.{} contains {} documents".format(appConfig['database'],appConfig['collection'],numDocuments))
    print("finding _id values for {} chunks, approximately {} documents in each".format(appConfig['numSegments'],feedbackDocuments))

    queryStartTime = time.time()

    # get the first _id
    currentId = col.find_one(filter=None,projection={"_id":True},sort=[("_id",pymongo.ASCENDING)])
    print("  found first _id")
    numDocsTotal = 0

    for x in range(numBoundaries):
        currentId = col.find_one(filter={"_id":{"$gt":currentId["_id"]}},projection={"_id":True},sort=[("_id",pymongo.ASCENDING)],skip=feedbackDocuments)
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

    numBoundaries = appConfig['numSegments'] - 1
    boundaryList = []

    client = pymongo.MongoClient(appConfig['uri'])
    db = client[appConfig['database']]
    col = db[appConfig['collection']]

    collStats = db.command("collStats",appConfig['collection'])
    numDocuments = collStats['count']
    feedbackDocuments = int(numDocuments/appConfig['numSegments'])
    progressDocuments = int((numDocuments - feedbackDocuments)*0.01)

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
                        type=str,
                        help='Number of segments')

    parser.add_argument('--single-cursor',
                        required=False,
                        action='store_true',
                        help='Scan the full _id index using a cursor')

    args = parser.parse_args()

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['database'] = args.database
    appConfig['collection'] = args.collection
    appConfig['numSegments'] = int(args.num_segments)

    if args.single_cursor:
        via_cursor(appConfig)

    else:
        via_skips(appConfig)


if __name__ == "__main__":
    main()
