from datetime import datetime, timezone
import sys
import json
import pymongo
from bson import ObjectId
import time
import os
import argparse
import warnings


def via_time(appConfig):
    # calculate boundaries by uniformly splitting the time range between the
    # first and last _id values (based on the ObjectId embedded timestamp)
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

    numInRange = col.count_documents({"_id":{"$gte":minId,"$lte":maxId}})

    print("")
    print("Min: {} -> {}".format(minId, minId.generation_time))
    print("Max: {} -> {}".format(maxId, maxId.generation_time))
    print("Total in timerange: {}".format(numInRange))
    print("finding _id values for {} time-uniform segments".format(numSegments))
    print("")

    queryStartTime = time.time()

    # first boundary is the actual minimum _id
    boundaryList.append(str(minId))
    print("  boundary   0 - {} (min _id)".format(minId))

    # remaining boundaries are time-uniform splits, zero-filled ObjectIds
    for n in range(1, numSegments):
        boundaryTs = minTs + (step * n)
        boundarySecs = int(boundaryTs)
        hexSecs = format(boundarySecs, '08x')
        boundaryId = ObjectId(hexSecs + '0000000000000000')
        boundaryList.append(str(boundaryId))
        print("  boundary {:3d} - {} ({})".format(n, boundaryId, datetime.fromtimestamp(boundarySecs, tz=timezone.utc)))

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
    parser = argparse.ArgumentParser(description='DMS Time-Based Segment Analysis Tool.')

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

    args = parser.parse_args()

    appConfig = {}
    appConfig['uri'] = args.uri
    appConfig['database'] = args.database
    appConfig['collection'] = args.collection
    appConfig['numSegments'] = int(args.num_segments)

    via_time(appConfig)


if __name__ == "__main__":
    main()
