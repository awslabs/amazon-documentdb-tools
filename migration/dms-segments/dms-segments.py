from datetime import datetime, timedelta
import sys
import json
import pymongo
import time
#from bson import json_util
import os

connectionString = sys.argv[1]
databaseName = sys.argv[2]
collectionName = sys.argv[3]
numChunks = int(sys.argv[4])

numBoundaries = numChunks - 1

client = pymongo.MongoClient(connectionString)
db = client[databaseName]
col = db[collectionName]

collStats = client[databaseName].command("collStats",collectionName)
numDocuments = collStats['count']
feedbackDocuments = int(numDocuments/numChunks)
progressDocuments = int((numDocuments - feedbackDocuments)*0.01)

print("")
print("collection {}.{} contains {} documents".format(databaseName,collectionName,numDocuments))
print("finding _id values for {} chunks, approximately {} documents in each".format(numChunks,feedbackDocuments))

queryStartTime = time.time()
cursor = col.find(filter=None,projection={"_id":True},sort=[("_id",pymongo.ASCENDING)])
print("..cursor created")
numDocsTotal = 0
numDocsBoundry = 0
thisBoundry = 0
for thisDoc in cursor:
    numDocsTotal += 1
    numDocsBoundry += 1
    
    if (numDocsBoundry >= feedbackDocuments):
        numDocsBoundry = 0
        thisBoundry += 1
        print("  boundry {} - objectid {}".format(thisBoundry,thisDoc["_id"]))
        if (thisBoundry >= numBoundaries):
            break
            
    if (numDocsTotal % progressDocuments == 0):
        pctDone = numDocsTotal/(numDocuments - feedbackDocuments)*100
        elapsedSecs = int(time.time() - queryStartTime)
        estimatedSecsToDone = int(((100/pctDone)*elapsedSecs)-elapsedSecs)
        print("  documents processed = {:12,d} - {:5,.1f} percent - {:10,d} seconds duration - done in approx {:10,d} seconds".format(numDocsTotal,pctDone,elapsedSecs,estimatedSecsToDone))
        
queryElapsedSecs = int(time.time() - queryStartTime)
print('query required {} seconds'.format(queryElapsedSecs))

print("")
    
client.close()
