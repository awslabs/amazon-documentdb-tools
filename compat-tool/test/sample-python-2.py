import sys
import pymongo
from datetime import datetime

connectionString = sys.argv[1]

client = pymongo.MongoClient(connectionString)
col = client["db1"]["coll1"]

start = datetime(2022, 1, 1, 00, 00, 00)
end = datetime(2022, 1, 1, 23, 59, 59)
inList = [1,2,3,4,5,6,7,8,9,10]

result = col.count_documents({"docId":{"$in":inList},"createDate": { "$gt": start, "$lte": end }})
print(result)
result = col.aggregate([{ "$match": {"docId":{"$in":inList},"createDate": { "$gt": start, "$lte": end }} },{ "$group": { "_id": None, "n": { "$sum": 1 } } }])
print(result)
result = col.find(filter={"createDate": { "$gt": start, "$lte": end }},projection={"_id":0, "patientId":1})
print(result)
result = col.aggregate([
                         { "$match" : { "createDate": { "$gt": start, "$lte": end }}},
                         { "$lookup": { "from": "tempCollection", "localField": "docId", "foreignField": "_id", "as": "am-allowed"}},
                         { "$match" : { "am-allowed": { "$ne": [] }}},
                         { "$count" : "numdocs" }
                       ])
print(result)                       
result = col.aggregate([
                         { "$match" : { "createDate": { "$gt": start, "$lte": end }}},
                         { "$sortByCount": "$state" }
                       ])
print(result)                       

client.close()