//For adding the everything_same test documents to both source collection and target collection, as this is called from both of those collections. 
//Everything is the exact same in both databses (for a perfect case) 
const mydb =_getEnv('myDB')
const coll = _getEnv('myColl')
db1 = new Mongo().getDB(mydb)
var mycoll = db1.getCollection(coll)
mycoll.insertMany([{"_id": 1.0, "name": "John", "age": 21.0, "siblings": 1.0}, 
{"_id": 2.0, "name": "Frank", "age": 22.0, "siblings": 14.0}, 
{"_id": 3.0, "name": "Susan", "age": 22.0, "siblings": 2.0}])
