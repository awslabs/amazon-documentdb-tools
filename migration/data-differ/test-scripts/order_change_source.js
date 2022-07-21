//For adding the order_change test documents in the source collection. 
//John in target has order of fields differently than John in source. Everything else is the same. 
const mydb =_getEnv('myDB')
const coll = _getEnv('myColl')
db1 = new Mongo().getDB(mydb)
var mycoll = db1.getCollection(coll)
mycoll.insertMany([{"_id": 1.0, "name": "John", "age": 21.0, "siblings": 1.0}, 
{"_id": 2.0, "name": "Frank", "age": 22.0, "siblings": 14.0}, 
{"_id": 3.0, "name": "Susan", "age": 22.0, "siblings": 2.0}])

