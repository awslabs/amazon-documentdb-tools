//For adding the extra_target_field test documents in the source collection. 
//Frank in the target collection has an additional field of pets with a value which does not exist in the source. Everything else is the same.
const mydb =_getEnv('myDB')
const coll = _getEnv('myColl')
db1 = new Mongo().getDB(mydb)
var mycoll = db1.getCollection(coll)
mycoll.insertMany([{"_id": 1.0, "name": "John", "age": 21.0, "siblings": 1.0}, 
{"_id": 2.0, "name": "Frank", "age": 22.0, "siblings": 14.0}, 
{"_id": 3.0, "name": "Susan", "age": 22.0, "siblings": 2.0}])