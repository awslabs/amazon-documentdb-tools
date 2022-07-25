//drops both collections we created 
const mydb =_getEnv('myDB')
const coll = _getEnv('myColl')
db1 = new Mongo().getDB(mydb)
var mycoll = db1.getCollection(coll)
mycoll.drop()

