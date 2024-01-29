# MongoDB Ops Tool

The MongoDB Ops tool gathers collection level query/insert/update/delete counters to assist in the process of sizing. 

# Requirements
 - Python 3.7+
 - PyMongo
   - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
   - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
   - MongoDB 5.1+      | pymongo 4.0+
   - DocumentDB        | pymongo 3.10 - 4.0

## Using the MongoDB Ops Tool
`python3 mongodb-ops.py --uri <mongodb-uri> --server-alias <server-alias> --collect`
- produces an output file for comparison

`python3 mongodb-ops.py --compare --file1 <first-compare-file> --file2 <second-compare-file>`
- compares the results of two executions to estimate the number of queries, inserts, updates, and deletes per second at the collection level.

## Notes
- Run on any instance in the replicaset (the larger the oplog the better)
- If sharded, run on one instance in each shard
- Each execution creates a file starting with \<server-alias> and ending with .json
- The \<mongodb-uri> options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/
- Use &directConnection=true

## License
This tool is licensed under the Apache 2.0 License. 
