# MongoDB Oplog Review Tool

The mongodb oplog review tool connects to any instance in a MongoDB replicaset (primary or secondary), reads the entire oplog, and produces a log file containing counters for insert/update/delete operations by collection. 

# Requirements
 - Python 3.7+
   - If using Snappy wire protocol compression and MongoDB, "apt install python-snappy"
 - PyMongo
   - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
   - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
   - MongoDB 5.1+      | pymongo 4.0+
   - DocumentDB        | pymongo 3.10 - 4.0

## Using the Index Review Tool
`python3 mongo-oplog-review.py --server-alias <server-alias> --uri <mongodb-uri> --stop-when-oplog-current`

- Run on any instance in the replicaset (the larger the oplog the better)
- Use a different \<server-alias> for each execution
- If sharded
 - Run on one instance in each shard
- Avoid running the tool from the server itself if possible, it consume disk space for the output files
- Each execution creates a file starting with \<server-alias> and ending with .log
- The \<mongodb-uri> options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/ 
- Consider adding "&compressor=snappy" to your \<mongodb-uri> if your MongoDB server supports it

## License
This tool is licensed under the Apache 2.0 License. 
