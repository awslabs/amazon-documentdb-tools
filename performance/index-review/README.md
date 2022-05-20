# Amazon DocumentDB Index Review Tool

The index review tool catalogs all collections and their indexes (structure and usage). It outputs a JSON file containing all collected information, a listing of unused and/or redundant indexes, and a pair of CSV files containing collection and index details. *NOTE:* indexes should never be dropped without discussing with all interested parties and performing performance testing.

# Requirements
 - Python 3.7+
   - If using Snappy wire protocol compression and MongoDB, "apt install python-snappy"
 - PyMongo
   - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
   - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
   - MongoDB 5.1+      | pymongo 4.0+
   - DocumentDB        | pymongo 3.10 - 4.0

## Using the Index Review Tool
`python3 index-review.py --server-alias <server-alias> --uri <mongodb-uri>`

- Run on all instances (primary and all secondaries)
- Connect directly to servers, not as replicaSet. If driver version supports &directConnection=true then provide it as part of the --uri
- Use a different \<server-alias> for each server, output files are named using \<server-alias> as the starting portion
- Avoid running the tool from the server itself if possible, it consume disk space for the output files
- The \<mongodb-uri> options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/ 
- Consider adding "&compressor=snappy" to your \<mongodb-uri> if your MongoDB server supports it
- For DocumentDB use the instance endpoints, not the cluster endpoint

## License
This tool is licensed under the Apache 2.0 License. 
