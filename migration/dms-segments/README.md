# Amazon Database Migration Service (DMS) Segment Analyzer

The DMS Segment Analyzer calculates the segment boundaries of MongoDB and Amazon DocumentDB collections to be used for segmenting DMS full load operations.

# Requirements
 - Python 3.7+
 - PyMongo
   - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
   - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
   - MongoDB 5.1+      | pymongo 4.0+
   - DocumentDB        | pymongo 3.10+

## Using the DMS Segment Analyzer
`python3 dms-segments.py --uri <server-uri> --database <database-name> --collection <collection-name> --num-segments <number-of-segments>`

- Run on any instance in your MongoDB or Amazon DocumentDB cluster
- Connect directly to servers, not as replicaSet. If driver version supports &directConnection=true then provide it as part of the --uri
- The \<mongodb-uri> options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/ 
- For DocumentDB use the instance endpoints, not the cluster endpoint
- By default the tool uses large .skip() operations to determine the boundary ObjectId's, if you experience timeouts consider using the --single-cursor option

## License
This tool is licensed under the Apache 2.0 License. 
