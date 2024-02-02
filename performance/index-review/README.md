# Amazon DocumentDB Index Review Tool

The index review tool catalogs all collections and their indexes (structure and usage). It outputs a JSON file containing all collected information, a listing of unused and/or redundant indexes, and a pair of CSV files containing collection and index details. 

*NOTE: indexes should never be dropped without discussing with all interested parties and testing performance*.

# Requirements
 - Python 3.7+
 - PyMongo

## Using the Index Review Tool
`python3 index-review.py --server-alias <server-alias> --uri <mongodb-uri>`

- Execute on all instances (primary and all secondaries/read-replicas), this is critical to give a complete review of index usage.
- Use a different `<server-alias>` for each server, output files are named using `<server-alias>` as the starting portion of the filename
- All `<mongodb-uri>` options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/ 
- For DocumentDB use the individual instance endpoints, not the cluster endpoint

## License
This tool is licensed under the Apache 2.0 License. 
