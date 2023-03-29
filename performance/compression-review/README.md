# Amazon DocumentDB Compression Review Tool

The compression review tool samples 1000 documents in each collection to determine the average compressibility of the data. A larger number of documents can be sampled via the --sample-size parameter. 

# Requirements
 - Python 3.7+
   - If using Snappy wire protocol compression and MongoDB, "apt install python-snappy"
 - pymongo Python package
   - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
   - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
   - MongoDB 5.1+      | pymongo 4.0+
   - DocumentDB        | pymongo 3.10+
 - lz4 Python package

## Using the Compression Review Tool
`python3 compression-review.py --uri <server-uri> --server-alias <server-alias>`

- Run on any instance in the replica set
- Use a different \<server-alias> for each server analyzed, output files are named using \<server-alias> as the starting portion
- The \<server-uri> options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/ 
- Consider adding "&compressor=snappy" to your \<mongodb-uri> if your MongoDB server supports it
- For DocumentDB use either the cluster endpoint or any of the instance endpoints

## License
This tool is licensed under the Apache 2.0 License. 
