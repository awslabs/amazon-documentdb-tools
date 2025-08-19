# Amazon DocumentDB Compression Review Tool

The compression review tool samples 1000 documents in each collection to determine the average compressibility of the data. A larger number of documents can be sampled via the --sample-size parameter. 

# Requirements
 - Python 3.7+
 - pymongo Python package - tested versions
   - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
   - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
   - MongoDB 5.1+      | pymongo 4.0+
   - DocumentDB        | pymongo 3.10+
   - If not installed - "$ pip3 install pymongo"
 - lz4 Python package
   - If not installed - "$ pip3 install lz4"
 - zstandard Python package
   - If not installed - "$ pip3 install zstandard"

## Using the Compression Review Tool
`python3 compression-review.py --uri <server-uri> --server-alias <server-alias>`

- Default compression tested is lz4/fast/level 1
- To test other compression techniques provide --compressor \<compression-type> with one of the following for \<compression-type>

| compression | description |
| ----------- | ----------- |
| lz4-fast | lz4/fast/level 1 |
| zstd-3-dict | zstandard/level 3/dictionary-provided (trained by sampling documents) |

- Run on any instance in the replica set
- Use a different \<server-alias> for each server analyzed, output file is named using \<server-alias> as the starting portion
- Creates a single CSV file per execution
- The \<server-uri> options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/ 
  - If your URI contains ampersand (&) characters they must be escaped with the backslash or enclosed your URI in double quotes
- For DocumentDB use either the cluster endpoint or any of the instance endpoints

## License
This tool is licensed under the Apache 2.0 License. 
