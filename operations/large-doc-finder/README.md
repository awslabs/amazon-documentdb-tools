# Large Document Finder for DocumentDB

This tool scans an Amazon DocumentDB collection to identify documents that exceed a specified size threshold. It processes documents in parallel using multiple threads and outputs results exceeding the threshold to a CSV file.

# Requirements
 - Python 3.9+
 - pymongo Python package - tested versions
   - DocumentDB        | pymongo 4.10.1
   - If not installed - "$ pip3 install pymongo"

## Example usage:
Basic usage:

    python large-docs.py --uri "mongodb://..." \
                        --processes 8 \
                        --batch-size 1000 \
                        --database mydb \
                        --collection mycollection \
                        --csv "mydb_mycollection_" \
                        --large-doc-size 10485760

## Parameters:
`--uri` : str
- Required
- DocumentDB connection string
- Example: `mongodb://user:password@name.cluster.region.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false`

`--processes` : int
- Required
- Number of parallel threads to use
- Example: 8

`--batch-size` : int
- Required
- Number of documents to process in each batch
- Example: 1000

`--database` : str
- Required
- Name of the database to scan
- Example: `mydb`

`--collection` : str
- Required
- Name of the collection to scan
- Example: `mycollection`

`--csv` : str
- Prefix for the CSV output filename 
- Default: `large_doc_`
- Example: `large_docs_prod`

`--large-doc-size` : int
- Size threshold in bytes 
- Default: 8388608 (8MB)
- Example: 10485760 (10MB)

## Example output:
----------------
The output CSV contains:
- Scan details (database, collection, threshold, etc.)
- Document details (ID, size in bytes, size in MB)

    ```Database,mydb
    Collection,mycollection
    Batch size,50000
    Number of threads,4
    Total documents,3156003
    Large document threshold (bytes),8388608
    Large document threshold (MB),8.00
    Scan Start Time,2025-03-02T22:17:04.761870
    Scan completion time,2025-03-02T22:17:36.291172
    Total scan time,00:00:31
    Large documents found,3

    Document _id,Size (bytes),Size (MB)
    65e8f2a1b3e8d97531abcdef,9437247,9.00
    65e8f2a2b3e8d97531abcd01,9437247,9.00
    65e8f2a3b3e8d97531abcd02,9437247,9.00

## Performance Considerations:
1. Thread count: Start with 2x CPU cores, adjust based on monitoring
2. Batch size: Larger batches = more memory but fewer DB round trips
3. Run during off-peak hours and monitor cluster performance metrics
4. Use `secondaryPreferred` read preference

## License
This tool is licensed under the Apache 2.0 License. 