## DataDiffer Tests

### Automated tests (test_data_differ.py)
Unit tests (keyspace partitioning, _id equality, document comparison) run without a database. Integration tests load
documents into a source and a target collection, run data-differ.py, and verify the exact _id values reported as
missing, extra, and different, for both the range partitioned and the batched strategies. The integration tests are
skipped unless `SOURCE_URI` and `TARGET_URI` are set. The source and target can be the same cluster, the tests create
and drop collections in the `datadiff_test` database (override with `DATADIFF_TEST_DB`).

```
    export SOURCE_URI="mongodb://user:password@source-host:27017/?tls=true&tlsCAFile=global-bundle.pem"
    export TARGET_URI="mongodb://user:password@target-host:27017/?tls=true&tlsCAFile=global-bundle.pem"
    python3 -m unittest -v test_data_differ.py
```

### Large data sets (generate_test_data.py)
Loads a large data set into the source and target with a known number of missing, extra, and different documents,
then verifies the output of data-differ.py against them. Useful for performance testing.

```
    python3 generate_test_data.py load --db perf --coll test --docs 10000000 --id-type objectid \
        --missing 100 --extra 50 --different 1000 --truth-file truth.json
    python3 ../data-differ.py --source-uri "$SOURCE_URI" --target-uri "$TARGET_URI" --source-db perf \
        --source-coll test --target-db perf --target-coll test --output-file differences.txt
    python3 generate_test_data.py verify --truth-file truth.json --output-file differences.txt
    python3 generate_test_data.py drop --db perf --coll test
```

`--id-type` can be `objectid`, `int`, `date` (range partitioned) or `string` (batched).

### Bash test scripts
The bash scripts load the JSON files with mongoimport and require mongoimport and mongosh. The same JSON files are
also used by the automated tests.

1. Ensure you have met the prerequsities from the general README.
2. Create an envrionment file(s) as needed based on your migration needs. The environment file should set environment variables and look something like this with each variable filled out for your use case:
```
    export SOURCE_URI=""
    export SOURCE_DB=""
    export SOURCE_COLL=""
    export TARGET_URI=""
    export TARGET_DB=""
    export TARGET_COLL=""
```
3. Source the environment file you built in the command line with a command such as the following:
```
    source <environment-file-name-here>.sh
```
4. Run the appropriate bash test script in the command line with a command such as the following:
```
    bash <test-script-name-here>.bash
```
5. See the output in the command line!
