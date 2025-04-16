#!/bin/bash
export MONGODB_URI='mongodb://.../?tls=true&tlsCAFile=global-bundle.pem&retryWrites=false'
export SAMPLE_SIZE=1000
export DICTIONARY_SAMPLE_SIZE=100
export FILENAME=output.log
declare -a COMPRESSION_ALGOS=("lz4-fast" "lz4-fast-dict" "zstd-1" "zstd-1-dict" "lz4-high" "lz4-high-dict" "zstd-5" "zstd-5-dict" "snappy")

# clean up previously download files
rm -f *.csv output.log
# download the global bundle ca file
if [ ! -f "global-bundle.pem" ]
then
    wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
fi
# create header entry
echo "algo,dbName,collName,numDocs,avgDocSize,sizeGB,storageGB,compRatio,minSample,maxSample,avgSample,minComp,maxComp,avgComp,lz4Ratio,exceptions,compTime(ms)
" > $FILENAME
# loop through the compression algorithms
for COMPRESSION_ALGO in "${COMPRESSION_ALGOS[@]}"
do
    python compression-review.py --uri=$MONGODB_URI --server-alias docdb-test --sample-size $SAMPLE_SIZE --dictionary-sample-size $DICTIONARY_SAMPLE_SIZE --compressor $COMPRESSION_ALGO
    mv docdb-test-*-compression-review.csv ${COMPRESSION_ALGO}-docdb-test-compression-review.csv
    cat ${COMPRESSION_ALGO}-docdb-test-compression-review.csv | tail -n +5 | awk "\$0=\"${COMPRESSION_ALGO},\"\$0" >> $FILENAME
done


