import os
import argparse
from pymongo import MongoClient
from deepdiff import DeepDiff
from tqdm import tqdm

def connect_to_db(uri, pool_size):
    client = MongoClient(uri, maxPoolSize=pool_size)
    return client

## Compare doc count, if not equal try to find the missing docs
## Needs optimisation for DocumentDB
def compare_document_counts(srcCollection, tgtCollection, output_file):
    count1 = srcCollection.count_documents({})
    count2 = tgtCollection.count_documents({})

    if count1 > count2:
        print(f"Document count mismatch: Found {count1} documents in srcCollection vs. {count2} documents in tgtCollection, scanning...")
        missing_docs = srcCollection.find({'_id': {'$nin': tgtCollection.distinct('_id')}})
        write_difference_to_file(output_file, "Document _IDs present in collection1 but not in collection2:")
    else:
        print(f"Document count mismatch: Found {count1} documents in srcCollection vs. {count2} documents in tgtCollection, scanning...")
        missing_docs = tgtCollection.find({'_id': {'$nin': srcCollection.distinct('_id')}})
        write_difference_to_file(output_file, "Document _IDs present in collection2 but not in collection1:")

    for doc in missing_docs:
        print(doc['_id'])
        write_difference_to_file(output_file, doc['_id'])

## Compare document data, open a cursor for both databases, stream batches of data based on batch_size
## and compare the batches with DeepDiff
def compare_document_data(srcCollection, tgtCollection, batch_size, output_file):
    cursor1 = srcCollection.find().sort('_id').batch_size(batch_size)
    cursor2 = tgtCollection.find().sort('_id').batch_size(batch_size)

    total_docs = min(srcCollection.count_documents({}), tgtCollection.count_documents({}))
    progress_bar = tqdm(total=total_docs, desc='Comparing documents', unit='doc')

    while cursor1.alive or cursor2.alive:
        batch1 = [next(cursor1, None) for _ in range(batch_size)]
        batch2 = [next(cursor2, None) for _ in range(batch_size)]

        for doc1, doc2 in zip(batch1, batch2):
            # Case for None
            if doc1 is None or doc2 is None:
                break

            diff = DeepDiff(doc1, doc2, verbose_level=2, report_repetition=True, cache_size=10000)
            if diff:
                print("Difference found at doc id", doc1["_id"])
                print(diff)
                write_difference_to_file(output_file, doc1["_id"])
                write_difference_to_file(output_file, diff)

            progress_bar.update(1)

    progress_bar.close()

def write_difference_to_file(output_file, content):
    with open(output_file, 'a') as file:
        file.write(str(content) + '\n')

def compare_collections(srcCollection, tgtCollection, batch_size, output_file):
    count1 = srcCollection.count_documents({})
    count2 = tgtCollection.count_documents({})

    if count1 != count2:
        compare_document_counts(srcCollection, tgtCollection, output_file)
    else:
        compare_document_data(srcCollection, tgtCollection, batch_size, output_file)

def main():
    parser = argparse.ArgumentParser(description='Compare MongoDB collections and report differences.')
    parser.add_argument('--batch_size', type=int, default=1000, help='Batch size for bulk reads (default: 1000)')
    parser.add_argument('--output_file', type=str, default='differences.txt', help='Output file path (default: differences.txt)')
    args = parser.parse_args()

    cluster1_uri = os.environ.get('SOURCE_URI')
    cluster2_uri = os.environ.get('TARGET_URI')
    srcDatabase = os.environ.get('SOURCE_DB')
    tgtDatabase = os.environ.get('TARGET_DB')
    srcCollection = os.environ.get('SOURCE_COLL')
    tgtCollection = os.environ.get('TARGET_COLL')

    # Connect to the source database cluster
    cluster1_client = connect_to_db(cluster1_uri, 50)
    srcdb = cluster1_client[srcDatabase]
    srcCollection = srcdb[srcCollection]

    # Connect to the target database cluster
    cluster2_client = connect_to_db(cluster2_uri, 50)
    tgtdb = cluster2_client[tgtDatabase]
    tgtCollection = tgtdb[tgtCollection]

    # Compare collections and report differences
    compare_collections(srcCollection, tgtCollection, args.batch_size, args.output_file)

if __name__ == '__main__':
    main()
