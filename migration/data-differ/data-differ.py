import os
import argparse
from pymongo import MongoClient
from deepdiff import DeepDiff
from tqdm import tqdm
from multiprocessing import Pool

def connect_to_db(uri, pool_size):
    client = MongoClient(uri, maxPoolSize=pool_size)
    return client

## Compare doc count, if not equal try to find the missing docs
## Needs optimisation for DocumentDB
def compare_document_counts(srcCollection, tgtCollection, output_file):
    count1 = srcCollection.count_documents({})
    count2 = tgtCollection.count_documents({})

    if count1 == 0:
        print("No documents found in the source collection, please re-check you selected the right source collection.")
        return
    elif count2 == 0:
        print("No documents found in the target collection, please re-check you selected the right target collection.")
        return
    elif count1 > count2:
        print(f"Document count mismatch: Found {count1} documents in the source collection vs. {count2} documents in the target collection, scanning...")
        missing_docs = srcCollection.find({'_id': {'$nin': tgtCollection.distinct('_id')}})
        write_difference_to_file(output_file, "Document _IDs present in the source collection but not in target collection:")
    else:
        print(f"Document count mismatch: Found {count1} documents in the source collection vs. {count2} documents in the target collection, scanning...")
        missing_docs = tgtCollection.find({'_id': {'$nin': srcCollection.distinct('_id')}})
        write_difference_to_file(output_file, "Document _IDs present in the target collection but not in the source collection:")

    for doc in missing_docs:
        print(doc['_id'])
        write_difference_to_file(output_file, doc['_id'])


def compare_docs_deepdiff(doc1, doc2, output_file):
    diff = DeepDiff(doc1, doc2, verbose_level=2, report_repetition=True, ignore_order=True)
    if diff:
        print("Difference found at doc id", doc1["_id"])
        print(diff)
        write_difference_to_file(output_file, doc1["_id"])
        write_difference_to_file(output_file, diff)


## Compare document data, open a cursor for both collections, stream batches of data based on batch_size
def compare_document_data(srcCollection, tgtCollection, batch_size, output_file):
    cursor1 = srcCollection.find().sort('_id').batch_size(batch_size)
    cursor2 = tgtCollection.find().sort('_id').batch_size(batch_size)

    total_docs = min(srcCollection.count_documents({}), tgtCollection.count_documents({}))
    progress_bar = tqdm(total=total_docs, desc='Comparing documents', unit='doc')

    ## Pool size based on CPU count
    pool_size = os.cpu_count()

    with Pool(pool_size) as pool:
        while cursor1.alive or cursor2.alive:
            batch1 = [next(cursor1, None) for _ in range(batch_size)]
            batch2 = [next(cursor2, None) for _ in range(batch_size)]

            # Remove any None values from the batches
            batch1 = [doc for doc in batch1 if doc is not None]
            batch2 = [doc for doc in batch2 if doc is not None]

            # Zip the batches together
            doc_pairs = zip(batch1, batch2)

            # Use the pool of worker processes to compare each document pair
            pool.starmap(compare_docs_deepdiff, [(doc1, doc2, output_file) for doc1, doc2 in doc_pairs])

            progress_bar.update(len(batch1))

    progress_bar.close()


# Compare indexes between the two collections
def compare_indexes(srcCollection, tgtCollection, output_file):
    src_indexes = srcCollection.index_information()
    tgt_indexes = tgtCollection.index_information()

    diff = DeepDiff(src_indexes, tgt_indexes, verbose_level=2, report_repetition=True, ignore_order=True)
    if diff:
        print("Difference found in indexes:")
        print(diff)
        write_difference_to_file(output_file, "Index differences:")
        write_difference_to_file(output_file, diff)


def write_difference_to_file(output_file, content):
    with open(output_file, 'a') as file:
        file.write(str(content) + '\n')


def compare_collections(srcCollection, tgtCollection, batch_size, output_file):
    count1 = srcCollection.count_documents({})
    count2 = tgtCollection.count_documents({})

    compare_indexes(srcCollection, tgtCollection, output_file)

    if count1 != count2:
        compare_document_counts(srcCollection, tgtCollection, output_file)
    else:
        compare_document_data(srcCollection, tgtCollection, batch_size, output_file)


def main():
    parser = argparse.ArgumentParser(description='Compare two collections and report differences.')
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
