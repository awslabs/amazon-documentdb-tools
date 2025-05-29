import argparse
import json
from pymongo import MongoClient
from deepdiff import DeepDiff
from tqdm import tqdm
from datetime import datetime
from multiprocessing import Pool, cpu_count


def connect_to_db(uri, pool_size):
    try:
        client = MongoClient(host=uri, maxPoolSize=pool_size, appname='datadiff')
        return client
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


## Find missing docs in source when doc count in target is higher
def check_target_for_extra_documents(srcCollection, tgtCollection, output_file):
    print("Check if extra documents exist in target database. Scanning......")
    # Using aggregation pipeline instead of distinct to handle dictionary _id values
    src_ids = [doc['_id'] for doc in srcCollection.find({}, {'_id': 1})]
    
    # Find documents in target that don't exist in source
    missing_docs = tgtCollection.find({'_id': {'$nin': src_ids}})
    if len(list(missing_docs.clone())) > 0:
        write_difference_to_file(output_file, "Document _IDs present in the target collection but not in the source collection:")
        for doc in missing_docs:
            print(doc['_id'])
            write_difference_to_file(output_file, doc['_id'])


## Compare documents for any difference using deepDiff
def compare_docs_deepdiff(doc1, doc2, output_file):
    try:
        diff = DeepDiff(doc1, doc2, verbose_level=2, report_repetition=True, ignore_order=True, cache_size=5000)
        if diff:
            print("Difference found at doc id:", doc1["_id"])
            write_difference_to_file(output_file, "Difference found at doc id: " + str(doc1["_id"]))
            write_difference_to_file(output_file, diff)
    except Exception as e:
        print(f"An error occurred while comparing documents: {e}")


## Helper function to make _id hashable
def make_id_hashable(id_value):
    if isinstance(id_value, dict):
        return json.dumps(id_value, sort_keys=True)
    return id_value

## Main compare document function
def compare_document_data(srcCollection, tgtCollection, batch_size, output_file, src_count, sample_size_percent, sampling_timeout_ms):
    if sample_size_percent:
        percentage_in_decimal = sample_size_percent / 100
        docs_to_sample = int(percentage_in_decimal * src_count)
        source_cursor = srcCollection.aggregate([ { "$sample": { "size": docs_to_sample } } ], batchSize=batch_size, maxTimeMS=sampling_timeout_ms)
        total_docs = docs_to_sample
    else:
        source_cursor = srcCollection.find().sort('_id').batch_size(batch_size)
        total_docs = src_count

    progress_bar = tqdm(total=total_docs, desc='Comparing documents', unit='doc')
    tgt_missing_ids = []
    processed_docs = 0

    try:
        while source_cursor.alive:
            batch1 = [next(source_cursor, None) for _ in range(batch_size)]
            doc_pairs = []
            src_ids_list = []

            for document in batch1:
                if document is not None:
                    doc_pairs.append((document, None))  # None is used as a placeholder for target document
                    src_ids_list.append(document['_id'])

            # Use MongoDB's $in operator directly without converting to set
            tgt_docs = tgtCollection.find({"_id": {"$in": src_ids_list}})
            
            # Create a dictionary mapping hashable versions of _id to documents
            tgt_docs_map = {}
            for doc in tgt_docs:
                hashable_id = make_id_hashable(doc['_id'])
                tgt_docs_map[hashable_id] = doc

            # Match source documents with target documents
            matched_doc_pairs = []
            for src_doc in [doc[0] for doc in doc_pairs if doc[0] is not None]:
                hashable_id = make_id_hashable(src_doc['_id'])
                tgt_doc = tgt_docs_map.get(hashable_id)
                matched_doc_pairs.append((src_doc, tgt_doc))
                if tgt_doc is None:
                    tgt_missing_ids.append(src_doc['_id'])

            # Check difference between docs, multi process based on cpu_count()
            pool_size = cpu_count()
            with Pool(pool_size) as pool:
                pool.starmap(compare_docs_deepdiff, [(doc1, doc2, output_file) for doc1, doc2 in matched_doc_pairs if doc2 is not None])

            processed_docs += len(matched_doc_pairs)
            progress_bar.update(len(matched_doc_pairs))

    except Exception as e:
        print(f"An error occurred while comparing documents: {e}")

    if len(tgt_missing_ids) > 0:
        print(f"Found {len(tgt_missing_ids)} documents in the source collection that are missing in the target collection!")
        write_difference_to_file(output_file, "Document _IDs present in the source collection, but not in the target collection:")
        for doc_id in tgt_missing_ids:
            write_difference_to_file(output_file, str(doc_id))

    # Adjust progress bar to the correct count
    progress_bar.n = processed_docs
    progress_bar.refresh()
    progress_bar.close()


# Compare indexes between the two collections
def compare_indexes(srcCollection, tgtCollection, output_file):
    src_indexes = srcCollection.index_information()
    tgt_indexes = tgtCollection.index_information()

    diff = DeepDiff(src_indexes, tgt_indexes, verbose_level=2, report_repetition=True, ignore_order=True)
    if diff:
        print("Found difference in indexes, check the output file! ")
        write_difference_to_file(output_file, "Index differences:")
        write_difference_to_file(output_file, diff)


def write_difference_to_file(output_file, content):
    with open(output_file, 'a') as file:
        file.write(str(content) + '\n')


def compare_collections(srcCollection, tgtCollection, batch_size, output_file, check_target, sample_size_percent, sampling_timeout_ms):
    src_count = srcCollection.count_documents({})
    trg_count = tgtCollection.count_documents({})

    if src_count == 0:
        print("No documents found in the source collection, please re-check you selected the right source collection.")
        return
    if trg_count == 0:
        print("No documents found in the target collection, please re-check you selected the right target collection.")
        return
    if src_count < trg_count:
        if not check_target:
            print(f"Warning: There are more documents in target collection than the source collection, {trg_count} vs. {src_count}. Use --check-target to identify the missing docs in the source collection. ")
    write_difference_to_file(output_file, "Count of documents in source:" + str(src_count) )
    write_difference_to_file(output_file, "Count of documents in target:" + str(trg_count) )

    print(f"Starting data differ at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} , output is saved to {output_file}")
    compare_document_data(srcCollection, tgtCollection, batch_size, output_file, src_count, sample_size_percent, sampling_timeout_ms)
    compare_indexes(srcCollection, tgtCollection, output_file)
    if check_target:
        check_target_for_extra_documents(srcCollection, tgtCollection, output_file)


def main():
    parser = argparse.ArgumentParser(description='Compare two collections and report differences.')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for bulk reads (optional, default: 100)')
    parser.add_argument('--output-file', type=str, default='differences.txt', help='Output file path (optional, default: differences.txt)')
    parser.add_argument('--check-target', action='store_true', default=False, help='optional, Check if extra documents exist in target database')
    parser.add_argument('--source-uri', type=str, required=True, help='Source cluster URI (required)')
    parser.add_argument('--target-uri', type=str, required=True, help='Target cluster URI (required)')
    parser.add_argument('--source-db', type=str, required=True, help='Source database name (required)')
    parser.add_argument('--target-db', type=str, required=True, help='Target database name (required)')
    parser.add_argument('--source-coll', type=str, required=True, help='Source collection name (required)')
    parser.add_argument('--target-coll', type=str, required=True, help='Target collection name (required)')
    parser.add_argument('--sample-size-percent', type=int, required=False, help='optional, if set only samples a percentage of the documents')
    parser.add_argument('--sampling-timeout-ms', type=int, default=500, required=False, help='optional, override the timeout for returning a sample of documents when using the --sample-size-percent argument')
    args = parser.parse_args()

    # Connect to the source database cluster
    cluster1_client = connect_to_db(args.source_uri, 50)
    srcdb = cluster1_client[args.source_db]
    srcCollection = srcdb[args.source_coll]

    # Connect to the target database cluster
    cluster2_client = connect_to_db(args.target_uri, 50)
    tgtdb = cluster2_client[args.target_db]
    tgtCollection = tgtdb[args.target_coll]

    # Compare collections and report differences
    compare_collections(srcCollection, tgtCollection, args.batch_size, args.output_file, args.check_target, args.sample_size_percent, args.sampling_timeout_ms)

if __name__ == '__main__':
    main()
