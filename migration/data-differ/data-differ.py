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
def check_target_for_extra_documents(srcCollection, tgtCollection, output_file):
    print("Check if extra documents exist in target database.Scanning......")
    missing_docs = tgtCollection.find({'_id': {'$nin': srcCollection.distinct('_id')}})
    if len(list(missing_docs.clone()))  > 0 :
        write_difference_to_file(output_file, "Document _IDs present in the target collection but not in the source collection:")
        for doc in missing_docs:
            print(doc['_id'])
            write_difference_to_file(output_file, doc['_id'])

def compare_docs_deepdiff(doc1, doc2, output_file):
    #print("doing DeepDiff")
    diff = DeepDiff(doc1, doc2, verbose_level=2, report_repetition=True, ignore_order=True)
    if diff:
        #print("Difference found at doc id :", doc1["_id"])
        write_difference_to_file(output_file, "Difference found at doc id :" + str(doc1["_id"]))
        write_difference_to_file(output_file, diff)
        
## Compare document data, open a cursor for both collections, stream batches of data based on batch_size
def compare_document_data(srcCollection, tgtCollection, batch_size, output_file,src_count):
    print("Compare document data, open a cursor for both collections, stream batches of data based on batch_size")
    source_cursor = srcCollection.find().sort('_id').batch_size(batch_size)
    #cursor2 = tgtCollection.find().sort('_id').batch_size(batch_size)
    total_docs = src_count
    progress_bar = tqdm(total=total_docs, desc='Comparing documents', unit='doc')
    tgt_missing_ids=[]
    while source_cursor.alive :
        batch1 = [next(source_cursor, None) for _ in range(batch_size)]
        doc_pairs=[]
        for document in batch1:
            if document is not None:
                source_id = document['_id']
                queried_doc = tgtCollection.find_one({"_id": document["_id"]})
                if queried_doc == None:
                    #print("source document _id not found in target :",source_id)
                    tgt_missing_ids.append(source_id)
                    continue
                else :
                    doc_pairs.append((document,queried_doc))
                #compare_docs_deepdiff(document,queried_doc,output_file)
        pool_size = os.cpu_count()
        with Pool(pool_size) as pool:
            pool.starmap(compare_docs_deepdiff, [(doc1, doc2, output_file) for doc1, doc2 in doc_pairs])
        progress_bar.update(batch_size)
    if len(tgt_missing_ids) > 0 :
        print(f"Document count mismatch: Found {len(tgt_missing_ids)} documents in the source collection that are missing in the target collection")
        write_difference_to_file(output_file, "Document _IDs present in the source collection but not in target collection:")
        for doc in tgt_missing_ids:
            write_difference_to_file(output_file, str(doc))
        
    progress_bar.close()


# Compare indexes between the two collections
def compare_indexes(srcCollection, tgtCollection, output_file):
    print("Compare indexes between the two collections")
    src_indexes = srcCollection.index_information()
    tgt_indexes = tgtCollection.index_information()

    diff = DeepDiff(src_indexes, tgt_indexes, verbose_level=2, report_repetition=True, ignore_order=True)
    if diff:
        write_difference_to_file(output_file, "Index differences:")
        write_difference_to_file(output_file, diff)


def write_difference_to_file(output_file, content):
    with open(output_file, 'a') as file:
        file.write(str(content) + '\n')


def compare_collections(srcCollection, tgtCollection, batch_size, output_file, check_target):
    src_count = srcCollection.count_documents({})
    trg_count = tgtCollection.count_documents({})
    
    if src_count == 0:
        print("No documents found in the source collection, please re-check you selected the right source collection.")
        return
    elif trg_count == 0:
        print("No documents found in the target collection, please re-check you selected the right target collection.")
        return
    write_difference_to_file(output_file, "Count of documents in source:" + str(src_count) )
    write_difference_to_file(output_file, "Count of documents in target:" + str(trg_count) )

    compare_document_data(srcCollection, tgtCollection, batch_size, output_file,src_count)
    compare_indexes(srcCollection, tgtCollection, output_file)
    if check_target :
        check_target_for_extra_documents(srcCollection, tgtCollection, output_file)


def main():
    parser = argparse.ArgumentParser(description='Compare two collections and report differences.')
    parser.add_argument('--batch_size', type=int, default=1000, help='Batch size for bulk reads (default: 1000)')
    parser.add_argument('--output_file', type=str, default='differences.txt', help='Output file path (default: differences.txt)')
    parser.add_argument('--check_target', type=str, default=False, help='Check if extra documents exist in target database')
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
    compare_collections(srcCollection, tgtCollection, args.batch_size, args.output_file, args.check_target)

if __name__ == '__main__':
    main()
