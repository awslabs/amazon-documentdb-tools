import bson
import pymongo
import time
import concurrent.futures
import csv
import argparse
import sys
import logging
import signal
import sys
from threading import Lock
from datetime import datetime

shutdown_flag = False

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)
logger = logging.getLogger(__name__)

class BatchCounter:
    def __init__(self, total_docs):
        self.docs_processed = 0
        self.start_time = time.time()
        self.last_log_time = self.start_time
        self.last_processed = 0 
        self.lock = Lock()
        self.total_docs = total_docs
        self.large_docs_count = 0
        
        header = f"{'Time':19} | {'Total docs':>11} | {'Processed':>11} | {'Perc':>6} | {'Elapsed':>8} | {'Docs/sec':>10} | {'Large docs':>10}"
        print(header)

    def increment(self, docs_in_batch, new_large_docs=0):
        current_time = time.time()
        with self.lock:
            self.docs_processed += docs_in_batch
            self.large_docs_count += new_large_docs
            self.last_processed += docs_in_batch
            
            if (current_time - self.last_log_time >= 10) or (self.last_processed >= 100000):
                elapsed = current_time - self.start_time
                elapsed_str = time.strftime('%H:%M:%S', time.gmtime(elapsed))
                percentage = (self.docs_processed / self.total_docs) * 100
                docs_per_sec = int(self.docs_processed / elapsed) if elapsed > 0 else 0
                
                current_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                line = (f"{current_time_str} | {self.total_docs:>11,d} | {self.docs_processed:>11,d} | "
                       f"{percentage:>5.1f}% | {elapsed_str} | {docs_per_sec:>10,d} | {self.large_docs_count:>10d}")
                print(line)
                
                self.last_log_time = current_time
                self.last_processed = 0 
                sys.stdout.flush()

class DocumentProcessor:
    _last_id_lock = Lock()
    _last_processed_id = None
    
    def __init__(self, collection, size_threshold, batch_counter, batch_size):
        self.collection = collection
        self.size_threshold = size_threshold
        self.batch_counter = batch_counter
        self.batch_size = batch_size

    def get_next_batch(self):
        with DocumentProcessor._last_id_lock:
            query = {"_id": {"$gt": DocumentProcessor._last_processed_id}} if DocumentProcessor._last_processed_id else {}
            
            docs = list(self.collection.find(
                query,
                {"_id": 1},
                limit=self.batch_size,
                sort=[("_id", 1)]
            ).hint("_id_"))
            
            if not docs:
                return None, None
                
            start_id = docs[0]["_id"]
            end_id = docs[-1]["_id"]
            DocumentProcessor._last_processed_id = end_id
            
            return start_id, end_id

    def process_batch(self, batch_cursor):
        while True:
            if shutdown_flag:
                break
                
            start_id, end_id = self.get_next_batch()
            if not start_id:
                break
                
            batch_large_docs = []
            doc_count = 0
            
            try:
                query = {
                    "_id": {
                        "$gte": start_id,
                        "$lte": end_id
                    }
                }
                
                batch_cursor = self.collection.find(query).hint("_id_")
                
                for doc in batch_cursor:
                    if shutdown_flag:
                        break
                        
                    doc_count += 1
                    doc_id = doc["_id"]
                    
                    try:
                        size = get_bson_size(doc)
                        
                        if size > self.size_threshold:
                            batch_large_docs.append((doc_id, size))
                            
                    except Exception as e:
                        if not shutdown_flag:
                            logger.error(f"Error processing document {doc_id}: {str(e)}")
                    
                    doc = None
                
                if doc_count > 0 or batch_large_docs:
                    self.batch_counter.increment(doc_count, len(batch_large_docs))
                    yield True, batch_large_docs
                    
            except Exception as e:
                logger.error(f"Error processing batch: {str(e)}")
                yield False, []
                
            batch_cursor = None


def signal_handler(signum, frame):
    global shutdown_flag
    print('\nShutdown requested. Cleaning up...', flush=True)
    shutdown_flag = True

def validate_config(config):
    # Validate provided parameters
    if not config.get('uri'):
        raise ValueError("URI cannot be empty")
        
    # Validate URI format
    try:
        uri = config['uri']
        if not uri.startswith(('mongodb://', 'mongodb+srv://')):
            raise ValueError("URI must start with 'mongodb://' or 'mongodb+srv://'")
            
        if '@' in uri:
            host_part = uri.split('@')[1].split('/')[0]
        else:
            host_part = uri.split('//')[1].split('/')[0]
            
        if not host_part:
            raise ValueError("Invalid URI: missing host")
            
    except IndexError:
        raise ValueError("Invalid URI format")
        
    if not isinstance(config['batchSize'], int) or config['batchSize'] <= 0:
        raise ValueError("Batch size must be a positive integer")
        
    if not isinstance(config['numProcesses'], int) or config['numProcesses'] <= 0:
        raise ValueError("Number of processes must be a positive integer")
        
    if not isinstance(config['largeDocThreshold'], int) or config['largeDocThreshold'] <= 0:
        raise ValueError("Large document threshold must be a positive integer")
    
    # Check for unreasonable memory requirements
    if config['batchSize'] * config['numProcesses'] > 1000000:
        logger.warning("High memory usage configuration detected")

    # Validate string parameters
    if not config.get('databaseName'):
        raise ValueError("Database name cannot be empty")
        
    if not config.get('collectionName'):
        raise ValueError("Collection name cannot be empty")

def create_id_ranges(batch_size, collection, total_docs):
    if total_docs == 0:
        return []

    # Just getting min_id
    pipeline = [
        {"$project": {"_id": 1}},
        {"$sort": {"_id": 1}},
        {"$limit": 1}
    ]
    
    result = list(collection.aggregate(pipeline, hint="_id_"))
    if not result:
        return []

    # Each thread will use this as a starting point to create their own range
    return [(result[0]["_id"], None)]

def write_to_csv(filename, data, mode='a', batch_size=1000):
    try:
        with open(filename, mode, newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            
            if isinstance(data, list):
                csv_writer.writerows(data)
            else:
                csv_writer.writerow(data)
                
    except IOError as e:
        logger.error(f"Error writing to CSV file: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error writing to CSV: {str(e)}")
        raise

def get_bson_size(document):
    try:
        return len(bson.BSON.encode(document))
    except Exception as e:
        logger.error(f"Error calculating BSON size: {str(e)}")
        return None

def get_collection_doc_count(collection):
    try:
        stats = collection.database.command('collStats', collection.name)
        return stats['count']
    except Exception as e:
        logger.error(f"Error getting document count from stats: {str(e)}")
        raise

def process_future_results(future, large_docs_data):
    large_docs_count = 0
    try:
        for success, batch_large_docs in future.result():
            if batch_large_docs:
                large_docs_count += len(batch_large_docs)
                for doc_id, size in batch_large_docs:
                    large_docs_data.append((str(doc_id), size, f"{size / (1024*1024):.2f}"))
    except Exception as e:
        logger.error(f"Error processing future results: {str(e)}")
    return large_docs_count

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    start_time = time.time()
    start_time_iso = datetime.fromtimestamp(start_time).isoformat()

    parser = argparse.ArgumentParser(description='Large Document Finder')
    parser.add_argument('--uri', required=True, type=str, help='URI (connection string)')
    parser.add_argument('--processes', required=True, type=int, help='Number of threads')
    parser.add_argument('--batch-size', required=True, type=int, help='Number of documents per batch')
    parser.add_argument('--database', required=True, type=str, help='Database name')
    parser.add_argument('--collection', required=True, type=str, help='Collection name')
    parser.add_argument('--csv', default='large_doc_', type=str, help='Prefix for the CSV output filename')
    parser.add_argument('--large-doc-size', type=int, default=8388608, help='Large document size threshold in bytes (default 8388608 - 8MB)')

    args = parser.parse_args()

    appConfig = {
        'uri': args.uri,
        'numProcesses': int(args.processes),
        'batchSize': int(args.batch_size),
        'databaseName': args.database,
        'collectionName': args.collection,
        'csvName': args.csv,
        'largeDocThreshold': int(args.large_doc_size)
    }

    try:
        validate_config(appConfig)
    except ValueError as e:
        logger.error(f"Configuration error: {str(e)}")
        sys.exit(1)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"{appConfig['csvName']}{timestamp}.csv"
    large_docs_found = 0
    large_docs_data = []

    logger.info("Connecting to database...")
    
    try:
        client = pymongo.MongoClient(appConfig['uri'])
        client.admin.command('ping')
        db = client[appConfig['databaseName']]
        col = db[appConfig['collectionName']]

        total_docs = get_collection_doc_count(col)
        logger.info(f"Total Documents: {total_docs:,}")

        batch_size = appConfig['batchSize']
        num_batches = (total_docs + batch_size - 1) // batch_size

        logger.info(f"Creating {num_batches:,} segments with {batch_size:,} docs per segment")

        logger.info('Starting document scan...')

        metadata = [
            ['Database', appConfig['databaseName']],
            ['Collection', appConfig['collectionName']],
            ['Batch size', batch_size],
            ['Number of threads', appConfig['numProcesses']],
            ['Total documents', total_docs],
            ['Large document threshold (bytes)', appConfig['largeDocThreshold']],
            ['Large document threshold (MB)', f"{appConfig['largeDocThreshold'] / (1024*1024):.2f}"],
            ['Scan Start Time', datetime.now().isoformat()],
        ]

        batch_counter = BatchCounter(total_docs) 
        processor = DocumentProcessor(col, appConfig['largeDocThreshold'], batch_counter=batch_counter, batch_size=batch_size)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=appConfig['numProcesses']) as executor:
            futures = set()
            large_docs_found = 0
                
            for _ in range(appConfig['numProcesses']):
                if shutdown_flag:
                    print('\nStopping batch submission...', flush=True)
                    break

                processor = DocumentProcessor(col, appConfig['largeDocThreshold'], 
                                        batch_counter=batch_counter, 
                                        batch_size=batch_size)
                future = executor.submit(processor.process_batch, None)
                futures.add(future)

            for future in concurrent.futures.as_completed(futures):
                if shutdown_flag:
                    print('\nCancelling remaining tasks...', flush=True)
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break

                large_docs_found += process_future_results(future, large_docs_data)

        end_time = time.time()
        duration_str = time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))

    except pymongo.errors.PyMongoError as e: 
        logger.error(f"DocumentDB error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Main loop error: {str(e)}")
        if 'client' in locals():
            client.close()
    
    finally:
        if 'client' in locals():
            client.close()
        print('\n\n', flush=True)

        end_time = time.time()
        duration_str = time.strftime('%H:%M:%S', time.gmtime(end_time - start_time))

        summary = {
            'large_docs_found': large_docs_found
        }
        
        if not shutdown_flag:
            threshold_mb = appConfig['largeDocThreshold'] / (1024 * 1024)

            try:
                metadata = [
                    ['Database', appConfig['databaseName']],
                    ['Collection', appConfig['collectionName']],
                    ['Batch size', batch_size],
                    ['Number of threads', appConfig['numProcesses']],
                    ['Total documents', total_docs],
                    ['Large document threshold (bytes)', appConfig['largeDocThreshold']],
                    ['Large document threshold (MB)', f"{appConfig['largeDocThreshold'] / (1024*1024):.2f}"],
                    ['Scan Start Time', start_time_iso],
                    ['Scan completion time', datetime.fromtimestamp(end_time).isoformat()],
                    ['Total scan time', duration_str],
                    ['Large documents found', large_docs_found],
                    [],
                    ['Document _id', 'Size (bytes)', 'Size (MB)']
                ]

                write_to_csv(csv_filename, metadata, mode='w')
                
                if large_docs_data:
                    write_to_csv(csv_filename, large_docs_data, mode='a')

            except Exception as e:
                logger.error(f"Failed to write to CSV: {str(e)}")
            
            print("=" * 80)
            print("Scan complete")
            print("=" * 80)
            print(f"Total documents processed: {total_docs:,}")
            print(f"Documents larger than {threshold_mb:.0f}MB: {large_docs_found:,}")
            print(f"Total scan time: {duration_str}") 
            if large_docs_found > 0:
                print(f"Large documents have been written to: {csv_filename}")
            print("=" * 80)
        else:
            print("\nScript terminated by user")

if __name__ == "__main__":
    main()