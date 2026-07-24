import pymongo
import json
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from cardinality_analyzer import analyze_index_cardinality_with_db

logger = logging.getLogger(__name__)


def get_documentdb_stats(connection_params, progress_callback=None):
    """Connect to DocumentDB and get collection statistics"""
    def _report(phase, detail, pct):
        if progress_callback:
            progress_callback(phase, detail, pct)

    try:
        _report('Connecting', 'Establishing connection to DocumentDB...', 0)
        # Bounded timeouts so a degraded/half-dead tunnel cannot hang the agent
        # thread indefinitely. Without these, an in-flight socket read on a stale
        # forward blocks until the ssh process fully dies (observed 17-33 min
        # stalls). 45s socket timeout is generous for metadata ops (collStats,
        # $indexStats, list_indexes) and the bounded find(limit) cardinality
        # sample, while bounding worst-case hang. Per-collection try/except lets
        # db_analysis log the failure and continue.
        client = pymongo.MongoClient(
            connection_params['connection_string'],
            appname='DocDB-Prism',
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=45000,
        )
        client.admin.command('ping')
        _report('Connecting', 'Connection established ✅', 5)

        database_name = connection_params['database_name']
        db = client[database_name]

        # Optional reader/secondary handle for offloading document sampling
        # (cardinality). Metadata ($indexStats, collStats, list_indexes) ALWAYS
        # stays on the primary. The reader connection string is resolved ONCE per
        # agent run by the caller and passed in here (we do NOT discover the
        # cluster per-database). A short socket timeout means a degraded/half-dead
        # reader fails fast and we fall back to primary sampling — so db_analysis
        # can never be slower or less reliable than the primary-only path.
        reader_client = None
        sample_db = None
        reader_conn = connection_params.get('reader_connection_string')
        if reader_conn:
            try:
                reader_client = pymongo.MongoClient(
                    reader_conn, appname='DocDB-Prism',
                    serverSelectionTimeoutMS=5000, connectTimeoutMS=5000,
                    socketTimeoutMS=8000)
                sample_db = reader_client[database_name]
                logger.info("db_analyzer[%s]: sampling cardinality from reader", database_name)
            except Exception as e:
                logger.debug("db_analyzer[%s]: reader offload unavailable: %s",
                             database_name, e)
                reader_client = None
                sample_db = None

        if connection_params.get('collections'):
            collections = connection_params['collections']
        else:
            _report('Discovery', 'Listing collections...', 8)
            collections = db.list_collection_names()
            _report('Discovery', f'Found {len(collections)} collections', 10)

        # Filter out system collections only — include all user collections regardless of size
        sized_colls = []
        skipped = []
        for cname in collections:
            if cname.startswith('system.'):
                skipped.append(cname)
                continue
            try:
                stats = db.command("collStats", cname)
                sized_colls.append((cname, stats))
            except Exception:
                sized_colls.append((cname, None))

        if skipped:
            _report('Discovery', f'Skipping {len(skipped)} small/system collections', 12)

        result = {database_name: {}}
        total = len(sized_colls)
        completed = [0]  # mutable counter for thread-safe progress
        lock = __import__('threading').Lock()

        def _analyze_collection(cname, prefetched_stats):
            """Analyze a single collection — runs in thread pool."""
            try:
                collection = db[cname]
                storage_stats = prefetched_stats or db.command("collStats", cname)

                compression_info = storage_stats.get('compression', {})
                compression_enabled = compression_info.get('enable', False)
                compression_threshold = compression_info.get('threshold', 0)

                # Index stats + usage in one pass
                indexes = []
                index_usage_stats = {}

                try:
                    index_stats = list(collection.aggregate([{"$indexStats": {}}]))
                    for idx_stat in index_stats:
                        index_name = idx_stat.get('name', 'unknown')
                        idx_unused = idx_stat.get('unusedStorageSize', {})
                        index_usage_stats[index_name] = {
                            'accesses': idx_stat.get('accesses', {}),
                            'host': idx_stat.get('host', 'unknown'),
                            'size': idx_stat.get('size', 0),
                            'unusedSizeBytes': idx_unused.get('unusedSizeBytes', 0),
                            'unusedSizePercent': idx_unused.get('unusedSizePercent', 0.0)
                        }
                except Exception:
                    pass

                for index_info in collection.list_indexes():
                    index_name = index_info.get('name', 'unknown')
                    index_keys = index_info.get('key', {})

                    usage_info = index_usage_stats.get(index_name, {})
                    accesses = usage_info.get('accesses', {})
                    ops_count = accesses.get('ops', 0)
                    since_date = accesses.get('since', None)

                    logger.info("index usage %s.%s: ops=%s since=%r (type=%s)",
                                cname, index_name, ops_count, since_date,
                                type(since_date).__name__)

                    # 'since' is the time the usage counter last started tracking
                    # (it resets on instance restart/failover), so it is shown to
                    # the user as context for the ops count rather than used to
                    # gate detection. Normalize it to an ISO date string.
                    since_str = None
                    if since_date:
                        try:
                            if isinstance(since_date, datetime):
                                since_str = since_date.date().isoformat()
                            else:
                                since_str = str(since_date).split("T", 1)[0]
                        except Exception as e:
                            logger.debug("index %s since parse failed: %s",
                                         index_name, e)

                    # Unused signal: zero operations on a droppable (non-_id) index.
                    is_system_idx = index_name in ('_id', '_id_')
                    potential_unused = (ops_count == 0 and not is_system_idx)

                    indexes.append({
                        'name': index_name,
                        'size': usage_info.get('size', 0),
                        'fields': index_keys,
                        'ordered_fields': [(field, direction) for field, direction in index_keys.items()],
                        'usage': {
                            'ops_count': ops_count,
                            'since': since_date,
                            'since_date': since_str,
                            'potential_unused': potential_unused
                        }
                    })

                # Cardinality — reuse existing db handle
                try:
                    logger.info("db_analyzer: %s cardinality start", cname)
                    cardinality_results = analyze_index_cardinality_with_db(
                        db, cname, sample_size=500, sample_db=sample_db
                    )
                    logger.info("db_analyzer: %s cardinality done", cname)
                    if isinstance(cardinality_results, list):
                        cardinality_map = {r['index_name']: r for r in cardinality_results}
                        for idx in indexes:
                            cd = cardinality_map.get(idx['name'], {})
                            idx['cardinality'] = {
                                'percentage': cd.get('cardinality_percentage', 0),
                                'is_low': cd.get('is_low_cardinality', False),
                                'distinct_values': cd.get('distinct_values', 0),
                                'total_sampled': cd.get('total_docs_sampled', 0)
                            }
                except Exception:
                    for idx in indexes:
                        idx['cardinality'] = {'percentage': 0, 'is_low': False, 'distinct_values': 0, 'total_sampled': 0}

                # Bloat from $indexStats
                for idx in indexes:
                    idx_stats = index_usage_stats.get(idx['name'], {})
                    idx['bloat'] = {
                        'unusedBytes': idx_stats.get('unusedSizeBytes', 0),
                        'unusedPercent': idx_stats.get('unusedSizePercent', 0.0)
                    }

                unused_storage = storage_stats.get('unusedStorageSize', {})

                return cname, {
                    'size': storage_stats.get('size', 0),
                    'storageSize': storage_stats.get('storageSize', 0),
                    'avgObjSize': storage_stats.get('avgObjSize', 0),
                    'count': storage_stats.get('count', 0),
                    'unusedStorageSize': {
                        'unusedBytes': unused_storage.get('unusedBytes', 0),
                        'unusedPercent': unused_storage.get('unusedPercent', 0.0)
                    },
                    'compression': {'enabled': compression_enabled, 'threshold': compression_threshold},
                    'indexes': indexes,
                    'index_analysis': {
                        'total_indexes': len(indexes),
                        'unused_indexes': [idx for idx in indexes if idx['usage']['potential_unused']],
                        'recent_indexes': [],
                        'low_cardinality_indexes': [idx for idx in indexes if idx.get('cardinality', {}).get('is_low', False)]
                    }
                }

            except Exception as e:
                return cname, {
                    'error': str(e),
                    'size': 0,
                    'avgObjSize': 0,
                    'compression': {'enabled': False, 'threshold': 0},
                    'indexes': [],
                    'index_analysis': {'total_indexes': 0, 'unused_indexes': [], 'recent_indexes': []}
                }

        # Gentle parallelism — 2 threads to overlap network latency without hammering DB
        logger.info("db_analyzer[%s]: submitting %d collection(s) to pool",
                    database_name, len(sized_colls))
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {pool.submit(_analyze_collection, cname, stats): cname
                       for cname, stats in sized_colls}

            for future in as_completed(futures):
                cname, coll_result = future.result()
                result[database_name][cname] = coll_result
                with lock:
                    completed[0] += 1
                    pct = 10 + int((completed[0] / max(total, 1)) * 85)
                    _report(f'Analyzing ({completed[0]}/{total})', f'📊 {cname}', pct)
            logger.info("db_analyzer[%s]: all %d collection(s) returned; closing pool",
                        database_name, len(sized_colls))

        _report('Complete', 'Analysis complete ✅', 100)
        client.close()
        if reader_client is not None:
            try:
                reader_client.close()
            except Exception:
                pass
        return result

    except Exception as e:
        if progress_callback:
            progress_callback('Error', str(e), 0)
        return {'error': str(e)}
