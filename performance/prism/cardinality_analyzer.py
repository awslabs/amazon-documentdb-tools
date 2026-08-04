"""Index Cardinality Analyzer — single sample pass per collection.

Samples documents once, then computes cardinality for all indexes from
the same sample set. ~90% less IO than per-index sampling.

Note: $sample is not supported on all DocumentDB configurations.
We attempt it once and permanently fall back to find+limit if it fails.
"""
import logging

logger = logging.getLogger(__name__)

# Set to True after first $sample failure — avoids repeated attempts and log spam
_sample_unsupported = False


def _fetch_sample(collection, db, collection_name, sample_size):
    """Fetch sample docs. Uses $sample if supported, otherwise find+limit."""
    global _sample_unsupported
    if not _sample_unsupported:
        try:
            docs = list(collection.aggregate([{"$sample": {"size": sample_size}}]))
            return docs
        except Exception as e:
            logger.warning(
                "$sample not supported on this cluster (%s) — switching permanently to find+limit", e
            )
            _sample_unsupported = True
    # find+limit: fast, no skip needed for cardinality estimation
    try:
        return list(collection.find({}, limit=sample_size))
    except Exception as e2:
        logger.debug("find+limit also failed for %s: %s", collection_name, e2)
        return []


def analyze_index_cardinality_with_db(db, collection_name, sample_size=500, threshold=1.0,
                                      sample_db=None):
    """Analyze index cardinality for all indexes in one $sample pass.

    1. Fetch all non-system indexes (from `db` — primary metadata)
    2. Run ONE $sample pipeline to get sample_size documents
    3. For each index, compute distinct value count from the sample
    4. Return cardinality results for all indexes

    sample_db: optional separate db handle (e.g. a reader/secondary) used ONLY
    for the document sampling, to offload read load from the writer. Index
    metadata is always read from `db`. If sampling against sample_db yields no
    docs, falls back to sampling against `db`.
    """
    collection = db[collection_name]

    # Gather non-system indexes
    idx_list = []
    try:
        for index in collection.list_indexes():
            if index['name'] in ('_id', '_id_'):
                continue
            idx_list.append({
                'name': index['name'],
                'key': index['key'],
                'fields': list(index['key'].keys()),
            })
    except Exception as e:
        logger.warning("list_indexes failed for %s: %s", collection_name, e)
        return []

    if not idx_list:
        return []

    # Single $sample pass — project only the fields we need
    all_fields = set()
    for idx in idx_list:
        all_fields.update(idx['fields'])

    projection = {f: 1 for f in all_fields}
    projection['_id'] = 0

    # Offload sampling to the reader/secondary when provided; fall back to the
    # primary handle if the reader returns nothing (or isn't supplied).
    sample_docs = []
    if sample_db is not None:
        try:
            sample_docs = _fetch_sample(sample_db[collection_name], sample_db,
                                        collection_name, sample_size)
        except Exception as e:
            logger.debug("reader sampling failed for %s; using primary: %s",
                         collection_name, e)
            sample_docs = []
    if not sample_docs:
        sample_docs = _fetch_sample(collection, db, collection_name, sample_size)

    if not sample_docs:
        return [_empty_result(idx, sample_size, threshold) for idx in idx_list]

    total = len(sample_docs)
    if total == 0:
        return [_empty_result(idx, sample_size, threshold) for idx in idx_list]

    # Compute cardinality for each index from the shared sample
    results = []
    for idx in idx_list:
        fields = idx['fields']
        try:
            # Build tuple keys for distinct counting
            distinct_values = set()
            for doc in sample_docs:
                key = tuple(_get_nested(doc, f) for f in fields)
                # Skip all-None keys
                if all(v is None for v in key):
                    continue
                distinct_values.add(key)

            distinct = len(distinct_values)
            if distinct > 0:
                cardinality = (distinct / total) * 100
            else:
                cardinality = 0

            results.append({
                'index_name': idx['name'],
                'index_keys': idx['key'],
                'cardinality_percentage': round(cardinality, 4),
                'is_low_cardinality': cardinality < threshold,
                'total_docs_sampled': total,
                'distinct_values': distinct,
                'sample_size': sample_size,
                'threshold': threshold,
            })
        except Exception as e:
            results.append(_empty_result(idx, sample_size, threshold, str(e)))

    return results


def _get_nested(doc, field_path):
    """Get a possibly nested field value (supports dot notation)."""
    parts = field_path.split('.')
    val = doc
    for part in parts:
        if isinstance(val, dict):
            val = val.get(part)
        else:
            return None
    # Make hashable for set operations
    if isinstance(val, (list, dict)):
        try:
            import json
            return json.dumps(val, sort_keys=True, default=str)
        except Exception:
            return str(val)
    return val


def _empty_result(idx, sample_size, threshold, error=None):
    """Return a zero-cardinality result for an index."""
    r = {
        'index_name': idx['name'],
        'index_keys': idx['key'],
        'cardinality_percentage': 0,
        'is_low_cardinality': False,
        'total_docs_sampled': 0,
        'distinct_values': 0,
        'sample_size': sample_size,
        'threshold': threshold,
    }
    if error:
        r['error'] = error
    return r


# Legacy function — kept for backward compatibility
def analyze_index_cardinality(connection_string, database_name, collection_name,
                               sample_size=500, threshold=1.0):
    """Analyze index cardinality for a collection (opens its own connection)."""
    import pymongo
    try:
        client = pymongo.MongoClient(connection_string, appname='DocDB-Prism')
        db = client[database_name]
        results = analyze_index_cardinality_with_db(db, collection_name, sample_size, threshold)
        client.close()
        return results
    except Exception as e:
        return {'error': str(e)}
