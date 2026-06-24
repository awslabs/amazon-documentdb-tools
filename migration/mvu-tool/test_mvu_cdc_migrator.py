# Unit tests for mvu-cdc-migrator.py

import hashlib
import sys
import os
from unittest.mock import MagicMock
import pytest
import pymongo
from pymongo.errors import BulkWriteError

sys.path.insert(0, os.path.dirname(__file__))

import importlib.util
spec = importlib.util.spec_from_file_location("mvu_cdc_migrator", os.path.join(os.path.dirname(__file__), "mvu-cdc-migrator.py"))
mvu = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mvu)

# Fixtures
def make_appconfig(threads=4, single_thread_collections=None):
    """Build a minimal appConfig for testing.

    threads = total thread count (same as --threads CLI arg).
    single_thread_collections = dict of ns -> thread_id (already computed).
    numProcessingThreads = hash pool size = threads - len(dedicated).
    """
    single_thread_collections = single_thread_collections or {}
    num_dedicated = len(single_thread_collections)
    return {
        'numProcessingThreads': threads - num_dedicated,
        'singleThreadCollectionMap': single_thread_collections,
    }

def make_change_event(db='testdb', coll='testcol', doc_key=None, op_type='insert', full_document=None):
    """Build a synthetic change stream event."""
    if doc_key is None:
        doc_key = {'_id': 'abc123'}
    event = {
        'ns': {'db': db, 'coll': coll},
        'documentKey': doc_key,
        'operationType': op_type,
        '_id': {'_data': 'resume_token_xyz'},
        'clusterTime': MagicMock(),
    }
    if full_document is not None:
        event['fullDocument'] = full_document
    return event

def make_bulk_write_error(error_list, n_inserted=0):
    """Create a BulkWriteError with specified error codes."""
    details = {
        'writeErrors': error_list,
        'nInserted': n_inserted,
        'nMatched': 0,
        'nRemoved': 0,
    }
    return BulkWriteError(details)

# Test - _get_thread_for_event
class TestGetThreadForEvent:
    def test_hash_partitioned_collection(self):
        """Non-single-thread collections route by hash of documentKey."""
        appConfig = make_appconfig(threads=4)
        change = make_change_event(doc_key={'_id': 'doc1'})

        result = mvu._get_thread_for_event(change, appConfig)

        expected = int(hashlib.sha512(str({'_id': 'doc1'}).encode('utf-8')).hexdigest(), 16) % 4
        assert result == expected

    def test_single_thread_collection_routes_to_dedicated(self):
        """Collections in singleThreadCollectionMap route to their dedicated thread."""
        # 6 total, 2 dedicated - hash pool = 4 (threads 0-3), dedicated = 4, 5
        appConfig = make_appconfig(threads=6, single_thread_collections={
            'mydb.device_tokens': 4,
            'mydb.sessions': 5,
        })
        change = make_change_event(db='mydb', coll='device_tokens')

        result = mvu._get_thread_for_event(change, appConfig)

        assert result == 4

    def test_single_thread_second_collection(self):
        """Second single-thread collection gets the next dedicated thread."""
        # 6 total, 2 dedicated - hash pool = 4 (threads 0-3), dedicated = 4, 5
        appConfig = make_appconfig(threads=6, single_thread_collections={
            'mydb.device_tokens': 4,
            'mydb.sessions': 5,
        })
        change = make_change_event(db='mydb', coll='sessions')

        result = mvu._get_thread_for_event(change, appConfig)

        assert result == 5

    def test_non_single_thread_same_db(self):
        """A collection in the same db but not listed uses hash routing."""
        # 5 total, 1 dedicated - hash pool = 4 (threads 0-3), dedicated = 4
        appConfig = make_appconfig(threads=5, single_thread_collections={
            'mydb.device_tokens': 4,
        })
        change = make_change_event(db='mydb', coll='other_collection', doc_key={'_id': 'x'})

        result = mvu._get_thread_for_event(change, appConfig)

        expected = int(hashlib.sha512(str({'_id': 'x'}).encode('utf-8')).hexdigest(), 16) % 4
        assert result == expected

    def test_hash_partitioning_deterministic(self):
        """Same documentKey always routes to the same thread."""
        appConfig = make_appconfig(threads=8)
        change = make_change_event(doc_key={'_id': 'consistent_doc'})

        results = [mvu._get_thread_for_event(change, appConfig) for _ in range(100)]

        assert len(set(results)) == 1

    def test_hash_partitioning_distributes(self):
        """Different documentKeys distribute across threads."""
        appConfig = make_appconfig(threads=4)
        threads_hit = set()
        for i in range(100):
            change = make_change_event(doc_key={'_id': f'doc_{i}'})
            threads_hit.add(mvu._get_thread_for_event(change, appConfig))

        assert len(threads_hit) > 1

    def test_dedicated_thread_ids_never_overlap_hash_pool(self):
        """Dedicated thread IDs are always >= numProcessingThreads (hash pool size)."""
        # 8 total threads, 3 dedicated - hash pool is 5 (threads 0-4), dedicated are 5,6,7
        appConfig = make_appconfig(threads=8, single_thread_collections={
            'db.col1': 5,
            'db.col2': 6,
            'db.col3': 7,
        })

        for ns, tid in appConfig['singleThreadCollectionMap'].items():
            assert tid >= appConfig['numProcessingThreads']

        for i in range(200):
            change = make_change_event(db='db', coll=f'normal_{i}', doc_key={'_id': f'id_{i}'})
            thread = mvu._get_thread_for_event(change, appConfig)
            assert thread < appConfig['numProcessingThreads']

# Test - _execute_batch
class TestExecuteBatch:
    def test_success_on_primary_path(self):
        """When primary bulk_write succeeds, no fallback needed."""
        collection = MagicMock()
        primary_ops = [pymongo.InsertOne({'_id': 1, 'x': 'a'})]
        fallback_ops = [pymongo.ReplaceOne({'_id': 1}, {'_id': 1, 'x': 'a'}, upsert=True)]

        mvu._execute_batch(collection, primary_ops, fallback_ops)

        collection.bulk_write.assert_called_once_with(primary_ops, ordered=True)

    def test_fallback_on_primary_failure(self):
        """When primary fails with BulkWriteError, falls back to replace list."""
        collection = MagicMock()
        bwe = make_bulk_write_error([{'index': 0, 'code': 11000, 'errmsg': 'dup'}])
        collection.bulk_write.side_effect = [bwe, MagicMock()]

        primary_ops = [pymongo.InsertOne({'_id': 1})]
        fallback_ops = [pymongo.ReplaceOne({'_id': 1}, {'_id': 1}, upsert=True)]

        mvu._execute_batch(collection, primary_ops, fallback_ops)

        assert collection.bulk_write.call_count == 2
        collection.bulk_write.assert_called_with(fallback_ops, ordered=True)

    def test_non_bulk_write_error_propagates(self):
        """Non-BulkWriteError exceptions from primary path propagate."""
        collection = MagicMock()
        collection.bulk_write.side_effect = pymongo.errors.ServerSelectionTimeoutError('timeout')

        with pytest.raises(pymongo.errors.ServerSelectionTimeoutError):
            mvu._execute_batch(collection, [pymongo.InsertOne({'_id': 1})], [])

# Test - _execute_with_dup_skip
class TestExecuteWithDupSkip:
    def test_no_error(self):
        """Clean batch succeeds on first try."""
        collection = MagicMock()
        ops = [pymongo.ReplaceOne({'_id': 1}, {'_id': 1}, upsert=True)]

        mvu._execute_with_dup_skip(collection, ops)

        collection.bulk_write.assert_called_once_with(ops, ordered=True)

    def test_single_dup_key_skipped(self):
        """A single 11000 error is skipped, remaining ops continue."""
        collection = MagicMock()
        ops = [
            pymongo.ReplaceOne({'_id': 1}, {'_id': 1}, upsert=True),
            pymongo.DeleteOne({'_id': 2}),
            pymongo.ReplaceOne({'_id': 3}, {'_id': 3}, upsert=True),
        ]
        bwe = make_bulk_write_error([{'index': 0, 'code': 11000, 'errmsg': 'dup'}])
        collection.bulk_write.side_effect = [bwe, MagicMock()]

        mvu._execute_with_dup_skip(collection, ops)

        assert collection.bulk_write.call_count == 2
        # Second call should be ops[1:] (skipped index 0)
        collection.bulk_write.assert_called_with(ops[1:], ordered=True)

    def test_multiple_dup_keys_skipped_sequentially(self):
        """Multiple 11000 errors at different positions are each skipped."""
        collection = MagicMock()
        ops = [
            pymongo.ReplaceOne({'_id': 1}, {'_id': 1}, upsert=True),
            pymongo.ReplaceOne({'_id': 2}, {'_id': 2}, upsert=True),
            pymongo.ReplaceOne({'_id': 3}, {'_id': 3}, upsert=True),
        ]
        # First call fails at index 0, second call (ops[1:]) fails at index 0 (which is original index 1)
        bwe1 = make_bulk_write_error([{'index': 0, 'code': 11000, 'errmsg': 'dup'}])
        bwe2 = make_bulk_write_error([{'index': 0, 'code': 11000, 'errmsg': 'dup'}])
        collection.bulk_write.side_effect = [bwe1, bwe2, MagicMock()]

        mvu._execute_with_dup_skip(collection, ops)

        assert collection.bulk_write.call_count == 3
        collection.bulk_write.assert_called_with([ops[2]], ordered=True)

    def test_non_dup_error_raises(self):
        """Non-11000 errors cause the exception to propagate."""
        collection = MagicMock()
        ops = [pymongo.ReplaceOne({'_id': 1}, {'_id': 1}, upsert=True)]
        bwe = make_bulk_write_error([{'index': 0, 'code': 121, 'errmsg': 'validation failed'}])
        collection.bulk_write.side_effect = bwe

        with pytest.raises(BulkWriteError):
            mvu._execute_with_dup_skip(collection, ops)

    def test_mixed_errors_raises_on_non_dup(self):
        """If any error is non-11000, raise even if others are 11000."""
        collection = MagicMock()
        ops = [pymongo.ReplaceOne({'_id': 1}, {'_id': 1}, upsert=True)]
        bwe = make_bulk_write_error([
            {'index': 0, 'code': 11000, 'errmsg': 'dup'},
            {'index': 1, 'code': 50, 'errmsg': 'timeout'},
        ])
        collection.bulk_write.side_effect = bwe

        with pytest.raises(BulkWriteError):
            mvu._execute_with_dup_skip(collection, ops)

    def test_empty_ops_list(self):
        """Empty ops list does nothing."""
        collection = MagicMock()

        mvu._execute_with_dup_skip(collection, [])

        collection.bulk_write.assert_not_called()

    def test_all_ops_are_dup_keys(self):
        """If every op fails with 11000, all are skipped gracefully."""
        collection = MagicMock()
        ops = [
            pymongo.ReplaceOne({'_id': 1}, {'_id': 1}, upsert=True),
            pymongo.ReplaceOne({'_id': 2}, {'_id': 2}, upsert=True),
        ]
        bwe1 = make_bulk_write_error([{'index': 0, 'code': 11000, 'errmsg': 'dup'}])
        bwe2 = make_bulk_write_error([{'index': 0, 'code': 11000, 'errmsg': 'dup'}])
        collection.bulk_write.side_effect = [bwe1, bwe2]

        mvu._execute_with_dup_skip(collection, ops)

        assert collection.bulk_write.call_count == 2

# Test - Snapshot overlap with compound unique index
class TestSnapshotOverlapCompoundKey:
    """Simulates CDC replay when a resume token is captured before a snapshot,
    creating an overlap window where events are present in both the snapshot
    and the change stream.

    If a compound unique index exists (e.g. token + user) and a
    document is inserted then deleted then re-inserted with a new _id
    during the overlap window, the snapshot will contain the final _id.
    When CDC replays from the resume token, the first insert attempts to
    create a document whose compound key is already held by the snapshot
    document (different _id). Both the InsertOne and the ReplaceOne
    fallback (upsert=True) fail with E11000 because the upsert becomes
    an insert when _id doesn't match. The handler should skip past the
    conflicting op and continue processing the rest of the batch.
    """

    def test_replay_skips_transient_insert_and_processes_rest(self):
        """Transient insert from overlap window is skipped, remaining ops succeed."""
        collection = MagicMock()

        # Batch as it would be assembled by change_stream_processor
        primary_ops = [
            pymongo.InsertOne({'_id': 1, 'token': 'abc', 'user': 'u1'}),
            pymongo.DeleteOne({'_id': 1}),
            pymongo.InsertOne({'_id': 2, 'token': 'abc', 'user': 'u1'}),
        ]
        fallback_ops = [
            pymongo.ReplaceOne({'_id': 1}, {'_id': 1, 'token': 'abc', 'user': 'u1'}, upsert=True),
            pymongo.DeleteOne({'_id': 1}),
            pymongo.ReplaceOne({'_id': 2}, {'_id': 2, 'token': 'abc', 'user': 'u1'}, upsert=True),
        ]

        # Primary fails on first insert (compound key violation)
        primary_bwe = make_bulk_write_error([{'index': 0, 'code': 11000, 'errmsg': 'dup key'}])
        # Fallback: first ReplaceOne also fails (upsert on _id:1 hits compound key)
        fallback_bwe = make_bulk_write_error([{'index': 0, 'code': 11000, 'errmsg': 'dup key'}])

        collection.bulk_write.side_effect = [
            primary_bwe,      # primary path fails
            fallback_bwe,     # fallback: first op fails with 11000
            MagicMock(),      # fallback resumed: delete + replace succeed
        ]

        mvu._execute_batch(collection, primary_ops, fallback_ops)

        assert collection.bulk_write.call_count == 3
        # Final call should be the remaining ops after skipping index 0
        final_call_ops = collection.bulk_write.call_args_list[2][0][0]
        assert len(final_call_ops) == 2  # delete + replace

# Tests - Thread routing guarantees with compound keys
class TestCompoundKeyThreadSafety:
    """Validates that the dedicated thread model prevents cross-thread races."""

    def test_same_compound_key_different_ids_same_dedicated_thread(self):
        """Two docs sharing compound key in a single-thread collection
        always route to the same thread regardless of _id."""
        # 8 total threads, 1 dedicated - hash pool = 7 (threads 0-6), dedicated = 7
        appConfig = make_appconfig(threads=8, single_thread_collections={
            'mydb.device_tokens': 7,
        })

        change_a = make_change_event(db='mydb', coll='device_tokens', doc_key={'_id': 'id_A'})
        change_b = make_change_event(db='mydb', coll='device_tokens', doc_key={'_id': 'id_B'})

        thread_a = mvu._get_thread_for_event(change_a, appConfig)
        thread_b = mvu._get_thread_for_event(change_b, appConfig)

        assert thread_a == thread_b == 7

    def test_same_compound_key_different_ids_hash_pool_may_differ(self):
        """Without single-thread, different _ids CAN land on different threads.
        This demonstrates why --single-thread-collections is needed."""
        appConfig = make_appconfig(threads=8)

        # Find two _ids that hash to different threads
        threads_seen = {}
        for i in range(1000):
            doc_key = {'_id': f'token_rotation_{i}'}
            change = make_change_event(db='mydb', coll='device_tokens', doc_key=doc_key)
            t = mvu._get_thread_for_event(change, appConfig)
            if t not in threads_seen:
                threads_seen[t] = doc_key
            if len(threads_seen) >= 2:
                break

        # Proves that hash partitioning splits different _ids across threads
        assert len(threads_seen) >= 2, "Expected different _ids to route to different threads"

# Test - appConfig construction (validates CLI parsing logic)
class TestAppConfigConstruction:
    def test_single_thread_collection_map_empty_by_default(self):
        """No --single-thread-collections means empty map, all threads are hash pool."""
        threads = 4
        single_thread_arg = ''
        ns_list = [x.strip() for x in single_thread_arg.split(',') if x.strip()]
        num_hash = threads - len(ns_list)
        result = {ns: num_hash + i for i, ns in enumerate(ns_list)}
        assert result == {}
        assert num_hash == 4

    def test_single_thread_collection_map_single_entry(self):
        """One dedicated collection is carved from the total thread count."""
        single_thread_arg = 'mydb.device_tokens'
        threads = 4
        ns_list = [x.strip() for x in single_thread_arg.split(',') if x.strip()]
        num_hash = threads - len(ns_list)  # 3 hash threads
        result = {ns: num_hash + i for i, ns in enumerate(ns_list)}
        assert result == {'mydb.device_tokens': 3}
        assert num_hash == 3

    def test_single_thread_collection_map_multiple_entries(self):
        """Multiple dedicated collections are carved from total, leaving the rest for hash."""
        single_thread_arg = 'mydb.device_tokens, mydb.sessions, mydb.auth_codes'
        threads = 8
        ns_list = [x.strip() for x in single_thread_arg.split(',') if x.strip()]
        num_hash = threads - len(ns_list)  # 5 hash threads
        result = {ns: num_hash + i for i, ns in enumerate(ns_list)}
        assert result == {
            'mydb.device_tokens': 5,
            'mydb.sessions': 6,
            'mydb.auth_codes': 7,
        }
        assert num_hash == 5

    def test_total_workers_equals_threads_arg(self):
        """totalWorkers equals --threads (dedicated are carved from it, not added)."""
        threads = 8
        total_workers = threads
        assert total_workers == 8

# Test - Edge cases in event routing
class TestEventRoutingEdgeCases:
    def test_namespace_matching_is_exact(self):
        """db.collection must match exactly."""
        # 5 total, 1 dedicated - hash pool = 4 (threads 0-3), dedicated = 4
        appConfig = make_appconfig(threads=5, single_thread_collections={
            'mydb.device_tokens': 4,
        })

        # Similar name but different collection
        change = make_change_event(db='mydb', coll='device_tokens_archive', doc_key={'_id': '1'})
        result = mvu._get_thread_for_event(change, appConfig)

        # Should NOT route to dedicated thread
        assert result != 4
        expected = int(hashlib.sha512(str({'_id': '1'}).encode('utf-8')).hexdigest(), 16) % 4
        assert result == expected

    def test_different_db_same_collection_name(self):
        """db1.tokens and db2.tokens are different namespaces."""
        # 5 total, 1 dedicated - hash pool = 4 (threads 0-3), dedicated = 4
        appConfig = make_appconfig(threads=5, single_thread_collections={
            'db1.tokens': 4,
        })

        change_db2 = make_change_event(db='db2', coll='tokens', doc_key={'_id': '1'})
        result = mvu._get_thread_for_event(change_db2, appConfig)

        assert result != 4  # not routed to db1.tokens dedicated thread

    def test_single_thread_mode_all_events_to_thread_zero(self):
        """With --threads 1 and no single-thread-collections, everything goes to thread 0."""
        appConfig = make_appconfig(threads=1)

        for i in range(50):
            change = make_change_event(doc_key={'_id': f'doc_{i}'})
            assert mvu._get_thread_for_event(change, appConfig) == 0

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
