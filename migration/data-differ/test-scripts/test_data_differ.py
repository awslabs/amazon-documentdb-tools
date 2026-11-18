"""
Tests for data-differ.py

Unit tests run without a database. Integration tests need a source and a target cluster (they can be the same
cluster, the test collections use different names), set the SOURCE_URI and TARGET_URI environment variables:

    export SOURCE_URI="mongodb://user:password@source-host:27017/?tls=true&tlsCAFile=global-bundle.pem"
    export TARGET_URI="mongodb://user:password@target-host:27017/?tls=true&tlsCAFile=global-bundle.pem"
    python3 -m unittest -v test_data_differ.py

Integration tests create and drop collections in the database named by DATADIFF_TEST_DB (default datadiff_test).
"""

import datetime
import importlib.util
import os
import random
import re
import subprocess
import sys
import tempfile
import unittest
import uuid
import warnings

import bson
import bson.json_util
from bson import Binary, Decimal128, Int64, ObjectId
from bson.datetime_ms import DatetimeMS
from bson.raw_bson import RawBSONDocument
from bson.son import SON
from pymongo import MongoClient

HERE = os.path.dirname(os.path.abspath(__file__))
DIFFER = os.path.join(HERE, '..', 'data-differ.py')

spec = importlib.util.spec_from_file_location('data_differ', DIFFER)
dd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dd)

SOURCE_URI = os.environ.get('SOURCE_URI')
TARGET_URI = os.environ.get('TARGET_URI')
TEST_DB = os.environ.get('DATADIFF_TEST_DB', 'datadiff_test')

warnings.filterwarnings("ignore", "You appear to be connected to a DocumentDB cluster.")


# ---------------------------------------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------------------------------------

class TestComputeChunks(unittest.TestCase):

    def check_cover(self, lo, hi, n):
        chunks = dd.compute_chunks(lo, hi, n)
        self.assertGreaterEqual(len(chunks), 1)
        self.assertLessEqual(len(chunks), n)
        self.assertEqual(chunks[0][0], lo)
        self.assertTrue(chunks[-1][2], "last chunk must include the maximum")
        self.assertEqual(chunks[-1][1], hi)
        for i, (start, end, inclusive) in enumerate(chunks[:-1]):
            self.assertFalse(inclusive)
            self.assertLess(start, end, "chunks must not be empty")
            self.assertEqual(end, chunks[i + 1][0], "chunks must be contiguous")
        self.assertLessEqual(chunks[-1][0], chunks[-1][1])
        return chunks

    def test_every_value_in_exactly_one_chunk(self):
        rnd = random.Random(42)
        for _ in range(300):
            lo = rnd.randint(-1000, 1000)
            hi = lo + rnd.randint(0, 300)
            n = rnd.randint(1, 50)
            chunks = self.check_cover(lo, hi, n)
            for value in range(lo - 2, hi + 3):
                owners = [c for c in chunks if c[0] <= value and (value <= c[1] if c[2] else value < c[1])]
                self.assertEqual(len(owners), 1 if lo <= value <= hi else 0, (lo, hi, n, value))

    def test_fractional_values_covered(self):
        # doubles between integer chunk boundaries belong to exactly one chunk
        chunks = self.check_cover(0, 100, 7)
        for value in [x / 4 for x in range(0, 401)]:
            owners = [c for c in chunks if c[0] <= value and (value <= c[1] if c[2] else value < c[1])]
            self.assertEqual(len(owners), 1, value)

    def test_more_chunks_than_values(self):
        self.assertEqual(dd.compute_chunks(5, 5, 10), [(5, 5, True)])
        self.assertEqual(len(dd.compute_chunks(1, 3, 100)), 3)

    def test_huge_ranges(self):
        self.check_cover(0, 2 ** 96 - 1, 128)                 # full ObjectId space
        self.check_cover(-2 ** 63, 2 ** 63 - 1, 64)           # full int64 space
        chunks = self.check_cover(0, 2 ** 96 - 1, 4)
        sizes = [end - start for start, end, _ in chunks]
        self.assertLessEqual(max(sizes) - min(sizes), 2)

    def test_invalid(self):
        with self.assertRaises(ValueError):
            dd.compute_chunks(10, 9, 4)


class TestIdHelpers(unittest.TestCase):

    def test_ordinal_round_trip(self):
        for oid in (ObjectId(), ObjectId(b'\x00' * 12), ObjectId(b'\xff' * 12)):
            self.assertEqual(dd.ordinal_to_id(dd.id_to_ordinal(oid, dd.KIND_OBJECTID), dd.KIND_OBJECTID), oid)
        for ms in (0, -1, 1700000000123, -62135596800000 - 5000, 253402300800000 + 5000):
            value = DatetimeMS(ms)
            self.assertEqual(int(dd.ordinal_to_id(dd.id_to_ordinal(value, dd.KIND_DATE), dd.KIND_DATE)), ms)
        dt = datetime.datetime(2024, 5, 6, 7, 8, 9, 123000)
        self.assertEqual(dd.id_to_ordinal(dt, dd.KIND_DATE), int(DatetimeMS(dt)))
        for value in (0, -5, 2 ** 31 - 1, 2 ** 31, -2 ** 31 - 1, 2 ** 63 - 1, -2 ** 63):
            out = dd.ordinal_to_id(value, dd.KIND_INTEGER)
            self.assertEqual(out, value)
            self.assertEqual(isinstance(out, Int64), not -2 ** 31 <= value < 2 ** 31)

    def test_partition_kind(self):
        self.assertEqual(dd.partition_kind(ObjectId()), dd.KIND_OBJECTID)
        self.assertEqual(dd.partition_kind(DatetimeMS(5)), dd.KIND_DATE)
        self.assertEqual(dd.partition_kind(datetime.datetime.now()), dd.KIND_DATE)
        self.assertEqual(dd.partition_kind(5), dd.KIND_INTEGER)
        self.assertEqual(dd.partition_kind(Int64(5)), dd.KIND_INTEGER)
        for value in (True, False, 1.5, 'a', None, {'a': 1}, Decimal128('1'), Binary(b'x')):
            self.assertIsNone(dd.partition_kind(value), value)

    def test_id_key_equality_follows_database_semantics(self):
        same = [
            (1, Int64(1)), (1, 1.0), (Int64(1), 1.0), (1, Decimal128('1')), (Decimal128('1.0'), 1),
            (0.0, -0.0), (float('nan'), float('nan')), (float('nan'), Decimal128('NaN')),
            (DatetimeMS(0), datetime.datetime(1970, 1, 1)),
            ({'a': 1, 'b': 2}, {'a': Int64(1), 'b': 2.0}),
            (RawBSONDocument(bson.encode({'a': 1})), {'a': 1}),
            ('x', 'x'), (None, None), ([1, 2], [1, 2]),
        ]
        different = [
            (True, 1), (False, 0), ('1', 1), (1, 2), (2 ** 53 + 1, float(2 ** 53)),
            ({'a': 1, 'b': 2}, {'b': 2, 'a': 1}), ([1, 2], [2, 1]),
            (Binary(b'x', 0), Binary(b'x', 2)), (ObjectId(b'\x00' * 12), ObjectId(b'\x01' + b'\x00' * 11)),
            (None, 0), (DatetimeMS(0), 0), ({'a': 1}, [1]),
        ]
        for a, b in same:
            self.assertEqual(dd.id_key(a), dd.id_key(b), (a, b))
            self.assertEqual(hash(dd.id_key(a)), hash(dd.id_key(b)), (a, b))
        for a, b in different:
            self.assertNotEqual(dd.id_key(a), dd.id_key(b), (a, b))

    def test_raw_doc_id_matches_decode(self):
        values = [ObjectId(), 5, -5, Int64(2 ** 40), 1.5, -0.0, 'abc', 'üñí', '', DatetimeMS(-5),
                  DatetimeMS(253402300800000 + 1), {'a': 1, 'b': 'x'}, Decimal128('3.25'), Binary(uuid.uuid4().bytes, 4),
                  Binary(b'abc', 0), Binary(b'abc', 128),
                  None, True, uuid.uuid4().hex, [1]]
        for value in values:
            raw = bson.encode(SON([('_id', value), ('x', 1)]))
            expected = bson.decode(raw, codec_options=dd.ID_CODEC_OPTIONS)['_id']
            got = dd.raw_doc_id(raw)
            self.assertEqual(type(got), type(expected), value)
            self.assertEqual(dd.id_key(got), dd.id_key(expected), value)
        # _id that isn't the first field
        raw = bson.encode(SON([('x', 1), ('_id', 7)]))
        self.assertEqual(dd.raw_doc_id(raw), 7)

    def test_format_id(self):
        self.assertEqual(dd.format_id(5), '5')
        self.assertEqual(dd.format_id('a'), '"a"')
        oid = ObjectId()
        self.assertIn(str(oid), dd.format_id(oid))

    def test_describe_uri_hides_credentials(self):
        text = dd.describe_uri('mongodb://user:secretpw@host1:27017,host2:27018/?replicaSet=rs0')
        self.assertNotIn('secretpw', text)
        self.assertIn('host1:27017', text)
        self.assertIn('host2:27018', text)


class TestDocumentComparison(unittest.TestCase):

    def diff(self, doc1, doc2):
        options = dd.CodecOptions()
        return dd.diff_raw_documents(bson.encode(doc1), bson.encode(doc2), options, options)

    def test_identical(self):
        self.assertIsNone(self.diff({'_id': 1, 'a': [1, {'b': 2}]}, {'_id': 1, 'a': [1, {'b': 2}]}))

    def test_value_change(self):
        self.assertIn('values_changed', self.diff({'_id': 1, 'a': 1}, {'_id': 1, 'a': 2}))

    def test_type_change(self):
        self.assertIn('type_changes', self.diff({'_id': 1, 'a': 1}, {'_id': 1, 'a': Int64(1)}))
        self.assertIn('type_changes', self.diff({'_id': 1, 'a': 1}, {'_id': 1, 'a': 1.0}))

    def test_field_order(self):
        diff = self.diff(SON([('_id', 1), ('a', 1), ('b', 2)]), SON([('_id', 1), ('b', 2), ('a', 1)]))
        self.assertEqual(diff, {'field_order_changed': {'root': {'old_order': ['_id', 'a', 'b'],
                                                                 'new_order': ['_id', 'b', 'a']}}})

    def test_nested_field_order(self):
        diff = self.diff({'_id': 1, 'n': [SON([('x', 1), ('y', 2)])]}, {'_id': 1, 'n': [SON([('y', 2), ('x', 1)])]})
        self.assertIn("root['n'][0]", diff['field_order_changed'])

    def test_array_order(self):
        self.assertIn('values_changed', self.diff({'_id': 1, 'a': [1, 2]}, {'_id': 1, 'a': [2, 1]}))

    def test_added_removed_fields(self):
        self.assertIn('dictionary_item_added', self.diff({'_id': 1}, {'_id': 1, 'a': 1}))
        self.assertIn('dictionary_item_removed', self.diff({'_id': 1, 'a': 1}, {'_id': 1}))

    def test_strict_bson_differences_deepdiff_misses(self):
        cases = [
            ({'_id': 1, 'a': 0.0}, {'_id': 1, 'a': -0.0}),
            ({'_id': 1, 'a': Binary(b'x', 5)}, {'_id': 1, 'a': Binary(b'x', 128)}),
            ({'_id': 1, 'a': Decimal128('1.0')}, {'_id': 1, 'a': Decimal128('1.00')}),
            ({'_id': 1, 'n': {'deep': [0.0]}}, {'_id': 1, 'n': {'deep': [-0.0]}}),
        ]
        for doc1, doc2 in cases:
            diff = self.diff(doc1, doc2)
            self.assertIsNotNone(diff, doc2)
            self.assertIn('bson_value_or_type_changed', diff, doc2)


# ---------------------------------------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------------------------------------

def parse_output(text):
    """Parse the last run in an output file into a summary dict and sets of reported _ids."""
    text = text[text.rfind('===== data-differ run started'):]
    summary = {}
    for line in text.splitlines():
        m = re.match(r'^(Identical documents|Different documents|Missing in target|Extra in target|'
                     r'Source documents compared|Target documents compared|Deleted from source):\s+([\d,]+)', line)
        if m:
            summary[m.group(1)] = int(m.group(2).replace(',', ''))
        m = re.match(r'^Result:\s+(.*)$', line)
        if m:
            summary['Result'] = m.group(1)
        m = re.match(r'^Mode: (\S+)', line)
        if m and 'Mode' not in summary:
            summary['Mode'] = m.group(1)
        m = re.match(r'^Chunks:\s+([\d,]+) of ([\d,]+) complete, ([\d,]+) with a different', line)
        if m:
            summary['Chunk count mismatches'] = int(m.group(3).replace(',', ''))
    reported = {
        'missing': set(re.findall(r'^Document missing in target, _id: (.*)$', text, re.M)),
        'extra': set(re.findall(r'^Document missing in source \(extra in target\), _id: (.*)$', text, re.M)),
        'different': set(re.findall(r'^Difference found at doc id: (.*)$', text, re.M)),
    }
    return summary, reported, text


@unittest.skipUnless(SOURCE_URI and TARGET_URI, 'set SOURCE_URI and TARGET_URI to run the integration tests')
class IntegrationTestBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            cls.src_client = MongoClient(SOURCE_URI)
            cls.tgt_client = MongoClient(TARGET_URI)

    @classmethod
    def tearDownClass(cls):
        cls.src_client.close()
        cls.tgt_client.close()

    def setUp(self):
        self.coll_name = f"t_{self._testMethodName}"[:60]
        self.src = self.src_client[TEST_DB][self.coll_name + '_src']
        self.tgt = self.tgt_client[TEST_DB][self.coll_name + '_tgt']
        self.src.drop()
        self.tgt.drop()
        self.tmp = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.src.drop()
        self.tgt.drop()
        self.tmp.cleanup()

    def load(self, src_docs, tgt_docs):
        if src_docs:
            self.src.insert_many(src_docs, ordered=False)
        if tgt_docs:
            self.tgt.insert_many(tgt_docs, ordered=False)

    def run_differ(self, *extra, workers=4, chunks=None):
        output_file = os.path.join(self.tmp.name, f"out_{len(os.listdir(self.tmp.name))}.txt")
        cmd = [sys.executable, DIFFER, '--source-uri', SOURCE_URI, '--target-uri', TARGET_URI,
               '--source-db', TEST_DB, '--source-coll', self.src.name,
               '--target-db', TEST_DB, '--target-coll', self.tgt.name,
               '--output-file', output_file, '--plain-progress', '--workers', str(workers)]
        if chunks is not None:
            cmd += ['--chunks', str(chunks)]
        cmd += list(extra)
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        with open(output_file) as f:
            summary, reported, text = parse_output(f.read())
        return proc.returncode, summary, reported, text, proc.stdout + proc.stderr

    def assertResult(self, result, mode=None, identical=None, missing=(), extra=(), different=(), exit_code=None):
        returncode, summary, reported, text, console = result
        context = f"\n--- console ---\n{console}\n--- output ---\n{text}"
        if mode is not None:
            self.assertEqual(summary.get('Mode'), mode, context)
        expected_missing = {dd.format_id(v) for v in missing}
        expected_extra = {dd.format_id(v) for v in extra}
        expected_different = {dd.format_id(v) for v in different}
        self.assertEqual(reported['missing'], expected_missing, context)
        self.assertEqual(reported['extra'], expected_extra, context)
        self.assertEqual(reported['different'], expected_different, context)
        self.assertEqual(summary['Missing in target'], len(expected_missing), context)
        self.assertEqual(summary['Extra in target'], len(expected_extra), context)
        self.assertEqual(summary['Different documents'], len(expected_different), context)
        if identical is not None:
            self.assertEqual(summary['Identical documents'], identical, context)
        if exit_code is None:
            exit_code = 1 if (missing or extra or different) else 0
        self.assertEqual(returncode, exit_code, context)
        self.assertNotIn('ERROR', text, context)


class TestRangePartitioned(IntegrationTestBase):

    def test_everything_same_objectid(self):
        docs = [{'_id': ObjectId(), 'i': i, 'payload': 'x' * (i % 50)} for i in range(5000)]
        self.load(docs, docs)
        self.assertResult(self.run_differ(chunks=37), mode='range', identical=5000)

    def test_everything_same_workers_1_chunks_1(self):
        docs = [{'_id': i, 'v': i} for i in range(2500)]
        self.load(docs, docs)
        self.assertResult(self.run_differ(workers=1, chunks=1), mode='range', identical=2500)

    def test_int_missing_extra_different_same_min_max(self):
        # target has the same min and max _id, the same document count, every source document but one exists
        # in the target with identical content... and the target has an extra document inside the range
        src = [{'_id': i, 'v': i} for i in range(0, 3000, 2)]
        tgt = [dict(d) for d in src]
        tgt = [d for d in tgt if d['_id'] != 1000]       # missing in target
        tgt.append({'_id': 1001, 'v': 'extra'})           # extra in target, between min and max
        for d in tgt:
            if d['_id'] in (0, 1500, 2998):
                d['v'] = 'changed'
        self.load(src, tgt)
        self.assertEqual(self.src.count_documents({}), self.tgt.count_documents({}))
        result = self.run_differ('--batch-size', '100', chunks=8)
        self.assertResult(result, mode='range', identical=1500 - 4, missing=[1000], extra=[1001],
                          different=[0, 1500, 2998])
        self.assertGreaterEqual(result[1]['Chunk count mismatches'], 0)

    def test_extras_and_missing_outside_other_side_range(self):
        src = [{'_id': i, 'v': i} for i in range(100, 200)]
        tgt = [{'_id': i, 'v': i} for i in range(105, 200)] + [{'_id': i} for i in (1, 2, 500, 10 ** 12)]
        self.load(src, tgt)
        result = self.run_differ('--batch-size', '7', chunks=13)
        self.assertResult(result, mode='range', identical=95, missing=range(100, 105),
                          extra=[1, 2, 500, 10 ** 12])
        self.assertIn('Minimum _id differs', result[3])
        self.assertIn('Maximum _id differs', result[3])

    def test_batch_boundaries(self):
        # differences on every batch boundary, batch size 10
        src = [{'_id': i, 'v': i} for i in range(1000)]
        tgt = [{'_id': i, 'v': i} for i in range(1000) if i % 10 != 9]
        tgt += [{'_id': i + 0.5, 'v': 'x'} for i in range(9, 1000, 50)]   # doubles between integers
        self.load(src, tgt)
        result = self.run_differ('--batch-size', '10', chunks=3)
        self.assertResult(result, mode='range', identical=900, missing=range(9, 1000, 10),
                          extra=[i + 0.5 for i in range(9, 1000, 50)])

    def test_large_window_of_extra_documents(self):
        # the source has only the minimum and maximum _id, every other target document is in a single window
        src = [{'_id': 0}, {'_id': 6000}]
        tgt = [{'_id': i} for i in range(6001)]
        self.load(src, tgt)
        self.assertResult(self.run_differ('--batch-size', '10', workers=1, chunks=1), mode='range', identical=2,
                          extra=range(1, 6000))

    def test_mixed_numeric_types(self):
        src = [{'_id': i, 'v': i} for i in range(50)] + [{'_id': Int64(2 ** 40 + i), 'v': i} for i in range(50)] \
            + [{'_id': i + 0.25, 'v': i} for i in range(50)] + [{'_id': Decimal128(f"{i}.75"), 'v': i} for i in range(50)]
        tgt = [dict(d) for d in src]
        tgt[120]['v'] = 'changed'                                       # a double _id
        del tgt[170]                                                    # a decimal _id
        self.load(src, tgt)
        # small batches so double and decimal _id values become window boundaries
        self.assertResult(self.run_differ('--batch-size', '7', chunks=16), mode='range', identical=198,
                          different=[120 - 100 + 0.25], missing=[Decimal128('20.75')])

    def test_id_type_changed_int_to_long(self):
        src = [{'_id': i, 'v': i} for i in range(100)]
        tgt = [{'_id': Int64(i) if i in (10, 20) else i, 'v': i} for i in range(100)]
        self.load(src, tgt)
        returncode, summary, reported, text, console = self.run_differ(chunks=4)
        self.assertResult((returncode, summary, reported, text, console), mode='range', identical=98,
                          different=[10, 20])
        self.assertIn('type_changes', text)

    def test_int64_extremes(self):
        ids = [-2 ** 63, -2 ** 31 - 1, -1, 0, 1, 2 ** 31, 2 ** 63 - 1]
        src = [{'_id': Int64(i) if not -2 ** 31 <= i < 2 ** 31 else i} for i in ids]
        tgt = [d for d in src if d['_id'] != 0]
        self.load(src, tgt)
        self.assertResult(self.run_differ(chunks=64), mode='range', identical=6, missing=[0])

    def test_objectid_extremes(self):
        ids = [ObjectId(b'\x00' * 12), ObjectId(b'\x7f' + b'\x00' * 11), ObjectId(), ObjectId(b'\xff' * 12)]
        src = [{'_id': i, 'v': 1} for i in ids]
        tgt = [{'_id': i, 'v': 1} for i in ids[1:]] + [{'_id': ObjectId(b'\x80' + b'\x00' * 11)}]
        self.load(src, tgt)
        self.assertResult(self.run_differ(chunks=128), mode='range', identical=3, missing=[ids[0]],
                          extra=[ObjectId(b'\x80' + b'\x00' * 11)])

    def test_skewed_objectids(self):
        # ObjectIds clustered at both ends of a wide range, most chunks are empty
        base_old = int(datetime.datetime(2015, 1, 1).timestamp())
        base_new = int(datetime.datetime(2025, 1, 1).timestamp())
        ids = [ObjectId.from_datetime(datetime.datetime.fromtimestamp(base_old + i, datetime.timezone.utc)) for i in range(500)]
        ids += [ObjectId() for _ in range(1500)]
        ids += [ObjectId.from_datetime(datetime.datetime.fromtimestamp(base_new + i, datetime.timezone.utc)) for i in range(5)]
        ids = list({i: None for i in ids})
        src = [{'_id': i, 'n': n} for n, i in enumerate(ids)]
        tgt = [{'_id': i, 'n': n if n != 1700 else -1} for n, i in enumerate(ids)]
        self.load(src, tgt)
        self.assertResult(self.run_differ(chunks=200), mode='range', identical=len(ids) - 1, different=[ids[1700]])

    def test_dates(self):
        start = datetime.datetime(1950, 1, 1)
        ids = [start + datetime.timedelta(days=i * 37, milliseconds=i) for i in range(1000)]
        src = [{'_id': d, 'v': i} for i, d in enumerate(ids)]
        tgt = [{'_id': d, 'v': i} for i, d in enumerate(ids) if i != 500]
        tgt.append({'_id': ids[10] + datetime.timedelta(milliseconds=1), 'v': 'extra'})
        tgt[3]['v'] = 'changed'
        self.load(src, tgt)
        self.assertResult(self.run_differ('--batch-size', '50', chunks=20), mode='range', identical=998,
                          missing=[ids[500]], extra=[ids[10] + datetime.timedelta(milliseconds=1)],
                          different=[ids[3]])

    def test_dates_outside_python_range(self):
        ids = [DatetimeMS(-62135596800000 - 86400000), DatetimeMS(0), DatetimeMS(253402300800000 + 86400000)]
        src = [{'_id': i, 'd': i} for i in ids]
        tgt = [{'_id': i, 'd': i if int(i) else DatetimeMS(1)} for i in ids]
        self.load(src, tgt)
        self.assertResult(self.run_differ(chunks=10), mode='range', identical=2, different=[ids[1]])

    def test_strict_bson_differences(self):
        src = [{'_id': 1, 'a': 0.0}, {'_id': 2, 'a': Binary(b'x', 5)}, {'_id': 3, 'a': Decimal128('1.0')},
               {'_id': 4, 'a': 1}]
        tgt = [{'_id': 1, 'a': -0.0}, {'_id': 2, 'a': Binary(b'x', 128)}, {'_id': 3, 'a': Decimal128('1.00')},
               {'_id': 4, 'a': 1}]
        self.load(src, tgt)
        result = self.run_differ(chunks=2)
        self.assertResult(result, mode='range', identical=1, different=[1, 2, 3])
        self.assertIn('bson_value_or_type_changed', result[3])

    def test_field_and_array_order(self):
        src = [SON([('_id', 1), ('a', 1), ('b', 2)]), {'_id': 2, 'arr': [1, 2, 3]},
               {'_id': 3, 'n': SON([('x', 1), ('y', 2)])}, {'_id': 4, 'z': 1}]
        tgt = [SON([('_id', 1), ('b', 2), ('a', 1)]), {'_id': 2, 'arr': [3, 2, 1]},
               {'_id': 3, 'n': SON([('y', 2), ('x', 1)])}, {'_id': 4, 'z': 1}]
        self.load(src, tgt)
        result = self.run_differ(chunks=2)
        self.assertResult(result, mode='range', identical=1, different=[1, 2, 3])
        self.assertIn('field_order_changed', result[3])

    def test_chunk_count_mismatch_reported(self):
        src = [{'_id': i} for i in range(100)]
        tgt = [{'_id': i} for i in range(100) if i not in (5, 6, 7)]
        self.load(src, tgt)
        result = self.run_differ(chunks=10)
        self.assertResult(result, mode='range', identical=97, missing=[5, 6, 7])
        self.assertEqual(result[1]['Chunk count mismatches'], 1)
        self.assertIn('document count differs: source 10, target 7', result[3])

    def test_range_matches_batched(self):
        """Randomized: range partitioned and batched mode must report exactly the expected differences."""
        rnd = random.Random(1234)
        ids = rnd.sample(range(-50000, 50000), 4000)
        src = [{'_id': i, 'v': rnd.random(), 'arr': [rnd.randint(0, 9) for _ in range(3)]} for i in ids]
        tgt = []
        missing, different = [], []
        for doc in src:
            r = rnd.random()
            if r < 0.02:
                missing.append(doc['_id'])
                continue
            doc = dict(doc)
            if r < 0.05:
                doc['v'] = 'changed'
                different.append(doc['_id'])
            tgt.append(doc)
        existing = set(ids)
        extra = [i for i in rnd.sample(range(-60000, 60000), 200) if i not in existing][:80]
        tgt += [{'_id': i, 'v': 'extra'} for i in extra]
        rnd.shuffle(tgt)
        self.load(src, tgt)
        identical = len(src) - len(missing) - len(different)
        for args, mode in (((), 'range'), (('--check-target', '--no-partition'), 'batched')):
            with self.subTest(mode=mode):
                self.assertResult(self.run_differ('--batch-size', '97', *args, chunks=23), mode=mode,
                                  identical=identical, missing=missing, extra=extra, different=different)

    def test_index_difference(self):
        docs = [{'_id': i, 'a': i} for i in range(10)]
        self.load(docs, docs)
        self.src.create_index([('a', 1)])
        returncode, summary, reported, text, console = self.run_differ()
        self.assertEqual(returncode, 1, text)
        self.assertIn('Index differences', text)
        self.assertEqual(summary['Result'], 'DIFFERENCES FOUND')


class TestBatched(IntegrationTestBase):

    def test_string_ids(self):
        src = [{'_id': f"key{i:05d}", 'v': i} for i in range(3000)]
        tgt = [dict(d) for d in src if d['_id'] != 'key00100']
        tgt[5]['v'] = 'changed'
        self.load(src, tgt)
        self.assertResult(self.run_differ('--batch-size', '250'), mode='batched', identical=2998,
                          missing=['key00100'], different=['key00005'])

    def test_string_ids_extra_requires_check_target(self):
        src = [{'_id': f"k{i}", 'v': i} for i in range(500)]
        tgt = src + [{'_id': 'zzz-extra'}, {'_id': 'aaa-extra'}]
        self.load(src, tgt)
        returncode, summary, reported, text, console = self.run_differ()
        self.assertEqual(returncode, 1, text)
        self.assertIn("2 document(s) that weren't matched", text)
        self.assertResult(self.run_differ('--check-target'), mode='batched', identical=500,
                          extra=['zzz-extra', 'aaa-extra'])

    def test_reverse_check_skipped_when_counts_match(self):
        src = [{'_id': f"k{i}", 'v': i} for i in range(500)]
        self.load(src, src)
        result = self.run_differ('--check-target')
        self.assertResult(result, mode='batched', identical=500)
        result = self.run_differ()
        self.assertResult(result, mode='batched', identical=500)
        self.assertIn('reverse check not needed', result[4])

    def test_compound_ids(self):
        src = [{'_id': {'a': i, 'b': f"x{i}"}, 'v': i} for i in range(300)]
        tgt = [{'_id': {'a': i, 'b': f"x{i}"}, 'v': i if i != 7 else 'changed'} for i in range(300) if i != 8]
        tgt.append({'_id': SON([('b', 'x9'), ('a', 9)]), 'v': 9})   # field order of _id matters
        self.load(src, tgt)
        self.assertResult(self.run_differ('--check-target'), mode='batched', identical=298,
                          missing=[{'a': 8, 'b': 'x8'}], different=[{'a': 7, 'b': 'x7'}],
                          extra=[SON([('b', 'x9'), ('a', 9)])])

    def test_mixed_id_types(self):
        src = [{'_id': i} for i in range(100)] + [{'_id': str(i)} for i in range(100)] + [{'_id': ObjectId()}]
        tgt = [d for d in src if d['_id'] != '50']
        self.load(src, tgt)
        self.assertResult(self.run_differ(), mode='batched', identical=200, missing=['50'])

    def test_no_partition_int(self):
        src = [{'_id': i, 'v': i} for i in range(1000)]
        tgt = [{'_id': i, 'v': i if i != 999 else 0} for i in range(1000) if i != 0]
        self.load(src, tgt)
        self.assertResult(self.run_differ('--no-partition'), mode='batched', identical=998, missing=[0],
                          different=[999])

    def test_sample(self):
        src = [{'_id': f"s{i}", 'v': i} for i in range(2000)]
        self.load(src, src)
        returncode, summary, reported, text, console = self.run_differ('--sample-size-percent', '10',
                                                                         '--sampling-timeout-ms', '10000')
        self.assertEqual(returncode, 0, text)
        self.assertEqual(summary['Mode'], 'sampled')
        self.assertEqual(summary['Identical documents'], summary['Source documents compared'])
        self.assertGreater(summary['Identical documents'], 100)
        self.assertLessEqual(summary['Identical documents'], 200)


class TestReverseCheckReasoning(IntegrationTestBase):
    """Same minimum and maximum _id, every source document exists and is identical in the target, but the target
    has an extra document between the minimum and maximum. Matching min/max _id values and a clean forward
    comparison are not enough to prove the target has no extra documents, the document counts must also match."""

    def setUp(self):
        super().setUp()
        src = [{'_id': i, 'v': i} for i in range(0, 1000, 2)]
        self.load(src, src + [{'_id': 501, 'v': 'extra'}])

    def test_range_mode_finds_extra(self):
        self.assertResult(self.run_differ(chunks=8), mode='range', identical=500, extra=[501])

    def test_batched_mode_reports_unmatched_count(self):
        returncode, summary, reported, text, console = self.run_differ('--no-partition')
        self.assertEqual(returncode, 1, text)
        self.assertIn("1 document(s) that weren't matched", text)
        self.assertResult(self.run_differ('--no-partition', '--check-target'), mode='batched', identical=500,
                          extra=[501])


class TestLegacyFixtures(IntegrationTestBase):
    """The JSON fixtures used by the bash test scripts (which need mongoimport and mongosh)."""

    def load_fixture(self, src_file, tgt_file):
        def read(name):
            with open(os.path.join(HERE, name)) as f:
                return [bson.json_util.loads(line) for line in f if line.strip()]
        self.load(read(src_file), read(tgt_file))

    def test_everything_same(self):
        self.load_fixture('everything_same.json', 'everything_same.json')
        self.assertResult(self.run_differ(), mode='range', identical=3)

    def test_dict_id(self):
        self.load_fixture('dict_id_source.json', 'dict_id_target.json')
        self.assertResult(self.run_differ(), mode='batched', identical=3)

    def test_dict_id_diff(self):
        self.load_fixture('dict_id_diff_source.json', 'dict_id_diff_target.json')
        self.assertResult(self.run_differ(), mode='batched', identical=2,
                          different=[SON([('key1', 'value3'), ('key2', 'value4')])])

    def test_extra_target_field(self):
        self.load_fixture('extra_target_field_source.json', 'extra_target_field_target.json')
        result = self.run_differ()
        self.assertResult(result, mode='range', identical=2, different=[2])
        self.assertIn('dictionary_item_added', result[3])

    def test_nested_dict_diff(self):
        self.load_fixture('nested_dict_diff_source.json', 'nested_dict_diff_target.json')
        self.assertResult(self.run_differ(), mode='range', identical=2, different=[3])

    def test_order_change(self):
        self.load_fixture('order_change_source.json', 'order_change_target.json')
        result = self.run_differ()
        self.assertResult(result, mode='range', identical=2, different=[1])
        self.assertIn('field_order_changed', result[3])

    def test_wrong_id(self):
        self.load_fixture('wrong_id_source.json', 'wrong_id_target.json')
        self.assertResult(self.run_differ(), mode='range', identical=2, missing=[3], extra=[10])


class TestEdgeCases(IntegrationTestBase):

    def test_empty_target(self):
        self.load([{'_id': 1}], [])
        returncode, summary, reported, text, console = self.run_differ()
        self.assertEqual(returncode, 1, text)
        self.assertIn('No documents found in the target collection', console)

    def test_empty_source(self):
        self.load([], [{'_id': 1}])
        returncode, summary, reported, text, console = self.run_differ()
        self.assertEqual(returncode, 1, text)
        self.assertIn('No documents found in the source collection', console)

    def test_both_empty(self):
        returncode, summary, reported, text, console = self.run_differ()
        self.assertEqual(returncode, 2, text)

    def test_min_type_differs(self):
        # source is all integers, target has a string _id: can't be range partitioned
        src = [{'_id': i} for i in range(10)]
        tgt = src + [{'_id': 'str'}]
        self.load(src, tgt)
        self.assertResult(self.run_differ('--check-target'), mode='batched', identical=10, extra=['str'])


if __name__ == '__main__':
    unittest.main()
