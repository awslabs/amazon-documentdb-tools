"""
Amazon DocumentDB DataDiffer - compare the documents and indexes of two collections.

Two comparison strategies are used:

  * Range partitioned (parallel): when every _id in both collections is an ObjectId, a date, or an integer
    (int/long), the _id keyspace between the minimum and maximum _id is divided mathematically into chunks.
    Each chunk is compared independently by a pool of worker processes. Within a chunk the source is read
    in _id order and, for every batch of source documents, the target is queried for exactly the same _id
    window, so documents missing from the target and extra documents in the target are both detected in a
    single pass (no reverse scan is needed).

  * Batched (fallback): used for any other _id type (string, compound, mixed types, ...) or when sampling.
    The main process streams source _id values, worker processes fetch the matching documents from both
    collections with $in and compare them.

Documents are read as raw BSON, byte-identical documents are equal. Documents whose bytes differ are decoded
and compared with DeepDiff, then checked for field order changes and strict BSON type/value differences
(-0.0 vs 0.0, binary subtype, decimal128 precision, ...) that DeepDiff does not report.
"""

import argparse
import concurrent.futures
import datetime
import math
import multiprocessing
import os
import queue
import signal
import struct
import sys
import threading
import time
import traceback
import uuid
import warnings
from itertools import islice

import bson
from bson import json_util
from bson.binary import Binary
from bson.code import Code
from bson.codec_options import CodecOptions, DatetimeConversion
from bson.datetime_ms import DatetimeMS
from bson.decimal128 import Decimal128
from bson.int64 import Int64
from bson.max_key import MaxKey
from bson.min_key import MinKey
from bson.objectid import ObjectId
from bson.raw_bson import RawBSONDocument
from bson.regex import Regex
from bson.timestamp import Timestamp
from deepdiff import DeepDiff
from pymongo import MongoClient
from pymongo.uri_parser import parse_uri

try:
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress_bar import ProgressBar
    from rich.table import Table
    from rich.text import Text
    RICH_AVAILABLE = True
except ImportError:  # the tool still works without rich, progress is printed as plain text
    RICH_AVAILABLE = False


EXIT_NO_DIFFERENCES = 0
EXIT_DIFFERENCES = 1
EXIT_ERROR = 2

KIND_OBJECTID = 'objectId'
KIND_DATE = 'date'
KIND_INTEGER = 'integer'

COUNTER_NAMES = ('src_docs', 'tgt_docs', 'src_bytes', 'tgt_bytes', 'identical', 'different',
                 'missing_in_target', 'extra_in_target', 'source_vanished')

# _id values are decoded with dates as DatetimeMS so dates outside the Python datetime range work
ID_CODEC_OPTIONS = CodecOptions(datetime_conversion=DatetimeConversion.DATETIME_MS)
RAW_CODEC_OPTIONS = CodecOptions(document_class=RawBSONDocument)

_INT32 = struct.Struct('<i')
_INT64 = struct.Struct('<q')
_DOUBLE = struct.Struct('<d')


def connect_to_db(uri, pool_size):
    warnings.filterwarnings("ignore", "You appear to be connected to a DocumentDB cluster.")
    return MongoClient(host=uri, maxPoolSize=pool_size, appname='datadiff')


def describe_uri(uri):
    """Host list of a connection string, never includes credentials."""
    try:
        return ','.join(f"{host}:{port}" for host, port in parse_uri(uri)['nodelist'])
    except Exception:
        return '<unparseable uri>'


# ---------------------------------------------------------------------------------------------------------
# _id helpers
# ---------------------------------------------------------------------------------------------------------

def raw_doc_id(raw):
    """Return the _id of a raw BSON document. Fast path when _id is the first field (always true for documents
    returned by MongoDB and DocumentDB) and is a common scalar type, otherwise decode the document."""
    if raw[5:9] == b'_id\x00':
        bson_type = raw[4]
        if bson_type == 0x07:
            return ObjectId(raw[9:21])
        if bson_type == 0x10:
            return _INT32.unpack_from(raw, 9)[0]
        if bson_type == 0x12:
            return Int64(_INT64.unpack_from(raw, 9)[0])
        if bson_type == 0x09:
            return DatetimeMS(_INT64.unpack_from(raw, 9)[0])
        if bson_type == 0x01:
            return _DOUBLE.unpack_from(raw, 9)[0]
        if bson_type == 0x02:
            length = _INT32.unpack_from(raw, 9)[0]
            return raw[13:13 + length - 1].decode('utf-8')
    return bson.decode(raw, codec_options=ID_CODEC_OPTIONS)['_id']


def id_key(value):
    """Hashable key for an _id value, two _id values that the database considers equal produce the same key.
    Numbers of different BSON types compare by value (1 == NumberLong(1) == 1.0), other types never compare
    equal to each other (true != 1, "1" != 1). Embedded documents are field order sensitive."""
    if isinstance(value, bool):
        return (8, value)
    if isinstance(value, (int, float)):
        if value != value:
            return (1, 'nan')
        return (1, value)
    if isinstance(value, ObjectId):
        return (7, value.binary)
    if isinstance(value, str):
        return (2, value)
    if isinstance(value, Decimal128):
        decimal_value = value.to_decimal()
        if decimal_value.is_nan():
            return (1, 'nan')
        return (1, decimal_value)
    if isinstance(value, DatetimeMS):
        return (9, int(value))
    if isinstance(value, datetime.datetime):
        return (9, int(DatetimeMS(value)))
    if isinstance(value, (dict, RawBSONDocument)):
        return (3, tuple((k, id_key(v)) for k, v in value.items()))
    if isinstance(value, list):
        return (4, tuple(id_key(v) for v in value))
    if value is None:
        return (10,)
    if isinstance(value, Binary):
        return (5, value.subtype, bytes(value))
    if isinstance(value, bytes):
        return (5, 0, value)
    if isinstance(value, uuid.UUID):
        return (5, 4, value.bytes)
    if isinstance(value, Timestamp):
        return (17, value.time, value.inc)
    if isinstance(value, Regex):
        return (11, value.pattern, value.flags)
    if isinstance(value, MinKey):
        return (-1,)
    if isinstance(value, MaxKey):
        return (127,)
    if isinstance(value, Code):
        return (13, str(value), id_key(value.scope) if value.scope is not None else None)
    return (99, type(value).__name__, repr(value))


def format_id(value):
    """Human readable, unambiguous _id (Relaxed Extended JSON)."""
    try:
        return json_util.dumps(value, json_options=json_util.RELAXED_JSON_OPTIONS)
    except Exception:
        return repr(value)


def short_id(value):
    """Compact _id for the dashboard."""
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, DatetimeMS):
        try:
            return value.as_datetime().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        except Exception:
            return f"{int(value)}ms"
    if isinstance(value, (int, float, str)):
        return str(value)
    return format_id(value)


def partition_kind(value):
    """The partitionable type class of an _id value, or None if it can't be range partitioned."""
    if isinstance(value, bool):
        return None
    if isinstance(value, ObjectId):
        return KIND_OBJECTID
    if isinstance(value, (DatetimeMS, datetime.datetime)):
        return KIND_DATE
    if isinstance(value, int):
        return KIND_INTEGER
    return None


def id_to_ordinal(value, kind):
    if kind == KIND_OBJECTID:
        return int.from_bytes(value.binary, 'big')
    if kind == KIND_DATE:
        return int(value) if isinstance(value, DatetimeMS) else int(DatetimeMS(value))
    return int(value)


def ordinal_to_id(ordinal, kind):
    if kind == KIND_OBJECTID:
        return ObjectId(ordinal.to_bytes(12, 'big'))
    if kind == KIND_DATE:
        return DatetimeMS(ordinal)
    if -2 ** 31 <= ordinal < 2 ** 31:
        return ordinal
    return Int64(ordinal)


def compute_chunks(min_ordinal, max_ordinal, num_chunks):
    """Divide the inclusive ordinal range [min_ordinal, max_ordinal] into at most num_chunks contiguous,
    non-overlapping chunks. Returns a list of (start, end, end_inclusive): every chunk covers start <= x < end
    except the last chunk which covers start <= x <= max_ordinal. Because the boundaries are contiguous the
    chunks also cover every non-integer number (e.g. a double _id) between the minimum and maximum."""
    if max_ordinal < min_ordinal:
        raise ValueError("max_ordinal is less than min_ordinal")
    span = max_ordinal - min_ordinal + 1
    num_chunks = max(1, min(num_chunks, span))
    boundaries = [min_ordinal + (span * i) // num_chunks for i in range(num_chunks)]
    chunks = []
    for i, start in enumerate(boundaries):
        if i + 1 < num_chunks:
            chunks.append((start, boundaries[i + 1], False))
        else:
            chunks.append((start, max_ordinal, True))
    return chunks


def chunk_filter(start, end, end_inclusive):
    return {'_id': {'$gte': start, ('$lte' if end_inclusive else '$lt'): end}}


# ---------------------------------------------------------------------------------------------------------
# Document comparison
# ---------------------------------------------------------------------------------------------------------

def find_field_order_differences(obj1, obj2, path="root", differences=None):
    """Find fields that exist in both documents but appear in a different order, DeepDiff compares dictionaries
    by key only so it never reports these. Recurses into nested documents and arrays."""
    if differences is None:
        differences = {}
    if isinstance(obj1, dict) and isinstance(obj2, dict):
        # Only compare the relative order of shared fields, added/removed fields are reported by DeepDiff
        common_keys1 = [key for key in obj1 if key in obj2]
        common_keys2 = [key for key in obj2 if key in obj1]
        if common_keys1 != common_keys2:
            differences[path] = {'old_order': list(obj1), 'new_order': list(obj2)}
        for key in common_keys1:
            find_field_order_differences(obj1[key], obj2[key], f"{path}[{key!r}]", differences)
    elif isinstance(obj1, list) and isinstance(obj2, list):
        for index, (item1, item2) in enumerate(zip(obj1, obj2)):
            find_field_order_differences(item1, item2, f"{path}[{index}]", differences)
    return differences


def find_strict_bson_differences(obj1, obj2, path="root", differences=None):
    """Find values that are equal according to Python/DeepDiff but are stored differently in BSON, for example
    0.0 vs -0.0, Binary subtype 0 vs 2, or Decimal128('1.0') vs Decimal128('1.00')."""
    if differences is None:
        differences = {}
    if type(obj1) is not type(obj2):
        differences[path] = {'old_value': repr(obj1), 'new_value': repr(obj2)}
    elif isinstance(obj1, dict):
        for key in obj1:
            if key in obj2:
                find_strict_bson_differences(obj1[key], obj2[key], f"{path}[{key!r}]", differences)
    elif isinstance(obj1, list):
        for index, (item1, item2) in enumerate(zip(obj1, obj2)):
            find_strict_bson_differences(item1, item2, f"{path}[{index}]", differences)
    elif isinstance(obj1, float):
        if _DOUBLE.pack(obj1) != _DOUBLE.pack(obj2):
            differences[path] = {'old_value': repr(obj1), 'new_value': repr(obj2)}
    elif isinstance(obj1, Binary):
        if obj1.subtype != obj2.subtype or bytes(obj1) != bytes(obj2):
            differences[path] = {'old_value': repr(obj1), 'new_value': repr(obj2)}
    elif isinstance(obj1, Decimal128):
        if obj1.bid != obj2.bid:
            differences[path] = {'old_value': repr(obj1), 'new_value': repr(obj2)}
    elif obj1 != obj2:
        differences[path] = {'old_value': repr(obj1), 'new_value': repr(obj2)}
    return differences


def diff_documents(doc1, doc2):
    """Compare two decoded documents, array order is significant. Returns an empty dict if they're the same."""
    diff = dict(DeepDiff(doc1, doc2, verbose_level=2, cache_size=5000))
    field_order_differences = find_field_order_differences(doc1, doc2)
    if field_order_differences:
        diff['field_order_changed'] = field_order_differences
    return diff


def diff_raw_documents(raw1, raw2, src_codec_options, tgt_codec_options):
    """Compare two raw BSON documents (bytes), returns None if they're identical, otherwise a description of the
    differences."""
    if raw1 == raw2:
        return None
    doc1 = bson.decode(raw1, codec_options=src_codec_options)
    doc2 = bson.decode(raw2, codec_options=tgt_codec_options)
    diff = diff_documents(doc1, doc2)
    if not diff:
        # the bytes differ but the decoded values compare as equal
        strict = find_strict_bson_differences(doc1, doc2)
        diff = {'bson_value_or_type_changed': strict} if strict else {'bson_bytes_differ': True}
    return diff


# ---------------------------------------------------------------------------------------------------------
# Worker processes
# ---------------------------------------------------------------------------------------------------------

_worker = {}


def worker_init(config, message_queue):
    # the main process handles Ctrl-C and shuts the workers down
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    src_client = connect_to_db(config['source_uri'], 4)
    tgt_client = connect_to_db(config['target_uri'], 4)
    src_coll = src_client[config['source_db']][config['source_coll']]
    tgt_coll = tgt_client[config['target_db']][config['target_coll']]
    _worker['config'] = config
    _worker['queue'] = message_queue
    _worker['pid'] = os.getpid()
    _worker['src'] = src_coll.with_options(codec_options=RAW_CODEC_OPTIONS)
    _worker['tgt'] = tgt_coll.with_options(codec_options=RAW_CODEC_OPTIONS)
    # documents that differ are decoded with the collection's codec options, dates outside the range supported
    # by Python's datetime are decoded as DatetimeMS instead of failing
    _worker['src_decode'] = src_coll.codec_options.with_options(datetime_conversion=DatetimeConversion.DATETIME_AUTO)
    _worker['tgt_decode'] = tgt_coll.codec_options.with_options(datetime_conversion=DatetimeConversion.DATETIME_AUTO)


class TaskReporter:
    """Accumulates counters and difference records for the current task and sends them to the main process."""

    def __init__(self, task_id, label):
        self.task_id = task_id
        self.totals = dict.fromkeys(COUNTER_NAMES, 0)
        self.delta = dict.fromkeys(COUNTER_NAMES, 0)
        self.records = []
        _worker['queue'].put(('start', task_id, _worker['pid'], label))

    def add(self, name, amount=1):
        self.delta[name] += amount

    def record(self, kind, _id, detail=None):
        self.records.append((kind, format_id(_id), None if detail is None else str(detail)))

    def flush(self):
        for name, amount in self.delta.items():
            self.totals[name] += amount
        _worker['queue'].put(('progress', self.task_id, _worker['pid'], self.delta, self.records))
        self.delta = dict.fromkeys(COUNTER_NAMES, 0)
        self.records = []

    def done(self, error=None, **extra):
        self.flush()
        result = dict(self.totals, **extra)
        _worker['queue'].put(('done', self.task_id, _worker['pid'], result, error))
        return result


def compare_window(reporter, src_docs, tgt_cursor, flush_every):
    """Compare a batch of source documents with the target documents of the same _id window. Every target
    document in the window that doesn't match a source document is an extra document in the target. Returns the
    number of target documents in the window."""
    tgt_count = 0
    src_by_key = {}
    for doc in src_docs:
        raw = doc.raw
        reporter.add('src_docs')
        reporter.add('src_bytes', len(raw))
        src_by_key[id_key(raw_doc_id(raw))] = raw
    for doc in tgt_cursor:
        raw = doc.raw
        tgt_count += 1
        if tgt_count % flush_every == 0:
            # a window can contain any number of extra target documents, report them as they're found
            reporter.flush()
        reporter.add('tgt_docs')
        reporter.add('tgt_bytes', len(raw))
        tgt_id = raw_doc_id(raw)
        src_raw = src_by_key.pop(id_key(tgt_id), None)
        if src_raw is None:
            reporter.add('extra_in_target')
            reporter.record('extra', tgt_id)
            continue
        diff = diff_raw_documents(src_raw, raw, _worker['src_decode'], _worker['tgt_decode'])
        if diff is None:
            reporter.add('identical')
        else:
            reporter.add('different')
            reporter.record('different', tgt_id, diff)
    for src_raw in src_by_key.values():
        reporter.add('missing_in_target')
        reporter.record('missing', raw_doc_id(src_raw))
    return tgt_count


def task_range_chunk(task):
    """Compare one _id range chunk of the source and target collections."""
    reporter = TaskReporter(task['task_id'], task['label'])
    try:
        batch_size = _worker['config']['batch_size']
        start, end, end_inclusive = task['start'], task['end'], task['end_inclusive']
        end_op = '$lte' if end_inclusive else '$lt'
        src_cursor = _worker['src'].find(chunk_filter(start, end, end_inclusive)).sort('_id', 1).batch_size(batch_size)
        lower_op, lower = '$gte', start
        src_count = tgt_count = 0
        while True:
            src_docs = list(islice(src_cursor, batch_size))
            if len(src_docs) == batch_size:
                # more source documents may follow, the window ends at the last _id of this batch
                upper_op, upper = '$lte', raw_doc_id(src_docs[-1].raw)
                final = False
            else:
                upper_op, upper = end_op, end
                final = True
            tgt_cursor = _worker['tgt'].find({'_id': {lower_op: lower, upper_op: upper}}).batch_size(batch_size)
            tgt_count += compare_window(reporter, src_docs, tgt_cursor, max(batch_size, 1000))
            src_count += len(src_docs)
            reporter.flush()
            if final:
                break
            lower_op, lower = '$gt', upper
        return reporter.done(src_count=src_count, tgt_count=tgt_count)
    except Exception as e:
        return reporter.done(error=f"{type(e).__name__}: {e}\n{traceback.format_exc()}")


def task_id_batch(task):
    """Compare the documents of a batch of source _id values, fetched from both collections with $in."""
    reporter = TaskReporter(task['task_id'], task['label'])
    try:
        ids = [raw_doc_id(raw) for raw in task['raw_ids']]
        src_by_key = {}
        for doc in _worker['src'].find({'_id': {'$in': ids}}):
            raw = doc.raw
            src_by_key[id_key(raw_doc_id(raw))] = raw
        tgt_by_key = {}
        for doc in _worker['tgt'].find({'_id': {'$in': ids}}):
            raw = doc.raw
            tgt_by_key[id_key(raw_doc_id(raw))] = raw
        for _id in ids:
            key = id_key(_id)
            src_raw = src_by_key.get(key)
            tgt_raw = tgt_by_key.get(key)
            if src_raw is None:
                # the document was deleted from the source after its _id was read
                reporter.add('source_vanished')
                reporter.record('vanished', _id)
                if tgt_raw is not None:
                    reporter.add('tgt_docs')
                    reporter.add('tgt_bytes', len(tgt_raw))
                continue
            reporter.add('src_docs')
            reporter.add('src_bytes', len(src_raw))
            if tgt_raw is None:
                reporter.add('missing_in_target')
                reporter.record('missing', _id)
                continue
            reporter.add('tgt_docs')
            reporter.add('tgt_bytes', len(tgt_raw))
            diff = diff_raw_documents(src_raw, tgt_raw, _worker['src_decode'], _worker['tgt_decode'])
            if diff is None:
                reporter.add('identical')
            else:
                reporter.add('different')
                reporter.record('different', _id, diff)
        return reporter.done()
    except Exception as e:
        return reporter.done(error=f"{type(e).__name__}: {e}\n{traceback.format_exc()}")


def task_reverse_batch(task):
    """Find target _id values of a batch that don't exist in the source."""
    reporter = TaskReporter(task['task_id'], task['label'])
    try:
        ids = [raw_doc_id(raw) for raw in task['raw_ids']]
        found = {id_key(raw_doc_id(doc.raw)) for doc in _worker['src'].find({'_id': {'$in': ids}}, {'_id': 1})}
        for _id in ids:
            if id_key(_id) not in found:
                reporter.add('extra_in_target')
                reporter.record('extra', _id)
        return reporter.done(checked=len(ids))
    except Exception as e:
        return reporter.done(error=f"{type(e).__name__}: {e}\n{traceback.format_exc()}")


# ---------------------------------------------------------------------------------------------------------
# Main process: statistics, output and user interface
# ---------------------------------------------------------------------------------------------------------

def human_bytes(num):
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if abs(num) < 1024 or unit == 'TB':
            return f"{num:,.0f} {unit}" if unit == 'B' else f"{num:,.1f} {unit}"
        num /= 1024.0


def human_duration(seconds):
    if seconds is None or math.isinf(seconds) or math.isnan(seconds):
        return '--:--:--'
    seconds = int(seconds)
    return f"{seconds // 3600:d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


class Stats:
    def __init__(self):
        self.counters = dict.fromkeys(COUNTER_NAMES, 0)
        self.reverse_checked = 0
        self.reverse_phase = False      # progress is measured by target _ids checked instead of source docs
        self.start_time = time.time()
        self.phase = 'Starting'
        self.total_docs = None          # documents expected to be compared, for the progress bar
        self.total_is_exact = False
        self.tasks_total = None         # None when the number of tasks isn't known in advance
        self.tasks_done = 0
        self.tasks_failed = 0
        self.chunk_count_mismatches = 0
        self.workers = {}               # pid -> {'n': index, 'label': str, 'docs': int}
        self.recent = []                # recent difference lines for the dashboard
        self.notes = []                 # additional findings (count mismatches, keyspace boundaries)

    @property
    def elapsed(self):
        return time.time() - self.start_time

    @property
    def differences(self):
        c = self.counters
        return c['different'] + c['missing_in_target'] + c['extra_in_target']


class DiffOutput:
    """Writes differences to the output file, only the main process writes to the file."""

    def __init__(self, path, max_console):
        self.file = open(path, 'a')
        self.max_console = max_console
        self.console_lines = 0
        self.console_print = print

    def write(self, text):
        self.file.write(str(text) + '\n')

    def record(self, stats, kind, id_text, detail):
        if kind == 'different':
            line = f"Difference found at doc id: {id_text}"
            self.write(line)
            self.write(detail)
        elif kind == 'missing':
            line = f"Document missing in target, _id: {id_text}"
            self.write(line)
        elif kind == 'extra':
            line = f"Document missing in source (extra in target), _id: {id_text}"
            self.write(line)
        elif kind == 'vanished':
            line = f"Document deleted from source during comparison, _id: {id_text}"
            self.write(line)
        else:
            line = f"{kind}: {id_text} {detail or ''}"
            self.write(line)
        stats.recent.append(line)
        del stats.recent[:-5]
        if self.console_lines < self.max_console:
            self.console_print(line)
            self.console_lines += 1
            if self.console_lines == self.max_console:
                self.console_print("... further differences are only written to the output file")

    def note(self, stats, text):
        self.write(text)
        stats.notes.append(text)
        self.console_print(text)

    def close(self):
        self.file.close()


class Dashboard:
    """Live terminal dashboard using rich, or plain periodic progress lines when not attached to a terminal."""

    def __init__(self, stats, title, details, plain=False, interval=10.0):
        self.stats = stats
        self.title = title
        self.details = details
        self.interval = interval
        self.last_plain = 0
        self.live = None
        self.console = None
        if RICH_AVAILABLE and not plain:
            self.console = Console()
            if not self.console.is_terminal:
                self.console = None
        self.last_rate_sample = (time.time(), 0)
        self.rate = 0.0
        self.byte_rate = 0.0
        self.last_byte_sample = 0

    def print(self, *args):
        if self.live is not None:
            self.live.console.print(*args, markup=False, highlight=False)
        else:
            print(*args, flush=True)

    def __enter__(self):
        if self.console is not None:
            self.live = Live(self.render(), console=self.console, refresh_per_second=4, transient=False)
            self.live.__enter__()
        return self

    def __exit__(self, *exc):
        if self.live is not None:
            self.live.update(self.render())
            self.live.__exit__(*exc)
            self.live = None

    def _update_rates(self):
        now = time.time()
        then, docs_then = self.last_rate_sample
        if now - then >= 1.0:
            c = self.stats.counters
            docs_now = self.stats.reverse_checked if self.stats.reverse_phase else c['src_docs']
            bytes_now = c['src_bytes'] + c['tgt_bytes']
            # exponentially smoothed rates
            inst_rate = (docs_now - docs_then) / (now - then)
            inst_byte_rate = (bytes_now - self.last_byte_sample) / (now - then)
            self.rate = inst_rate if self.rate == 0 else 0.7 * self.rate + 0.3 * inst_rate
            self.byte_rate = inst_byte_rate if self.byte_rate == 0 else 0.7 * self.byte_rate + 0.3 * inst_byte_rate
            self.last_rate_sample = (now, docs_now)
            self.last_byte_sample = bytes_now

    def refresh(self, force=False):
        self._update_rates()
        if self.live is not None:
            self.live.update(self.render())
        elif force or time.time() - self.last_plain >= self.interval:
            self.last_plain = time.time()
            print(self.plain_line(), flush=True)

    def _progress(self):
        s = self.stats
        done = s.reverse_checked if s.reverse_phase else s.counters['src_docs']
        total = s.total_docs
        if not total:
            return done, total, None, None
        pct = min(100.0, 100.0 * done / total)
        avg_rate = done / s.elapsed if s.elapsed > 0 else 0
        rate = self.rate or avg_rate
        eta = (total - done) / rate if rate > 0 and done < total else (0 if done >= total else None)
        return done, total, pct, eta

    def plain_line(self):
        s = self.stats
        c = s.counters
        done, total, pct, eta = self._progress()
        pct_text = f"{pct:5.1f}%" if pct is not None else '  ?  '
        tasks = f" chunks {s.tasks_done}/{s.tasks_total}" if s.tasks_total else ''
        return (f"[{human_duration(s.elapsed)}] {s.phase}: {pct_text} {done:,}/{total or 0:,} docs "
                f"({self.rate:,.0f} docs/s, {human_bytes(self.byte_rate)}/s) identical {c['identical']:,} "
                f"different {c['different']:,} missing-in-target {c['missing_in_target']:,} "
                f"extra-in-target {c['extra_in_target']:,}{tasks} eta {human_duration(eta)}")

    def render(self):
        s = self.stats
        c = s.counters
        done, total, pct, eta = self._progress()

        header = Table.grid(padding=(0, 2))
        header.add_column(style='bold cyan', no_wrap=True)
        header.add_column()
        for name, value in self.details:
            header.add_row(name, value)

        bar = Table.grid(padding=(0, 1))
        bar.add_column(no_wrap=True)
        bar.add_column(width=50)
        bar.add_column(no_wrap=True)
        estimate = '' if s.total_is_exact else ' (estimated)'
        bar.add_row(Text(s.phase, style='bold'),
                    ProgressBar(total=total or 100, completed=done if total else 0, width=50),
                    Text(f"{pct:5.1f}%  {done:,} / {total:,} docs{estimate}" if pct is not None
                         else f"{done:,} docs"))

        def colour(value, bad_style='bold red'):
            return Text(f"{value:,}", style=bad_style if value else 'green')

        metrics = Table.grid(padding=(0, 3))
        for _ in range(4):
            metrics.add_column(no_wrap=True)
        metrics.add_row(Text('Elapsed', style='cyan'), human_duration(s.elapsed),
                        Text('ETA', style='cyan'), human_duration(eta))
        metrics.add_row(Text('Source docs read', style='cyan'), f"{c['src_docs']:,}",
                        Text('Target docs read', style='cyan'), f"{c['tgt_docs']:,}")
        metrics.add_row(Text('Source data read', style='cyan'), human_bytes(c['src_bytes']),
                        Text('Target data read', style='cyan'), human_bytes(c['tgt_bytes']))
        metrics.add_row(Text('Throughput', style='cyan'), f"{self.rate:,.0f} docs/s",
                        Text('Data throughput', style='cyan'), f"{human_bytes(self.byte_rate)}/s")
        metrics.add_row(Text('Identical documents', style='cyan'), Text(f"{c['identical']:,}", style='green'),
                        Text('Different documents', style='cyan'), colour(c['different']))
        metrics.add_row(Text('Missing in target', style='cyan'), colour(c['missing_in_target']),
                        Text('Extra in target', style='cyan'), colour(c['extra_in_target']))
        tasks = f"{s.tasks_done:,} / {s.tasks_total:,}" if s.tasks_total else f"{s.tasks_done:,}"
        metrics.add_row(Text('Chunks complete' if s.tasks_total else 'Batches complete', style='cyan'), tasks,
                        Text('Chunk count mismatches', style='cyan'), colour(s.chunk_count_mismatches))
        if c['source_vanished'] or s.tasks_failed:
            metrics.add_row(Text('Deleted from source', style='cyan'), colour(c['source_vanished'], 'yellow'),
                            Text('Failed tasks', style='cyan'), colour(s.tasks_failed))

        workers = Table(box=None, padding=(0, 2), show_edge=False, header_style='bold cyan')
        workers.add_column('Worker', justify='right')
        workers.add_column('Working on')
        workers.add_column('Current docs', justify='right')
        workers.add_column('Total docs', justify='right')
        workers.add_column('Tasks done', justify='right')
        for info in sorted(s.workers.values(), key=lambda w: w['n']):
            busy = bool(info['label'])
            workers.add_row(str(info['n']), info['label'] if busy else Text('idle', style='dim'),
                            f"{info['docs']:,}" if busy else '', f"{info['total']:,}", f"{info['tasks']:,}")

        parts = [header, Text(''), bar, Text(''), metrics]
        if s.workers:
            parts += [Text(''), workers]
        if s.recent:
            parts += [Text(''), Text('Recent differences', style='bold red')]
            parts += [Text(line, overflow='ellipsis', no_wrap=True) for line in s.recent]
        return Panel(Group(*parts), title=f"[bold]{self.title}", border_style='blue')


class Coordinator:
    """Runs tasks in a pool of worker processes and aggregates their progress messages."""

    def __init__(self, args, stats, output, dashboard):
        self.args = args
        self.stats = stats
        self.output = output
        self.dashboard = dashboard
        ctx = multiprocessing.get_context('spawn')
        self.queue = ctx.Queue()
        config = {
            'source_uri': args.source_uri, 'target_uri': args.target_uri,
            'source_db': args.source_db, 'source_coll': args.source_coll,
            'target_db': args.target_db, 'target_coll': args.target_coll,
            'batch_size': args.batch_size,
        }
        self.executor = concurrent.futures.ProcessPoolExecutor(
            max_workers=args.workers, mp_context=ctx, initializer=worker_init, initargs=(config, self.queue))
        self.lock = threading.Lock()
        self.next_task_id = 0
        self.pending = {}             # task_id -> future, until its 'done' message has been processed
        self.results = {}             # task_id -> result dict
        self.errors = []
        self.on_done = None
        self.slots = threading.Semaphore(args.workers * 3)   # limits in-flight batches in batched mode
        self.slot_tasks = set()

    def submit(self, fn, task, uses_slot=False):
        with self.lock:
            task_id = self.next_task_id
            self.next_task_id += 1
            task['task_id'] = task_id
            if uses_slot:
                self.slot_tasks.add(task_id)
            self.pending[task_id] = self.executor.submit(fn, task)
        return task_id

    def _handle(self, message):
        kind, task_id, pid = message[0], message[1], message[2]
        s = self.stats
        worker = s.workers.get(pid)
        if worker is None:
            worker = s.workers[pid] = {'n': len(s.workers) + 1, 'label': '', 'docs': 0, 'total': 0, 'tasks': 0}
        if kind == 'start':
            worker['label'] = message[3]
            worker['docs'] = 0
        elif kind == 'progress':
            delta, records = message[3], message[4]
            for name, amount in delta.items():
                s.counters[name] += amount
            worker['docs'] += delta['src_docs']
            worker['total'] += delta['src_docs']
            for record_kind, id_text, detail in records:
                self.output.record(s, record_kind, id_text, detail)
        elif kind == 'done':
            result, error = message[3], message[4]
            worker['label'] = ''
            worker['tasks'] += 1
            with self.lock:
                self.pending.pop(task_id, None)
            self.results[task_id] = result
            s.tasks_done += 1
            if error:
                s.tasks_failed += 1
                self.errors.append(error)
                self.output.write(f"ERROR in task {task_id}: {error}")
                self.dashboard.print(f"ERROR in task {task_id}: {error.splitlines()[0]}")
            elif self.on_done is not None:
                self.on_done(task_id, result)
            with self.lock:
                holds_slot = task_id in self.slot_tasks
                self.slot_tasks.discard(task_id)
            if holds_slot:
                self.slots.release()

    def _check_broken(self):
        with self.lock:
            futures = list(self.pending.items())
        for task_id, future in futures:
            if future.done() and future.exception() is not None:
                raise RuntimeError(f"worker process failed: {future.exception()!r}")

    def wait(self, producers_alive=lambda: False):
        """Process messages until every submitted task has finished and the producers are done."""
        while True:
            try:
                message = self.queue.get(timeout=0.25)
                self._handle(message)
                # drain whatever else is queued before refreshing the display
                for _ in range(1000):
                    self._handle(self.queue.get_nowait())
            except queue.Empty:
                pass
            self.dashboard.refresh()
            with self.lock:
                idle = not self.pending
            if idle and not producers_alive():
                with self.lock:
                    if not self.pending:
                        break
            self._check_broken()

    def shutdown(self, kill=False):
        if kill:
            for process in list(getattr(self.executor, '_processes', {}).values()):
                try:
                    process.terminate()
                except Exception:
                    pass
            self.executor.shutdown(wait=False, cancel_futures=True)
        else:
            self.executor.shutdown(wait=True)


# ---------------------------------------------------------------------------------------------------------
# Comparison orchestration
# ---------------------------------------------------------------------------------------------------------

def get_id_bounds(coll):
    """The minimum and maximum _id of a collection, (None, None) if it's empty."""
    coll = coll.with_options(codec_options=ID_CODEC_OPTIONS)
    first = list(coll.find({}, {'_id': 1}).sort('_id', 1).limit(1))
    if not first:
        return None, None
    last = list(coll.find({}, {'_id': 1}).sort('_id', -1).limit(1))
    return first[0]['_id'], last[0]['_id']


class BackgroundCount(threading.Thread):
    """Exact document count of a collection, runs while the comparison is in progress."""

    def __init__(self, coll, on_result=None):
        super().__init__(daemon=True)
        self.coll = coll
        self.count = None
        self.error = None
        self.on_result = on_result

    def run(self):
        try:
            self.count = self.coll.count_documents({})
            if self.on_result:
                self.on_result(self.count)
        except Exception as e:
            self.error = e


# Compare indexes between the two collections, the order of fields within an index key is significant
def compare_indexes(srcCollection, tgtCollection, output):
    src_indexes = srcCollection.index_information()
    tgt_indexes = tgtCollection.index_information()
    # ignore the namespace, it differs whenever the database or collection names are not the same
    diff = DeepDiff(src_indexes, tgt_indexes, verbose_level=2, exclude_regex_paths=[r"root\[.+?\]\['ns'\]"])
    if diff:
        output.write("Index differences:")
        output.write(diff)
        return False
    return True


def run_range_partitioned(coord, stats, output, kind, lo, hi, args):
    chunks = compute_chunks(id_to_ordinal(lo, kind), id_to_ordinal(hi, kind), args.chunks)
    stats.tasks_total = len(chunks)
    stats.phase = 'Comparing'
    chunk_info = {}

    def on_done(task_id, result):
        info = chunk_info[task_id]
        if result['src_count'] != result['tgt_count']:
            stats.chunk_count_mismatches += 1
            output.write(f"Chunk {info['n']} {info['range']} document count differs: "
                         f"source {result['src_count']:,}, target {result['tgt_count']:,}")

    coord.on_done = on_done
    for n, (start, end, end_inclusive) in enumerate(chunks, 1):
        start_id, end_id = ordinal_to_id(start, kind), ordinal_to_id(end, kind)
        range_text = f"[{format_id(start_id)}, {format_id(end_id)}{']' if end_inclusive else ')'}"
        short_range = f"[{short_id(start_id)}, {short_id(end_id)}{']' if end_inclusive else ')'}"
        task = {'start': start_id, 'end': end_id, 'end_inclusive': end_inclusive,
                'label': f"chunk {n}/{len(chunks)} {short_range}"}
        task_id = coord.submit(task_range_chunk, task)
        chunk_info[task_id] = {'n': n, 'range': range_text}
    coord.wait()


def run_id_batches(coord, stats, cursor, task_fn, label, args, dedupe=False):
    """Stream _id values from a cursor in the main process and compare them in batches in the workers."""
    producer_error = []
    batch_size = args.batch_size

    def produce():
        try:
            seen = set() if dedupe else None
            batch = []
            n = 0
            for doc in cursor:
                raw = doc.raw
                if seen is not None:
                    key = id_key(raw_doc_id(raw))
                    if key in seen:
                        continue
                    seen.add(key)
                batch.append(raw)
                if len(batch) == batch_size:
                    n += 1
                    coord.slots.acquire()
                    coord.submit(task_fn, {'raw_ids': batch, 'label': f"{label} batch {n:,}"}, uses_slot=True)
                    batch = []
            if batch:
                n += 1
                coord.slots.acquire()
                coord.submit(task_fn, {'raw_ids': batch, 'label': f"{label} batch {n:,}"}, uses_slot=True)
        except Exception as e:
            producer_error.append(e)

    producer = threading.Thread(target=produce, daemon=True)
    producer.start()
    coord.wait(producers_alive=producer.is_alive)
    producer.join()
    if producer_error:
        raise producer_error[0]


def compare_collections(args):
    run_start = datetime.datetime.now()
    src_client = connect_to_db(args.source_uri, 10)
    tgt_client = connect_to_db(args.target_uri, 10)
    src_coll = src_client[args.source_db][args.source_coll]
    tgt_coll = tgt_client[args.target_db][args.target_coll]
    src_raw = src_coll.with_options(codec_options=RAW_CODEC_OPTIONS)
    tgt_raw = tgt_coll.with_options(codec_options=RAW_CODEC_OPTIONS)

    stats = Stats()
    output = DiffOutput(args.output_file, args.max_console_differences)
    output.write(f"===== data-differ run started {run_start.strftime('%Y-%m-%d %H:%M:%S')} =====")
    output.write(f"Source: {describe_uri(args.source_uri)} {args.source_db}.{args.source_coll}")
    output.write(f"Target: {describe_uri(args.target_uri)} {args.target_db}.{args.target_coll}")

    print(f"Starting data differ at {run_start.strftime('%Y-%m-%d %H:%M:%S')}, output is saved to {args.output_file}")

    # exact counts run in the background, they're verified against the number of documents scanned
    src_counter = BackgroundCount(src_coll)
    tgt_counter = BackgroundCount(tgt_coll)
    src_counter.start()
    tgt_counter.start()

    src_min, src_max = get_id_bounds(src_coll)
    tgt_min, tgt_max = get_id_bounds(tgt_coll)

    def finish_counts():
        src_counter.join()
        tgt_counter.join()
        for counter in (src_counter, tgt_counter):
            if counter.error:
                raise counter.error
        return src_counter.count, tgt_counter.count

    # empty collections
    if src_min is None or tgt_min is None:
        src_count, tgt_count = finish_counts()
        output.write(f"Count of documents in source:{src_count}")
        output.write(f"Count of documents in target:{tgt_count}")
        if src_min is None:
            print("No documents found in the source collection, please re-check you selected the right source collection.")
        if tgt_min is None:
            print("No documents found in the target collection, please re-check you selected the right target collection.")
        output.write("Result: " + ("both collections are empty" if src_count == tgt_count == 0
                                   else "DIFFERENCES FOUND, one collection is empty"))
        output.close()
        return EXIT_ERROR if src_count == tgt_count == 0 else EXIT_DIFFERENCES

    # choose the comparison strategy
    kinds = {partition_kind(v) for v in (src_min, src_max, tgt_min, tgt_max)}
    kind = kinds.pop() if len(kinds) == 1 else None
    if args.sample_size_percent:
        mode, reason = 'sample', f"sampling {args.sample_size_percent}% of the source documents"
    elif args.no_partition:
        mode, reason = 'batched', '--no-partition was specified'
    elif kind is None:
        mode, reason = 'batched', ("the _id values aren't all ObjectId, date, or integer values of a single type "
                                   "(min/max _id: source " f"{format_id(src_min)}/{format_id(src_max)}, target "
                                   f"{format_id(tgt_min)}/{format_id(tgt_max)})")
    else:
        mode, reason = 'range', f"all _id values are {kind} values"

    details = [
        ('Source', f"{args.source_db}.{args.source_coll} @ {describe_uri(args.source_uri)}"),
        ('Target', f"{args.target_db}.{args.target_coll} @ {describe_uri(args.target_uri)}"),
        ('_id range', f"source {short_id(src_min)} .. {short_id(src_max)}"),
        ('', f"target {short_id(tgt_min)} .. {short_id(tgt_max)}"),
    ]
    if mode == 'range':
        mode_text = f"range partitioned by _id ({kind}), {args.workers} workers, up to {args.chunks} chunks"
    elif mode == 'sample':
        mode_text = f"sampled ({args.sample_size_percent}%), {args.workers} workers, batch size {args.batch_size}"
    else:
        mode_text = f"batched _id lookups, {args.workers} workers, batch size {args.batch_size}"
    details.insert(0, ('Mode', mode_text))
    output.write(f"Mode: {mode_text} ({reason})")
    print(f"Mode: {mode_text}")
    print(f"  reason: {reason}")

    # progress bar total, an estimate until the exact count is available
    try:
        stats.total_docs = src_coll.estimated_document_count()
    except Exception:
        stats.total_docs = None

    def exact_source_count(count):
        if mode != 'sample':
            stats.total_docs = count
            stats.total_is_exact = True
    src_counter.on_result = exact_source_count
    if src_counter.count is not None:
        exact_source_count(src_counter.count)

    indexes_identical = compare_indexes(src_coll, tgt_coll, output)
    print("Indexes: identical" if indexes_identical else "Indexes: DIFFERENT, see the output file")

    if mode == 'range' and (src_min != tgt_min or src_max != tgt_max):
        if id_key(src_min) != id_key(tgt_min):
            output.note(stats, f"Minimum _id differs: source {format_id(src_min)}, target {format_id(tgt_min)}")
        if id_key(src_max) != id_key(tgt_max):
            output.note(stats, f"Maximum _id differs: source {format_id(src_max)}, target {format_id(tgt_max)}")

    dashboard = Dashboard(stats, 'DocumentDB Data Differ', details, plain=args.plain_progress,
                          interval=args.progress_interval)
    output.console_print = dashboard.print
    coord = Coordinator(args, stats, output, dashboard)
    interrupted = False
    exit_code = EXIT_NO_DIFFERENCES
    reverse_done = False
    try:
        with dashboard:
            if mode == 'range':
                lo = min((src_min, tgt_min), key=lambda v: id_to_ordinal(v, kind))
                hi = max((src_max, tgt_max), key=lambda v: id_to_ordinal(v, kind))
                run_range_partitioned(coord, stats, output, kind, lo, hi, args)
            elif mode == 'sample':
                src_count = src_counter.count if src_counter.count is not None else finish_counts()[0]
                docs_to_sample = int(args.sample_size_percent / 100 * src_count)
                stats.total_docs, stats.total_is_exact = docs_to_sample, True
                stats.phase = 'Sampling'
                cursor = src_raw.aggregate([{'$sample': {'size': docs_to_sample}}, {'$project': {'_id': 1}}],
                                           batchSize=args.batch_size, maxTimeMS=args.sampling_timeout_ms)
                run_id_batches(coord, stats, cursor, task_id_batch, 'sample', args, dedupe=True)
            else:
                stats.phase = 'Comparing'
                cursor = src_raw.find({}, {'_id': 1}).sort('_id', 1).batch_size(max(args.batch_size, 1000))
                run_id_batches(coord, stats, cursor, task_id_batch, 'source', args)

            stats.phase = 'Verifying counts'
            dashboard.refresh(force=True)
            src_count, tgt_count = finish_counts()
            output.write(f"Count of documents in source:{src_count}")
            output.write(f"Count of documents in target:{tgt_count}")
            c = stats.counters
            matched = c['identical'] + c['different']

            if coord.errors:
                pass    # the comparison is incomplete, the document totals can't be verified
            elif mode == 'range':
                if args.check_target:
                    dashboard.print("Note: --check-target isn't needed in range partitioned mode, extra target "
                                    "documents are always detected")
                if c['src_docs'] != src_count:
                    output.note(stats, f"Source documents compared ({c['src_docs']:,}) doesn't match the source count "
                                f"({src_count:,}), the collection changed during the comparison")
                if c['tgt_docs'] != tgt_count:
                    output.note(stats, f"Target documents compared ({c['tgt_docs']:,}) doesn't match the target count "
                                f"({tgt_count:,}), the collection changed during the comparison")
            elif mode == 'batched':
                if c['src_docs'] + c['source_vanished'] != src_count:
                    output.note(stats, f"Source documents compared ({c['src_docs'] + c['source_vanished']:,}) "
                                f"doesn't match the source count ({src_count:,}), the collection changed during "
                                f"the comparison")
            if mode in ('batched', 'sample') and not coord.errors:
                # every matched target document has a unique _id, so if the target count equals the number of
                # matched documents the target can't contain documents that aren't in the source
                need_reverse = args.check_target or (mode == 'batched' and tgt_count != matched)
                if mode == 'batched' and tgt_count == matched:
                    dashboard.print("Target count equals the number of matched documents, the target contains no "
                                    "extra documents (reverse check not needed)")
                elif mode == 'batched' and not args.check_target:
                    output.note(stats, f"The target contains {tgt_count - matched:,} document(s) that weren't matched "
                                f"to a source document, use --check-target to list them")
                    need_reverse = False
                if need_reverse:
                    stats.phase = 'Checking target for extra documents'
                    stats.total_docs, stats.total_is_exact = tgt_count, True
                    stats.reverse_phase = True
                    dashboard.rate = dashboard.byte_rate = 0.0
                    dashboard.last_rate_sample = (time.time(), 0)

                    def on_reverse_done(task_id, result):
                        stats.reverse_checked += result['checked']
                    coord.on_done = on_reverse_done
                    cursor = tgt_raw.find({}, {'_id': 1}).sort('_id', 1).batch_size(max(args.batch_size, 1000))
                    run_id_batches(coord, stats, cursor, task_reverse_batch, 'target', args)
                    reverse_done = True
                    if mode == 'batched' and not coord.errors and matched + c['extra_in_target'] != stats.reverse_checked:
                        output.note(stats, f"Target documents checked ({stats.reverse_checked:,}) doesn't match the "
                                    f"documents matched plus extra documents found "
                                    f"({matched + c['extra_in_target']:,}), the collection changed during the comparison")
            stats.phase = 'Complete' if not coord.errors else 'Complete (with errors)'
    except KeyboardInterrupt:
        interrupted = True
        stats.phase = 'Interrupted'
    except Exception as e:
        coord.errors.append(f"{type(e).__name__}: {e}")
        output.write(f"ERROR: {type(e).__name__}: {e}\n{traceback.format_exc()}")
        print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
    finally:
        coord.shutdown(kill=interrupted or bool(coord.errors))

    # summary
    c = stats.counters
    elapsed = stats.elapsed
    summary = [
        "----- Summary -----",
        f"Mode:                        {mode_text}",
        f"Elapsed:                     {human_duration(elapsed)} ({elapsed:,.1f} seconds)",
        f"Source documents compared:   {c['src_docs']:,} ({human_bytes(c['src_bytes'])})",
        f"Target documents compared:   {c['tgt_docs']:,} ({human_bytes(c['tgt_bytes'])})",
        f"Throughput:                  {c['src_docs'] / elapsed if elapsed else 0:,.0f} docs/s, "
        f"{human_bytes((c['src_bytes'] + c['tgt_bytes']) / elapsed if elapsed else 0)}/s",
        f"Identical documents:         {c['identical']:,}",
        f"Different documents:         {c['different']:,}",
        f"Missing in target:           {c['missing_in_target']:,}",
        f"Extra in target:             {c['extra_in_target']:,}" + ('' if mode == 'range' or reverse_done
                                                                     else ' (not checked)'),
    ]
    if mode == 'range':
        summary.append(f"Chunks:                      {stats.tasks_done - stats.tasks_failed:,} of {stats.tasks_total:,} "
                       f"complete, {stats.chunk_count_mismatches:,} with a different document count"
                       + (f", {stats.tasks_failed:,} FAILED" if stats.tasks_failed else ''))
    elif stats.tasks_failed:
        summary.append(f"Failed batches:              {stats.tasks_failed:,}")
    if c['source_vanished']:
        summary.append(f"Deleted from source:         {c['source_vanished']:,}")
    if not indexes_identical:
        summary.append("Indexes:                     DIFFERENT")
    for note in stats.notes:
        summary.append(f"Note: {note}")

    if interrupted:
        result = "INTERRUPTED, the comparison is incomplete"
        exit_code = EXIT_ERROR
    elif coord.errors:
        result = f"ERROR, {len(coord.errors)} task(s) failed, the comparison is incomplete"
        exit_code = EXIT_ERROR
    elif stats.differences or stats.notes or not indexes_identical or c['source_vanished']:
        result = "DIFFERENCES FOUND"
        exit_code = EXIT_DIFFERENCES
    else:
        result = "NO DIFFERENCES FOUND"
    summary.append(f"Result:                      {result}")
    for line in summary:
        output.write(line)
        print(line)
    output.close()
    return exit_code


def main():
    parser = argparse.ArgumentParser(description='Compare two collections and report differences.')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for bulk reads (optional, default: 1000)')
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
    parser.add_argument('--workers', type=int, default=os.cpu_count() or 4, help=f'Degree of parallelism, number of worker processes (optional, default: number of CPUs, {os.cpu_count()})')
    parser.add_argument('--chunks', type=int, default=None, help='Number of _id range chunks in range partitioned mode (optional, default: 8 x workers)')
    parser.add_argument('--no-partition', action='store_true', default=False, help='optional, never use range partitioning, always use batched _id lookups')
    parser.add_argument('--max-console-differences', type=int, default=20, help='Maximum number of differences printed to the console, all differences are written to the output file (optional, default: 20)')
    parser.add_argument('--plain-progress', action='store_true', default=False, help='optional, print plain progress lines instead of the live dashboard')
    parser.add_argument('--progress-interval', type=float, default=10.0, help='Seconds between plain progress lines (optional, default: 10)')
    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error('--batch-size must be at least 1')
    if args.workers < 1:
        parser.error('--workers must be at least 1')
    if args.chunks is None:
        args.chunks = args.workers * 8
    if args.chunks < 1:
        parser.error('--chunks must be at least 1')
    if args.sample_size_percent is not None and not 0 < args.sample_size_percent <= 100:
        parser.error('--sample-size-percent must be between 1 and 100')

    sys.exit(compare_collections(args))


if __name__ == '__main__':
    main()
