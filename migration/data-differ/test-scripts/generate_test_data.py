"""
Load a large test data set into a source and a target collection, optionally with known differences, and verify
a data-differ output file against those known differences. Useful for performance testing and for validating
the results of the tool at scale.

    export SOURCE_URI="mongodb://..."  TARGET_URI="mongodb://..."

    # 10 million documents with ObjectId _id values, 100 missing in the target, 50 extra, 1000 different
    python3 generate_test_data.py load --db perf --coll test --docs 10000000 --id-type objectid \\
        --missing 100 --extra 50 --different 1000 --truth-file truth.json

    python3 ../data-differ.py --source-uri "$SOURCE_URI" --target-uri "$TARGET_URI" --source-db perf \\
        --source-coll test --target-db perf --target-coll test --output-file differences.txt

    python3 generate_test_data.py verify --truth-file truth.json --output-file differences.txt

    python3 generate_test_data.py drop --db perf --coll test
"""

import argparse
import datetime
import json
import os
import random
import re
import string
import sys
import time
import warnings
from multiprocessing import Pool

from bson import ObjectId, json_util
from pymongo import MongoClient

warnings.filterwarnings("ignore", "You appear to be connected to a DocumentDB cluster.")

BASE_TIME = 1600000000
BASE_DATE = datetime.datetime(2020, 1, 1)


def make_id(id_type, i, rnd):
    """Unique, increasing _id for document number i."""
    if id_type == 'objectid':
        # 50 documents per second of ObjectId time, the remaining bytes are random but unique per i
        return ObjectId((BASE_TIME + i // 50).to_bytes(4, 'big') + rnd.randbytes(4) + i.to_bytes(4, 'big'))
    if id_type == 'int':
        return i * 3
    if id_type == 'date':
        return BASE_DATE + datetime.timedelta(milliseconds=i * 7)
    return f"key-{i:012d}"


def make_doc(_id, i, rnd, size):
    return {'_id': _id, 'i': i, 'name': ''.join(rnd.choices(string.ascii_letters, k=20)),
            'ts': BASE_DATE + datetime.timedelta(seconds=i), 'tags': [rnd.randint(0, 100) for _ in range(5)],
            'nested': {'a': rnd.random(), 'b': 'x' * rnd.randint(size // 2, size), 'c': {'d': i}}}


def load_part(job):
    args, part, parts, missing, extra_after, different = job
    rnd = random.Random(part)
    src = MongoClient(os.environ['SOURCE_URI'])[args['db']][args['coll']]
    tgt = MongoClient(os.environ['TARGET_URI'])[args['db']][args['coll']]
    per = (args['docs'] + parts - 1) // parts
    start, end = part * per, min(args['docs'], (part + 1) * per)
    for batch_start in range(start, end, 1000):
        src_docs, tgt_docs = [], []
        for i in range(batch_start, min(batch_start + 1000, end)):
            doc = make_doc(make_id(args['id_type'], i, rnd), i, rnd, args['size'])
            src_docs.append(doc)
            if i in missing:
                continue
            if i in different:
                doc = dict(doc, name='changed')
            tgt_docs.append(doc)
            if i in extra_after:
                # an extra target document with an _id between document i and i + 1
                tgt_docs.append({'_id': extra_after[i], 'extra': True})
        if src_docs:
            src.insert_many(src_docs, ordered=False)
        if tgt_docs:
            tgt.insert_many(tgt_docs, ordered=False)


def extra_id(id_type, i):
    """An _id that sorts between the _id of document i and document i + 1."""
    if id_type == 'objectid':
        return ObjectId((BASE_TIME + i // 50).to_bytes(4, 'big') + b'\xff' * 4 + i.to_bytes(4, 'big'))
    if id_type == 'int':
        return i * 3 + 1
    if id_type == 'date':
        return BASE_DATE + datetime.timedelta(milliseconds=i * 7 + 3)
    return f"key-{i:012d}-extra"


def cmd_load(args):
    rnd = random.Random(args.seed)
    picks = rnd.sample(range(args.docs), args.missing + args.extra + args.different)
    missing = set(picks[:args.missing])
    extra_after = {i: extra_id(args.id_type, i) for i in picks[args.missing:args.missing + args.extra]}
    different = set(picks[args.missing + args.extra:])
    config = {'db': args.db, 'coll': args.coll, 'docs': args.docs, 'id_type': args.id_type, 'size': args.size}
    start = time.time()
    with Pool(args.processes) as pool:
        jobs = [(config, part, args.processes, missing, extra_after, different) for part in range(args.processes)]
        pool.map(load_part, jobs)
    print(f"loaded {args.docs:,} documents in {time.time() - start:,.1f} seconds")

    # the _id values of the picked documents, read back from the source in document order
    src = MongoClient(os.environ['SOURCE_URI'])[args.db][args.coll]
    by_i = {doc['i']: doc['_id'] for doc in src.find({'i': {'$in': list(missing | different)}}, {'i': 1})}
    truth = {
        'missing': [json_util.dumps(by_i[i]) for i in sorted(missing)],
        'extra': [json_util.dumps(v) for v in extra_after.values()],
        'different': [json_util.dumps(by_i[i]) for i in sorted(different)],
    }
    with open(args.truth_file, 'w') as f:
        json.dump(truth, f, indent=1)
    print(f"known differences written to {args.truth_file}: {len(missing)} missing, {len(extra_after)} extra, "
          f"{len(different)} different")


def normalize(id_text):
    return json_util.dumps(json_util.loads(id_text))


def cmd_verify(args):
    with open(args.truth_file) as f:
        truth = {k: {normalize(v) for v in values} for k, values in json.load(f).items()}
    with open(args.output_file) as f:
        text = f.read()
    text = text[text.rfind('===== data-differ run started'):]
    reported = {
        'missing': {normalize(v) for v in re.findall(r'^Document missing in target, _id: (.*)$', text, re.M)},
        'extra': {normalize(v) for v in re.findall(r'^Document missing in source \(extra in target\), _id: (.*)$', text, re.M)},
        'different': {normalize(v) for v in re.findall(r'^Difference found at doc id: (.*)$', text, re.M)},
    }
    ok = True
    for kind in ('missing', 'extra', 'different'):
        expected, got = truth[kind], reported[kind]
        if expected == got:
            print(f"{kind:10s} OK ({len(got):,})")
        else:
            ok = False
            print(f"{kind:10s} MISMATCH expected {len(expected):,} reported {len(got):,}, "
                  f"not reported {sorted(expected - got)[:5]}, unexpected {sorted(got - expected)[:5]}")
    sys.exit(0 if ok else 1)


def cmd_drop(args):
    MongoClient(os.environ['SOURCE_URI'])[args.db][args.coll].drop()
    MongoClient(os.environ['TARGET_URI'])[args.db][args.coll].drop()
    print(f"dropped {args.db}.{args.coll} from the source and the target")


def main():
    parser = argparse.ArgumentParser(description='Generate and verify data-differ test data sets.')
    sub = parser.add_subparsers(dest='command', required=True)
    load = sub.add_parser('load', help='load documents into the source and target collections')
    load.add_argument('--db', required=True)
    load.add_argument('--coll', required=True)
    load.add_argument('--docs', type=int, required=True)
    load.add_argument('--id-type', choices=['objectid', 'int', 'date', 'string'], default='objectid')
    load.add_argument('--size', type=int, default=400, help='approximate payload size in bytes (default 400)')
    load.add_argument('--missing', type=int, default=0, help='documents missing in the target')
    load.add_argument('--extra', type=int, default=0, help='extra documents in the target')
    load.add_argument('--different', type=int, default=0, help='documents that are different in the target')
    load.add_argument('--processes', type=int, default=os.cpu_count())
    load.add_argument('--seed', type=int, default=1)
    load.add_argument('--truth-file', default='truth.json')
    verify = sub.add_parser('verify', help='verify a data-differ output file against the known differences')
    verify.add_argument('--truth-file', default='truth.json')
    verify.add_argument('--output-file', default='differences.txt')
    drop = sub.add_parser('drop', help='drop the source and target collections')
    drop.add_argument('--db', required=True)
    drop.add_argument('--coll', required=True)
    args = parser.parse_args()
    if not os.environ.get('SOURCE_URI') and args.command != 'verify':
        parser.error('set the SOURCE_URI and TARGET_URI environment variables')
    {'load': cmd_load, 'verify': cmd_verify, 'drop': cmd_drop}[args.command](args)


if __name__ == '__main__':
    main()
