"""Tests for issues #205 (sampling and cache provenance), #210 (_id_ never
redundant) and #211 (compat-tool default version).

Run from the metrics-collector directory:  python -m pytest tests/
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import metrics_collector as mc  # noqa: E402

COMPAT = Path(__file__).resolve().parents[3] / "compat-tool" / "compat.py"


# ---------------------------------------------------------------- #205 defect 1

def test_nested_value_scalar_path():
    assert mc._get_nested_value({"a": {"b": 1}}, "a.b") == (1, True)
    assert mc._get_nested_value({"a": 1}, "b") == (None, False)


def test_nested_value_traverses_array_of_subdocuments():
    doc = {"tags": [{"name": "x"}, {"name": "yy"}, {"other": 1}]}
    assert mc._get_nested_value(doc, "tags.name") == (["x", "yy"], True)


def test_nested_value_nested_arrays_flatten():
    doc = {"a": [{"b": [{"c": 1}, {"c": 2}]}, {"b": [{"c": 3}]}]}
    assert mc._get_nested_value(doc, "a.b.c") == ([1, 2, 3], True)


def test_nested_value_positional_component():
    assert mc._get_nested_value({"a": [{"b": 5}, {"b": 6}]}, "a.1.b") == (6, True)


def test_nested_value_empty_array_is_found_and_multikey():
    assert mc._get_nested_value({"tags": []}, "tags.name") == ([], True)


class _Cursor(list):
    def limit(self, n):
        return _Cursor(self[:n])

    def sort(self, *_a, **_k):
        return _Cursor(list(reversed(self)))


class _Coll:
    def __init__(self, docs):
        self.docs = docs

    def find(self, *_a, **_k):
        return _Cursor(self.docs)


def test_key_types_measure_array_paths():
    docs = [{"_id": i, "tags": [{"name": "abcd"} for _ in range(4)]} for i in range(10)]
    kt = mc._sample_index_key_types({"c": _Coll(docs)}, "c", {"tags.name": 1})[0]
    assert kt["bson_type"] == "string"
    assert kt["avg_bytes"] == 4
    assert kt["is_array"] is True
    assert kt["avg_array_length"] == 4.0
    assert kt["sampled"] is True and kt["sampled_docs"] == 10


def test_key_types_mark_unsampled_defaults():
    docs = [{"_id": i, "x": 1} for i in range(5)]
    kt = mc._sample_index_key_types({"c": _Coll(docs)}, "c", {"missing": 1})[0]
    assert kt["bson_type"] == "unknown" and kt["avg_bytes"] == 32
    assert kt["sampled"] is False and kt["sampled_docs"] == 0
    empty = mc._sample_index_key_types({"c": _Coll([])}, "c", {"a": 1})[0]
    assert empty["sampled"] is False


# ---------------------------------------------------------------- #210

def _idx(name, key, **kw):
    d = {"namespace": "db.c", "name": name, "key": key}
    d.update(kw)
    return d


def test_id_index_never_redundant():
    idx = [_idx("_id_", {"_id": 1}), _idx("_id_1_ts_1", {"_id": 1, "ts": 1})]
    mc._mark_redundant_indexes(idx)
    assert not idx[0].get("redundant")


def test_prefix_index_still_redundant():
    idx = [_idx("a_1", {"a": 1}), _idx("a_1_b_1", {"a": 1, "b": 1})]
    mc._mark_redundant_indexes(idx)
    assert idx[0]["redundant"] is True and idx[0]["redundant_of"] == "a_1_b_1"
    assert not idx[1].get("redundant")


def test_unique_sparse_partial_never_redundant():
    idx = [_idx("u", {"a": 1}, unique=True), _idx("s", {"a": 1}, sparse=True),
           _idx("p", {"a": 1}, partial=True), _idx("a_1_b_1", {"a": 1, "b": 1})]
    mc._mark_redundant_indexes(idx)
    assert [i.get("redundant") for i in idx[:3]] == [None, None, None]


def test_redundancy_is_per_shard():
    idx = [_idx("a_1", {"a": 1}, shard="s0"), _idx("a_1_b_1", {"a": 1, "b": 1}, shard="s1")]
    mc._mark_redundant_indexes(idx)
    assert not idx[0].get("redundant")


def test_no_inline_redundancy_loops_remain():
    src = (Path(mc.__file__)).read_text()
    import re
    calls = re.findall(r"^\s+_mark_redundant_indexes\(all_indexes", src, re.M)
    assert len(calls) == 3, calls
    assert "# idx is redundant if it's a prefix of other" not in src


# ---------------------------------------------------------------- #211

def test_compat_default_version_read_from_tool():
    mc._COMPAT_DEFAULT_VERSION = None
    v = mc._compat_default_version(COMPAT)
    assert v and v[0].isdigit(), v
    assert f"default: {v}" in " ".join(
        __import__("subprocess").run([sys.executable, str(COMPAT), "--help"],
                                     capture_output=True, text=True).stdout.split())


def test_compat_never_pinned_to_a_version():
    src = Path(mc.__file__).read_text()
    assert '"--version", version' not in src
    assert 'for version in ["8.0"]' not in src
    assert 'version = "8.0"' not in src


# ---------------------------------------------------------------- #205 defects 2-4

def test_cache_used_gb_stamped_into_collstats(tmp_path):
    (tmp_path / "collstats.json").write_text(json.dumps({"wt_server_cache": {}, "collections": []}))
    procs = [
        {"host": "n0:27017", "wiredtiger": {"cache_used_bytes": {"p95": 2e9, "max": 4e9}}},
        {"host": "n1:27017", "wiredtiger": {"cache_used_bytes": {"p95": 3e9, "max": 3e9}}},
        {"host": "config-0:27017", "wiredtiger": {"cache_used_bytes": {"p95": 9e9, "max": 9e9}}},
    ]
    v = mc._stamp_cache_used_into_collstats(tmp_path, procs, 95, "PT5M", "P14D", None)
    data = json.loads((tmp_path / "collstats.json").read_text())
    assert v == 3.0 and data["cache_used_gb"] == 3.0
    src = data["cache_used_gb_source"]
    assert src["statistic"] == "p95" and src["saturated_nodes"] == 1
    assert set(src["per_node_gb"]) == {"n0:27017", "n1:27017"}
    assert data["collections"] == []


def test_cache_used_gb_noop_without_collstats(tmp_path):
    assert mc._stamp_cache_used_into_collstats(tmp_path, [], 95, "PT5M", "P14D", None) is None


def test_ec2_sampled_cache_has_own_name(tmp_path):
    (tmp_path / "collstats.json").write_text(json.dumps({"wt_server_cache": {}, "collections": []}))
    sampling = {"samples": 2, "delta_interval_seconds": 60, "snapshots": [
        {"ts": "t0", "wt_cache": {"bytes_currently_in_cache": 1e9, "maximum_bytes_configured": 4e9}},
        {"ts": "t1", "wt_cache": {"bytes_currently_in_cache": 1.5e9, "maximum_bytes_configured": 4e9}},
        {"ts": "t2", "wt_cache": {"bytes_currently_in_cache": 1.2e9, "maximum_bytes_configured": 4e9}},
    ]}
    mc._ec2_stamp_sampled_cache(tmp_path, sampling, "h:27017")
    data = json.loads((tmp_path / "collstats.json").read_text())
    assert "cache_used_gb" not in data
    assert data["cache_sampled_gb"] == 1.5
    s = data["cache_sampled_gb_source"]
    assert s["readings"] == 3 and s["span_seconds"] == 120 and s["cache_configured_gb"] == 4.0


def test_mongos_fallback_declares_cache_loss():
    src = Path(mc.__file__).read_text()
    assert '"wt_server_caches_unavailable"' in src
    assert "Loss: `wt_server_cache.max_bytes_configured` per shard" not in src
