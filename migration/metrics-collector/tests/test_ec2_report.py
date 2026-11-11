"""Tests for the --source ec2 sizing report, summary, CSV inputs and
operator_usage.json (v2.6.0).

Run from the metrics-collector directory:  python -m pytest tests/
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import metrics_collector as mc  # noqa: E402

TS = ["2026-11-10T00:00:00+00:00", "2026-11-10T00:05:00+00:00", "2026-11-10T00:10:00+00:00"]


def _cw():
    vol = {"VolumeReadOps": {"timestamps": TS, "values": [60.0, 120.0, 0.0]},
           "VolumeWriteOps": {"timestamps": TS, "values": [600.0, 600.0, 600.0]},
           "VolumeTotalReadTime": {"timestamps": TS, "values": [0.06, 0.24, 0.0]},
           "VolumeTotalWriteTime": {"timestamps": TS, "values": [0.6, 0.6, 0.6]},
           "VolumeQueueLength": {"timestamps": TS, "values": [0.5, 0.5, 0.5]}}
    root = {"VolumeWriteOps": {"timestamps": TS, "values": [60.0, 60.0, 60.0]}}
    return {"period_seconds": 300, "start": TS[0], "end": TS[-1], "instances": {
        "i-1": {"ec2_metrics": {"CPUUtilization": {"timestamps": TS, "values": [10.0, 20.0, 30.0]}},
                "ebs_metrics": {"vol-data": vol, "vol-root": root}}}}


def _snap(ts, ins, resident=500, conns=10, lag=None):
    s = {"ts": ts, "opcounters": {"insert": ins}, "connections": {"current": conns},
         "mem": {"resident_mb": resident, "virtual_mb": 2000},
         "wt_cache": {"bytes_currently_in_cache": 1e9, "maximum_bytes_configured": 2e9}}
    if lag is not None:
        s["repl_lag_sec"] = lag
    return s


def _ctx():
    return {"hostnames": ["192.0.2.1"], "private_ips": ["192.0.2.1"], "topology": "replicaset",
            "aws_region": "us-east-1", "version": (7, 0, 37),
            "instances": [{"instance_id": "i-1", "private_ip": "192.0.2.1", "instance_type": "t3.medium"}]}


def _sampling():
    snaps = [_snap(TS[1], 0), _snap(TS[2], 600)]
    return {"samples": 1, "delta_interval_seconds": 60, "snapshots": snaps,
            "members": [{"address": "192.0.2.1:27017", "type": "REPLICA_PRIMARY", "snapshots": snaps,
                         "deltas": [{"insert_per_sec": 10.0, "query_per_sec": 5.0, "update_per_sec": 0.0,
                                     "delete_per_sec": 0.0, "getmore_per_sec": 0.0}]}]}


def test_cloudwatch_iops_and_latency_math():
    ms = {m["name"]: [p["value"] for p in m["dataPoints"]]
          for m in mc._ec2_cloudwatch_measurements(_cw()["instances"]["i-1"], 300)}
    assert ms["DISK_PARTITION_IOPS_READ"] == [1.0, 2.0, 0.0]          # ops/min / 60
    assert ms["DISK_PARTITION_IOPS_WRITE"] == [11.0, 11.0, 11.0]      # both volumes summed
    assert ms["DISK_PARTITION_LATENCY_READ"] == [1.0, 2.0]             # skips the zero-op point
    assert ms["EC2_CPU_UTILIZATION"] == [10.0, 20.0, 30.0]
    assert "PROCESS_CPU_USER" not in ms                                 # never relabelled


def test_ec2_report_and_summary(tmp_path):
    (tmp_path / "c1").mkdir()
    (tmp_path / "c1" / "collstats.json").write_text(json.dumps({"wt_server_cache": {}, "collections": []}))
    am, procs, extra = mc._ec2_build_report_inputs(
        "c1", _ctx(), _cw(), _sampling(), {"data_size": 3e9, "storage_size": 1e9, "index_size": 2e8})
    extra["cache_sampled"] = {"gb": 1.0, "readings": 2, "span_seconds": 60, "cache_configured_gb": 2.0}
    assert procs == [{"hostname": "192.0.2.1", "port": 27017, "typeName": "REPLICA_PRIMARY"}]
    paths = mc.generate_report(am, procs, "PT5M", "P14D", tmp_path, 95, label="14d",
                               collstats={"c1": []}, report_extra=extra)
    rep = json.loads(Path(paths[0]).read_text())
    p = rep["processes"][0]
    assert rep["source"] == "ec2" and "metric_sources" in rep
    assert p["cpu"]["total_utilization_pct"]["max"] == 30.0
    assert p["cpu"]["user_normalized"] is None
    assert p["operations_per_sec"]["insert"]["avg"] == 10.0
    assert p["disk_io"]["write_iops"]["avg"] == 11.0
    assert p["storage"]["data_size_bytes"]["last"] == 3e9
    assert p["wiredtiger"]["cache_used_bytes"] is None               # sampled cache never posed as P95
    cs = json.loads((tmp_path / "c1" / "collstats.json").read_text())
    assert "cache_used_gb" not in cs
    md = Path(str(paths[0]).replace("sizing-report.json", "sizing-summary.md")).read_text()
    assert "Data sources (--source ec2)" in md
    assert "Total (EC2 CPUUtilization)" in md
    assert "Cache in use (max of 2 readings over 60s, not a P95)" in md


def test_atlas_report_has_no_ec2_fields(tmp_path):
    pts = [{"timestamp": "2026-10-19T00:00:00Z", "value": 1.0}]
    am = {"h:27017": {"cluster": "c1", "type": "REPLICA_PRIMARY",
                      "batches": {"cpu": [{"name": "PROCESS_CPU_USER", "dataPoints": pts}]}}}
    procs = [{"hostname": "h", "port": 27017, "typeName": "REPLICA_PRIMARY"}]
    rep = json.loads(Path(mc.generate_report(am, procs, "PT5M", "P14D", tmp_path)[0]).read_text())
    assert set(rep["processes"][0]["cpu"]) == {"user_normalized", "steal", "kernel", "iowait"}
    assert "source" not in rep and "metric_sources" not in rep


def test_unsampled_member_still_reported_with_cloudwatch():
    am, procs, extra = mc._ec2_build_report_inputs(
        "c1", _ctx(), _cw(), {"samples": 1, "snapshots": [], "members": [
            {"address": "192.0.2.9:27017", "type": "REPLICA_SECONDARY", "snapshots": [], "deltas": []}]},
        None, unsampled=[{"address": "192.0.2.1:27017", "type": "UNKNOWN", "error": "auth failed"}])
    assert {p["hostname"] for p in procs} == {"192.0.2.9", "192.0.2.1"}
    assert "cloudwatch" in am["192.0.2.1:27017"]["batches"]
    assert extra["unsampled_members"][0]["error"] == "auth failed"


def test_parse_compat_uri_output_and_operator_usage():
    out = ("No unsupported operators found.\n\nThe following 2 supported operators were found:\n"
           "  $add | executed 14 time(s)\n  $match | executed 3 time(s)\n\n")
    out2 = ("The following 1 unsupported operators were found:\n  $where | executed 2 time(s)\n\n"
            "The following 1 supported operators were found:\n  $add | executed 1 time(s)\n")
    p1, p2 = mc._parse_compat_uri_output(out), mc._parse_compat_uri_output(out2)
    assert p1 == {"unsupported": {}, "supported": {"$add": 14, "$match": 3}}
    assert p2["unsupported"] == {"$where": 2}
    u = mc._ec2_operator_usage({"n1": p1, "n2": p2}, "8.0.3", "c1", primary_host="n2")
    assert u["sampled_from"] == "n2" and u["summary"]["unsupported_executions"] == 2
    assert u["target_version"] == "8.0.3" and set(u["per_node"]) == {"n1", "n2"}
    atlas_keys = {"generated", "cluster", "target_version", "topology", "sampled_from", "source",
                  "classifier", "coverage_note", "summary", "unsupported", "supported", "unclassified"}
    assert atlas_keys <= set(u)


def test_ec2_log_lives_in_cluster_dir():
    src = Path(mc.__file__).read_text()
    assert "_init_log(Path(out) / args.cluster)" in src
