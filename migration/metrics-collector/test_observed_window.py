"""Tests for the observed-window reporting (issue #209).

Run from this directory:  python -m pytest test_observed_window.py
"""
import json
from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import metrics_collector as mc  # noqa: E402


def _series(count, step_minutes=5):
    start = datetime(2026, 10, 19, tzinfo=timezone.utc)
    points = [{"timestamp": (start + timedelta(minutes=i * step_minutes)).strftime("%Y-%m-%dT%H:%M:%SZ"),
               "value": float(i)} for i in range(count)]
    return {"name": "SYSTEM_NORMALIZED_CPU_USER", "dataPoints": points}


def _cluster_metrics(days):
    count = int(days * 24 * 12) + 1
    return {"host:27017": {"cluster": "c1", "type": "REPLICA_PRIMARY",
                           "batches": {"cpu": [_series(count)]}}}


def test_observed_window_reports_actual_span():
    ow = mc._observed_window(_cluster_metrics(2))
    assert ow["days"] == 2.0
    assert ow["start"] == "2026-10-19T00:00:00Z"
    assert ow["end"] == "2026-10-21T00:00:00Z"


def test_observed_window_none_without_datapoints():
    assert mc._observed_window({"h:1": {"batches": {"cpu": [{"name": "x", "dataPoints": []}]}}}) is None


def test_report_and_summary_show_observed_period(tmp_path):
    procs = [{"hostname": "host", "port": 27017, "typeName": "REPLICA_PRIMARY"}]
    paths = mc.generate_report(_cluster_metrics(2), procs, "PT5M", "P14D", tmp_path)
    report = json.loads(Path(paths[0]).read_text())
    assert report["period"] == "P14D"
    assert report["observed_window"]["days"] == 2.0

    md = tmp_path / "summary.md"
    mc._generate_sizing_summary_md(report, md, 95)
    text = md.read_text()
    assert "Period requested:** P14D" in text
    assert "Period observed:** 2.0 days" in text
    assert "less history than requested" in text
