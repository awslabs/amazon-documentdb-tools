# Changelog

All notable changes to atlas_metrics.py are documented here.

## [2.3.1] - 2026-08-27

### Fixed - sizing summary crash on sharded clusters

`generate_report` aborted with `AttributeError: 'NoneType' object has no
attribute 'get'` when run against a sharded cluster (mongos topology), so no
`*-sizing-summary.md` was written and the `## Index Compatibility` section
(appended after the scan) never rendered. The JSON artifacts and the handoff
zip were still produced, and `_Step` isolation reported partial success
(exit 2), so the failure was easy to miss.

Root cause: `_generate_sizing_summary_md` reads node metrics with
`X.get(KEY, {})`, which returns the `{}` default only when KEY is absent. On a
sharded topology some processes (the mongos and config nodes) yield a metric
value that is present but `None` (from the internal `_s()` helper), so the `{}`
default never applied and the chained `.get()` on `None` raised. The first hit
was `ops.get("query", {}).get(pk, 0)`.

Fix: guard every unguarded-chained accessor with `(X.get(KEY) or {})` across the
operations, storage, sizing-hints, and connections sections, and coerce
`total_writes_pct` to a number. On replica-set input the guards are no-ops --
verified byte-identical rendering across four production replica-set reports.
Validated end to end against a sharded Atlas cluster (mongos): summary renders,
exit 0, Index Compatibility section appended.

## [2.3.0] - 2026-08-24

### Added - index compatibility scan via amazon-documentdb-tools index-tool

`--compat` now also runs an index compatibility scan. Previously the tool only
checked operator/API compatibility via `compat-tool/compat.py`; index
definitions were never checked against DocumentDB, so unsupported index types
were invisible in every run.

The scan is two delegated subprocess calls, in both the atlas and ec2 paths:

1. `index-tool --dump-indexes` - the tool decides what to dump and in what shape
2. `index-tool --show-issues` - the tool applies every compatibility rule

No classification logic is reimplemented here. A rule added upstream is picked
up with no change to this script. Reconstructing mongodump metadata from our own
derived fields was considered and rejected: it requires synthesizing markers
such as `textIndexVersion`, and a marker we invented cannot validate a rule
about that marker.

New artifacts per cluster:

- `index_compat.json` - flattened findings with scope (database / collection /
  index), issue type, and detail, plus the raw upstream report
- `index_metadata/` - the dump itself, in mongodump format. Durable ground
  truth: re-scannable later without re-contacting the customer
- `index_specs.json` - `list_indexes()` output verbatim, serialized with
  `bson.json_util` so BSON types inside `partialFilterExpression` survive as
  Extended JSON rather than being flattened by `str()`

New flag `--index-compat-from DIR` re-classifies a stored `index_metadata/`
directory with no cluster connection. Use it to re-evaluate past runs after
amazon-documentdb-tools adds a rule.

### Added - index findings rendered into the sizing summary

`*-sizing-summary.md` gains an `## Index Compatibility (DocumentDB 8.0)` section
listing the count by issue type and a per-finding table (scope, namespace, index,
issue, detail), capped at 50 rows with a pointer to the JSON beyond that.

This exists because the summary markdown is the artifact that actually gets
read. In one engagement `index_analysis.json` carried `{"_fts": "text"}` for
weeks and nobody saw it - collected, stored, and never surfaced.
Note the summary previously rendered no compatibility content at all, not even
operator compat; this is the first.

The section is appended after `_run_index_compat_scan` rather than emitted inside
`_generate_sizing_summary_md`, because the summary is written during
`generate_report`, which runs before the scan. Appending avoids reordering the
pipeline. It is idempotent (skips when the heading already exists) and no-ops
when no summary is present, which is the `--source ec2` case.

An incomplete-coverage verdict renders as a blockquote warning at the top of the
section, above the counts, naming the likely privilege cause and the roles that
fix it. A run missing a whole database reports 1 finding where the truth is 6,
so the count must never appear without that qualifier.

### Added - coverage guard on the index scan

MongoDB's `listDatabases` does not error for a user without cluster-wide
privilege. It returns only the databases that user is authorized on, and the
dump still exits 0. Measured 2026-08-24 on a lab replica set: a
`readWrite`-on-one-database user dumped 1 of 2 databases, missing 9 collections
carrying unsupported indexes, with no error and no non-zero exit.

Exit status therefore cannot establish coverage. `index_compat.json` now carries
a `coverage` block that cross-checks the dumped namespace set against the
namespaces `collStats` observed in the same run, and reports
`complete: true | false | null` with the count of anything missing. An
incomplete scan is logged at error level and flagged in the stdout banner. The
scan raises rather than writing a degraded report, because a reader cannot
distinguish "no unsupported indexes" from "we were unable to look".

### Fixed - compression sampling silently disabled outside the repo layout

`generate_sizing_csv` located `compression-review.py` at
`Path(__file__).parent.parent` only. For a script placed in a home directory -
the normal customer case - that resolves to `/home/amazon-documentdb-tools`,
which does not exist, so the run fell through to the per-collection collStats
ratio. That fallback underestimates DocumentDB Zstandard and oversizes the
target. The message was a single informational line in a long run.

Fixing the path exposed a second failure in the same code: `compression-review`
writes its CSV to the current working directory, so under SSM Send-Command,
cron, or a systemd unit it raised `PermissionError` and fell back again. The
call now runs inside a temporary directory, making the result independent of how
the tool was invoked.

Measured on a test Atlas replica set: before, `compression-review.py not found`;
after, `Compression ratios for 14 collections`.

### Fixed - compat scan clone target not writable

The atlas `--compat` path cloned amazon-documentdb-tools to
`Path(__file__).parent.parent`, which is `/home` for a script in a home
directory. Clone failed with `Permission denied` and the whole compat step was
lost (recorded by `_Step`, exit 2). It now uses the shared resolver, which
clones into the script's own directory.

### Changed - one resolver for all amazon-documentdb-tools lookups

`_resolve_documentdb_tool(subpath, auto_clone=True)` replaces four separate
locate-or-clone blocks (atlas compat, ec2 compat, compression-review, and the
new index scan). It searches `script_dir.parent`, `script_dir`, and the working
directory, clones into `script_dir` when absent, and raises with remediation
steps instead of returning `None`. Net effect: ~70 lines of duplication removed,
and a clone-path change is now a one-line edit rather than a four-site edit.

### Validated

End-to-end on both sources, against a seeded matrix of
unsupported shapes (hashed, compound text with a scalar prefix, pure text as a
negative control, wildcard `$**`, 2dsphere, capped collection, partial filter
carrying an ObjectId, key direction `2`):

- Atlas (MongoDB 7.0.40, replica set): 7 findings, coverage
  14/14 complete
- EC2 self-hosted `rs0` (MongoDB 7.0.37, replica set): 6 findings, coverage
  14/14 complete
- `--index-compat-from` against the stored dump reproduced the live result
  exactly, including the coverage verdict
- Pure text index correctly not flagged; compound text index flagged with the
  offending scalar key named

Note: the index-compatibility findings reflect whatever rules the installed
index-tool version applies. Recent index-tool versions flag text indexes that
carry scalar fields and numeric key directions other than `1` / `-1`; DocumentDB
rejects non-1/-1 key directions at build time (error 67).

## [2.2.0] - 2026-07-30

### Changed - `--compat` auto-clones amazon-documentdb-tools when missing

The `--compat` code path in versions ≤ 2.1.0 called `input()` to prompt the
operator before cloning `amazon-documentdb-tools`. In non-interactive shells
(nohup, SSM Send-Command, background jobs, CI), `input()` raised `EOFError`
immediately with an essentially empty string, producing a useless runtime.log
entry: `ERROR compat scan (8.0):  (0.1s)`.

Real-world impact: observed on a customer cluster in July 2026, on the first
run of the tool from a fresh runner. The operator had no diagnostic
breadcrumb - just an empty error and a 0.1s elapsed time.

**Change:** the tool now auto-clones the repo when `--compat` is set and the
repo isn't already present locally. Design rationale:

- `--compat` is the operator's affirmative request for the compat scan.
- `amazon-documentdb-tools` is the official AWS-labs repo that implements the
  scan, not a random third-party dependency.
- A separate consent gesture ("Clone this repo?") was noise that broke
  silently in the exact scenario where it was supposed to help.

If `git clone` itself fails (git not on PATH, network unreachable, disk full),
the tool now emits a clear `RuntimeError`:

```
Automatic clone of amazon-documentdb-tools failed (git exit N): <stderr>
The compat scan requires this repo. To recover:
  1) Verify `git` is on PATH: `git --version`
  2) Verify network reachability to github.com
  3) Or pre-clone manually before re-running:
       git clone https://github.com/awslabs/amazon-documentdb-tools.git '<path>'
```

### Removed - interactive `input()` prompt in the `--compat` path

Never released via the tag pipeline; short-lived `--auto-clone-tools` flag from
the internal 2.1.1 iteration is also removed since auto-clone is now the
default. If you had a pipeline invocation that included `--auto-clone-tools`,
drop the flag - behavior is identical.

### Backwards compatibility

- **Runs where the repo is already cloned:** zero behavior change.
- **Runs without `--compat`:** zero behavior change.
- **Runs where the repo is missing and `--compat` is set:** used to prompt (in
  a TTY) or crash silently (headless); now auto-clones and proceeds.
- **The rare user who wanted to answer "no" to the prompt:** don't pass
  `--compat`. Or omit the flag and run compat.py manually.

## [2.1.0] - 2026-07-28

### Added - `--source ec2`: consolidated ec2_metrics.py into atlas_metrics.py

Single tool now handles two source topologies. Backwards-compatible: existing
Atlas invocations require no changes (default `--source atlas`).

```bash
# Atlas (existing behavior, unchanged)
python3 atlas_metrics.py --all --compat --uri "mongodb+srv://..." --cluster <name>

# Self-managed MongoDB on EC2 (new)
python3 atlas_metrics.py --source ec2 --compat \
    --uri "mongodb://user:pass@node1:27017,node2:27017,node3:27017/?replicaSet=rs0" \
    --cluster <name> --aws-region us-east-1
```

**What `--source ec2` does:**

- 6-gate preflight (MongoDB reachable + clusterMonitor role + AWS creds valid +
  ec2:DescribeInstances + cloudwatch:GetMetricData + region consistency +
  compat version check)
- Discovers EC2 instances hosting each MongoDB member via
  `rs.status()` / `sh.status()` → private IPs → `ec2:DescribeInstances`
- Pulls 14 days of CloudWatch metrics for EC2 (CPU, network) + EBS (IOPS,
  latency, queue depth) across all discovered instances
- 60-second `serverStatus` delta sampling on the primary
- Full `collStats + $indexStats` via the shared mongos-aware collector (v2.0.2
  fix reused - sharded EC2 clusters use mongos-aggregated collstats correctly)
- Optional `--compat` scan via amazon-documentdb-tools (per-node --file mode)
- Auto-zip handoff bundle

**IAM requirements for `--source ec2`:**

```json
{
  "Effect": "Allow",
  "Action": [
    "cloudwatch:GetMetricData",
    "cloudwatch:GetMetricStatistics",
    "ec2:DescribeInstances",
    "ec2:DescribeVolumes",
    "ec2:DescribeNetworkInterfaces",
    "sts:GetCallerIdentity"
  ],
  "Resource": "*"
}
```

**Additional dependencies for `--source ec2`:** `boto3` (lazy-imported - Atlas
users don't need it installed).

**`--source onp` (on-premises):** stub added. Raises NotImplementedError with a
clear "planned for future release" message. Delivery gated on the first
customer engagement that needs it.

### Added - `--samples N` for `serverStatus` variance sampling

New optional argument (default `1`). Each sample is a 60-second `serverStatus`
delta. Use `--samples 3` or `--samples 5` for variance analysis on quiet
workloads. Same behavior as the same-named argument in the retired
`ec2_metrics.py`.

### Added - `--aws-region`

New optional argument. Only used when `--source ec2`. Auto-detected from
environment variables (`AWS_REGION`, `AWS_DEFAULT_REGION`), boto3 session
config, or EC2 IMDSv2 (in that order) if omitted. Required if none of those
sources can identify a region.

### Deprecated - standalone `ec2_metrics.py`

A previous standalone `ec2_metrics.py` script is superseded by
`atlas_metrics.py --source ec2`. Use `atlas_metrics.py --source ec2` going
forward.

Reason for consolidation: ~70% of the code was already shared logic (collstats,
compat scan, sizing summary MD, cost estimator CSV, auto-zip, compression
sampling, BSON key-type scan, prefix-subset index redundancy detection). The
v2.0.2 mongos-aggregated collstats fix and v2.0.3 step isolation had to be
double-maintained. Consolidation eliminates that maintenance debt. Verified
2026-07-27 across four topology axes: Atlas RS + Atlas sharded + EC2 RS +
EC2 sharded (3-node RS Harman-shape topology).

### Fixed - namespace pre-discovery for the shared collstats collector

`_collect_collstats_via_uri()` early-exits on empty `namespaces` argument.
The atlas source path populates namespaces from the Atlas API metric response;
the ec2 source path had no equivalent discovery step, so the shared collector
was called with `namespaces=[]` and silently produced no output - missing
`collstats.json` + `index_analysis.json` from `--source ec2` runs.

**Fix:** the ec2 pipeline now runs
`client.list_database_names()` + `list_collection_names()` (filtering
`admin`/`local`/`config` + `system.*`) before calling the shared collector.
Caught during step 9 of the 2026-07-27 validation run. No change to the atlas
source path.

### Version scheme note

Additive change (default source unchanged, existing invocations unchanged),
hence minor bump v2.0.3 → v2.1.0. The rename to `assessment_metrics.py` - when
it happens, on `--source onp` shipping or after ~30 days of stable v2.1.x with
zero critical bug reports - will be the v3.0.0 (or fresh v1.0.0 under the new
name) cut.

## [2.0.3] - 2026-07-22

### Fixed - sizing-summary MD generation crashed on collections with empty `cursor_stats`

Reproducer: a run against a customer's sharded Atlas cluster produced a valid
`sizing-report.json` but no `sizing-summary.md`. The runtime log ended with:

```
14:59:57 INFO START generate_report 14d
14:59:57 ERROR ERROR generate_report 14d: Cannot specify ',' with 's'. (0.0s)
```

Root cause: in the "Per-Collection Workload" render (section 10 of the summary
MD), a fallback string was passed into a numeric format specifier.

The renderer takes the detailed cursor-column branch when
`any(c.get("cursor_stats") for c in colls)` is true - i.e. as soon as one
collection has populated cursor stats. It then iterates ALL collections. For
collections whose `cursor_stats` came back as an empty dict `{}` from the
Atlas sampling stage, `cur.get('insert_calls', '-')` returns the literal `'-'`,
which the adjacent `:,` thousands-separator format spec rejects with
`ValueError: Cannot specify ',' with 's'`. This aborts MD generation for the
entire cluster, even though the JSON report is fully written.

On the failing customer run, 4 of 739 collections had `cursor_stats: {}` -
enough to trigger, small enough to slip past casual testing on smaller
clusters. Empty `cursor_stats` typically appears on idle collections where
the Atlas sampling window returned no cursor telemetry.

**Fix:** small local helper `_n(x)` inside the render function that emits
`f"{x:,}"` for numeric input and `"-"` otherwise. This preserves the
"unsampled" signal (`-`) rather than collapsing to a misleading `0`, which
would be visually indistinguishable from a real zero-ops row.

Only affected the per-collection workload table (2 lines of formatting). No
change to JSON output, no change to any collection or sizing logic. All
existing runs re-render cleanly.

### Added - pipeline-level step isolation (defense in depth)

Before this release, a crash in the sizing-summary MD render also blocked
every downstream artifact - cost-estimator CSV, compat-8.0.txt,
operator_usage.json, and the auto-zip handoff bundle. In the customer report
above the entire run took ~2 hours (14-day P5M collection on a 3-node Atlas
cluster) and the cursor_stats crash at the very last mile wiped out 4 of the
5 output artifacts. The raw sizing-report.json was produced (it's written
before the MD render), but the customer received an unusable output folder.

Root cause: `main()` was a serial sequence of `with _Timer(...)` blocks. The
`_Timer` context manager logged exceptions but re-raised them, so a crash in
any step propagated up and skipped the rest.

**Fix:** added a `_Step` context manager (same shape as `_Timer` but
catches exceptions, records the failure, and returns `True` from `__exit__`
so downstream steps still run). Converted the four downstream artifact
steps to `_Step`:

- `generate_report` (writes sizing-summary.md AND writes the raw
  sizing-report.json - the JSON is written first, so even if the MD render
  crashes the JSON survives)
- `generate_sizing_csv` (writes cost-estimator.csv)
- `compat scan (8.0)` (writes compat-8.0.txt and operator_usage.json)
- `auto-zip output` (bundles the whole cluster directory)

Kept `_Timer` semantics (hard-fail on error) for `collect_metrics`. That
step is foundational - if the underlying data collection fails there is
nothing to work with, and downstream artifact generation would be
meaningless.

When any step fails, `main()` now prints a partial-success summary listing
which steps failed and their errors, and exits with code 2 so shell scripts
and CI can distinguish "completed cleanly" (exit 0) from "completed with
missing artifacts" (exit 2). Foundational failures still raise unhandled
and exit 1 with a Python traceback.

Net effect: a future bug of the same class as the cursor_stats crash - or
any downstream failure Atlas can throw at us (subprocess timeout, disk full
during zip, transient MongoDB connection drop during compat sampling) -
degrades the run to a partial-success output instead of an empty output.
The customer gets every artifact the tool can still produce, and the
partial-success summary tells them exactly what to rerun.

## [2.0.2] - 2026-07-21

### Fixed - compat scan behavior on PrivateLink and replica-set clusters

Previous versions delegated compatibility scanning to `compat.py --uri` from
amazon-documentdb-tools. That upstream tool has two limitations that surfaced
during customer engagements:

1. **`ensureDirect()` forces `directConnection=True`** on the pymongo client and
   pins the target to `parsedUri['nodelist'][0]` - the first host returned by
   the SRV resolution.
2. **On replica sets, `nodelist[0]` is non-deterministic** across resolutions. A
   scan could land on a secondary and report "No unsupported operators found"
   even when the primary's `serverStatus.metrics.aggStageCounters` showed
   dozens of unsupported ones. We reproduced this: scanning a secondary
   returned 7 basic operators with 0 unsupported; scanning the primary of the
   same cluster returned 22 operators with 5 correctly flagged.
3. **On clusters with load-balanced endpoints** (some Atlas PrivateLink setups,
   any `loadBalanced=true` URI), the forced `directConnection=True` gets
   rejected at the NLB with `[Errno 9] Bad file descriptor` - a socket-level
   EBADF from mid-handshake rejection - and the scan returns zero coverage.

Neither behavior can be fixed by changing how atlas_metrics calls `compat.py`.
Whatever URI you pass, compat.py rewrites it into a direct-connect URI targeting
the first resolved host.

**New approach in v2.0.2** - sidestep the compat.py `--uri` mode entirely:

1. Connect via pymongo using the customer URI (standard driver behavior, works
   in PrivateLink, sharded, and replica set topologies).
2. Discover the primary explicitly via `hello()` and sample `serverStatus` from
   the primary using `ReadPreference.PRIMARY` (guarantees deterministic
   coverage on replica sets).
3. Extract `metrics.aggStageCounters` and `metrics.operatorCounters` - the
   same source data compat.py reads.
4. Filter out zero-count entries (MongoDB pre-registers all known operators
   with count=0; keeping them would produce false-positive unsupported flags).
5. Dump the executed operator names to a temp file and delegate classification
   to `compat.py --file` - the offline mode that has no directConnect
   requirement.
6. Merge compat.py's supported/unsupported classification with the real runtime
   execution counts from serverStatus.

Result: compat scan works reliably regardless of network topology, samples the
primary deterministically, and produces the same unsupported-operator list
compat.py's `--uri` mode would produce on a healthy setup - with additional
impact data (execution counts and percentages) that neither mode surfaced
before.

### Added

- **`operator_usage.json`** - new structured, machine-readable artifact
  generated alongside `compat-8.0.txt`. Contains a `summary` block with
  `unsupported_execution_pct`, `unsupported_executions`, `total_executions`,
  and per-operator execution counts. Downstream sizing analysts can now answer
  "how much of the workload is impacted by unsupported operators?" directly
  from JSON instead of parsing text.
- **Topology metadata in output.** Both `compat-8.0.txt` and
  `operator_usage.json` now include `topology` (`replica_set` or `sharded`)
  and `sampled_from` (primary hostname or "mongos"). On sharded clusters, the
  `coverage_note` field explicitly documents that operators evaluated only
  shard-side may not surface via mongos sampling, and points to
  `compat.py --directory` as a complementary offline scan.
- **Impact-tiered output section in `compat-8.0.txt`** - the runtime execution
  count table now groups operators as `UNSUPPORTED IN DOCUMENTDB` (with total
  execution count and workload percentage) and `SUPPORTED IN DOCUMENTDB`, so
  a customer-facing reader sees migration risk at a glance without reading
  the full compat.py output.

### Notes for upgrade

- Downstream consumers that parse `compat-8.0.txt` will still find the
  compat.py classification section unchanged; the new "Runtime execution
  counts" section is additive.
- Consumers of the compat output can migrate to `operator_usage.json` for
  cleaner programmatic access; the text file is retained for human review.
- Sharded clusters now emit a coverage-caveat header in `compat-8.0.txt`.
  Sizing reports for sharded clusters should mention that a complementary
  offline scan via `compat.py --directory` against source code is advisable.
- No change to any other output artifact (sizing, collstats, index_analysis,
  cost-estimator CSV, per-node measurement JSONs, zip bundle).

## [2.0.0] - 2026-07-02

### Breaking changes

- **`--uri` and `--cluster` are now required.** Previously optional; the tool would try (and often fail) to auto-resolve. Running without both now returns a clear argparse error.
- **`--all` mode window changed from 30 days to 14 days.** Atlas retention at PT5M granularity is 14 days; days 15-30 were silently coarser rollups presented as PT5M buckets, biasing P95 estimates low. The 14-day window returns accurate data.
- **Output filename pattern changed** from `<cluster>-30d-sizing-summary.md` to `<cluster>-14d-sizing-summary.md` to match the accurate window.
- **`zstandard` is now a required dependency.** Previously optional with a 5:1 fallback ratio; the fallback was overly optimistic (real Atlas Snappy → DocDB Zstd is ~3-4:1). Missing zstandard now falls back to 3.5:1 with a prominent warning.

### Added

- **Mid-run authentication failure detection.** All 6 mid-run pymongo client opens (post-preflight) wrapped with `_check_auth_error()`. If credentials rotate during the ~30-min collection (Vault, AWS Secrets Manager auto-rotation, manual password change), the tool detects it, prints actionable guidance ("Use a static atlasAdmin user for the run window, or ensure TTL > 45 min"), preserves partial output, and exits with code 2. Prevents cryptic mid-run crashes with no context.
- **Per-cluster output zip.** At end of run, packages the `<cluster>/` output directory into `<output>/<cluster>.zip` (typical size 200-500 KB, ~10x compressed). One-file handoff for customers - email/upload the zip directly instead of tarring manually.
- **Preflight checks (5 gates, ~10s).** Validate credentials, cluster existence, DB user role (`serverStatus` probe), URI reachability, topology match, and MongoDB version (if `--compat`) BEFORE the 30-min collection. Prevents wasted runs.
- **Paused cluster detection.** Fails fast if the target cluster's `paused` field is true, with resume instructions.
- **DB user permission check.** Preflight probes `serverStatus` and reports the exact role gap (`atlasAdmin` or `clusterMonitor` + `readAnyDatabase`).
- **Typo suggestions.** Uses `difflib.get_close_matches` (cutoff=0.75) to suggest cluster names when `--cluster` is misspelled.
- **Period-vs-retention warning.** Warns when `--period` exceeds granularity retention (prevents the "silent rollup" bias).
- **Sharded cluster support via mongos-aggregated `collStats`.** Works universally from any network (public URI, PrivateLink, VPC Peering). No special customer network configuration required.
- **`__version__` constant** for programmatic version detection.

### Fixed

- Retention constants for `PT5M` corrected from "~30 days" to "~14 days" (was a documentation lie).
- Direct-shard code path removed - was silently broken because MongoDB rejects `collStats` on sharded collections when connected directly to a shard, regardless of network path.
- Python 3.9 f-string+backslash compatibility (pulled regex out of f-string context).

### Removed

- `--all` mode's two-window suggestion (was PT5M/P30D + PT1M/P2D). Now a single accurate window (PT5M/P14D).
- `lz4` dependency (was in requirements but never imported).

### Notes for upgrade

- Callers with scripted invocations that omitted `--uri` or `--cluster` will now fail. Update scripts to pass both explicitly.
- Output directory structure is unchanged except for the filename pattern (`14d` instead of `30d`).
- The Cost Estimator CSV format is backward compatible.

## [1.x] - before 2026-07-02

Historical versions. Not documented in this changelog.
