"""Code Review Engine — scans source code for DocumentDB client best practices."""
import os
import re
import logging
import threading
import time

logger = logging.getLogger(__name__)

# Supported extensions
SOURCE_EXTENSIONS = {
    ".js", ".ts", ".mjs", ".cjs", ".jsx", ".tsx",
    ".java", ".py", ".go", ".cs", ".rb", ".php", ".r",
}
CONFIG_FILES = {
    "docker-compose.yml", "docker-compose.yaml",
    ".env", ".env.example", ".env.docker.example",
    "serverless.yml", "template.yaml",
    "package.json", "pom.xml", "build.gradle", "Gemfile", "composer.json",
    "requirements.txt", "go.mod", "Cargo.toml",
}
# Also match config files by extension in specific directories
CONFIG_EXTENSIONS = {".yml", ".yaml", ".json", ".toml", ".env"}
CDK_DIRS = {"cdk", "infra", "infrastructure"}
EXCLUDE_DIRS = {"node_modules", "vendor", ".git", "dist", "build", "__pycache__", ".next", "coverage", ".venv", "venv", "env"}

# Review state (thread-safe)
_review_state = {
    "running": False, "done": False, "error": None,
    "progress": "", "results": None, "target_dir": "", "output_dir": "",
}
_review_lock = threading.Lock()

# ── Path security ─────────────────────────────────────────────────────────────
# Hardcoded denylist: system/sensitive directories that must NEVER be scanned.
# This is not configurable — it's a safety invariant.
_DENIED_PATHS = [
    "/etc", "/proc", "/sys", "/dev", "/boot", "/root",
    "/var/log", "/var/run", "/var/spool",
    "/usr/bin", "/usr/sbin", "/usr/lib", "/usr/lib64",
    "/sbin", "/bin", "/lib", "/lib64",
]
_DENIED_EXACT = {"/", "/tmp"}  # /tmp/code-review is allowed via allowlist, but bare /tmp is not

_APP_DIR = os.path.dirname(os.path.abspath(__file__))


def validate_target_dir(path):
    """Validate a target directory for code review scanning.

    Returns None if OK, or an error string if rejected.
    Checks: denylist (hardcoded) → allowlist (from prism_config.yaml) → self-scan prevention.
    """
    if not path or not path.strip():
        return "No directory provided."

    resolved = os.path.realpath(path.strip())

    # Deny exact matches
    if resolved in _DENIED_EXACT:
        return f"Cannot scan system directory: {resolved}"

    # Deny prefix matches
    for denied in _DENIED_PATHS:
        if resolved == denied or resolved.startswith(denied + "/"):
            return f"Cannot scan system/sensitive directory: {resolved}"

    # Prevent scanning the Prism app directory itself
    if resolved == _APP_DIR or resolved.startswith(_APP_DIR + "/"):
        return f"Cannot scan the Prism application directory itself."

    # Check allowlist from config
    try:
        from prism_cfg import get_config
        cfg = get_config().get("code_review", {})
        allowed = cfg.get("allowed_directories", [])
    except Exception:
        allowed = []

    if allowed:
        allowed_resolved = [os.path.realpath(d) for d in allowed if d]
        if not any(resolved == a or resolved.startswith(a + "/")
                   for a in allowed_resolved):
            return (f"Directory not in allowed list. "
                    f"Allowed: {', '.join(allowed)}. "
                    f"Edit prism_config.yaml → code_review.allowed_directories to add paths.")

    return None  # OK


def get_review_state():
    with _review_lock:
        return dict(_review_state)


def start_code_review(target_dir, output_dir):
    """Start code review in background thread."""
    with _review_lock:
        if _review_state["running"]:
            return False
        _review_state.update(
            running=True, done=False, error=None, progress="Discovering files...",
            results=None, target_dir=target_dir, output_dir=output_dir,
        )
    threading.Thread(target=_run_review, args=(target_dir, output_dir), daemon=True).start()
    return True


def _update_progress(msg):
    with _review_lock:
        _review_state["progress"] = msg


def _run_review(target_dir, output_dir):
    """Main review logic."""
    try:
        # Step 1: Validate path security
        rejection = validate_target_dir(target_dir)
        if rejection:
            raise ValueError(rejection)
        if not os.path.isdir(target_dir):
            raise ValueError(f"Target directory does not exist: {target_dir}")
        if not os.path.isdir(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        # Step 2: Discover files
        _update_progress("Scanning for source files...")
        source_files, config_files = _discover_files(target_dir)
        total_files = len(source_files) + len(config_files)
        logger.info("Code review: discovered %d source files, %d config files in %s",
                    len(source_files), len(config_files), target_dir)
        _update_progress(f"Found {len(source_files)} source files, {len(config_files)} config files ({total_files} total)")
        time.sleep(0.5)

        # Step 3: Scan for patterns
        _update_progress("Analyzing code patterns...")
        patterns = _scan_patterns(target_dir, source_files + config_files)
        _update_progress(f"Found patterns in {patterns['files_with_mongo']} files")
        time.sleep(0.5)

        # Step 4: Evaluate checklist
        _update_progress("Evaluating 54 checklist items...")
        findings = _evaluate_checklist(patterns)

        # Step 5: Generate report
        _update_progress("Generating report...")
        report_name = os.path.basename(target_dir.rstrip("/\\")) + "_Code_Review.md"
        report_path = os.path.join(output_dir, report_name)
        _write_report(report_path, target_dir, findings, patterns)

        # Build summary
        summary = _build_summary(findings)
        summary["report_path"] = report_path
        summary["source_files"] = len(source_files)
        summary["config_files"] = len(config_files)
        summary["files_with_mongo"] = patterns["files_with_mongo"]
        summary["languages"] = patterns.get("languages", [])
        summary["has_lambda"] = patterns.get("has_lambda", False)

        with _review_lock:
            _review_state.update(running=False, done=True, results=summary,
                                 progress="Complete")

    except Exception as e:
        logger.error("Code review failed: %s", e, exc_info=True)
        with _review_lock:
            _review_state.update(running=False, done=True, error=str(e), progress="Failed")


def _discover_files(target_dir):
    """Recursively find source and config files. Respects max_files and max_file_size from config."""
    # Load limits from config
    try:
        from prism_cfg import get_config
        cfg = get_config().get("code_review", {})
    except Exception:
        cfg = {}
    max_files = cfg.get("max_files", 5000)
    max_file_size = cfg.get("max_file_size_bytes", 1_048_576)

    source_files = []
    config_files = []
    total_found = 0

    for root, dirs, files in os.walk(target_dir):
        dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]
        rel_root = os.path.relpath(root, target_dir)
        # Check if we're inside a CDK/infra directory
        root_parts = set(rel_root.replace("\\", "/").split("/"))
        in_cdk = bool(root_parts & CDK_DIRS)

        for f in files:
            if total_found >= max_files:
                logger.warning("Code review: hit max_files limit (%d), stopping discovery.", max_files)
                return source_files, config_files

            full_path = os.path.join(root, f)
            # Skip files exceeding size limit
            try:
                if os.path.getsize(full_path) > max_file_size:
                    continue
            except OSError:
                continue

            rel_path = os.path.relpath(full_path, target_dir)
            ext = os.path.splitext(f)[1].lower()

            if ext in SOURCE_EXTENSIONS:
                source_files.append(rel_path)
                total_found += 1
                # CDK .ts files also count as config
                if in_cdk and ext in (".ts", ".js", ".py"):
                    config_files.append(rel_path)
            elif f in CONFIG_FILES or f.startswith(".env"):
                config_files.append(rel_path)
                total_found += 1
            elif in_cdk and ext in CONFIG_EXTENSIONS:
                config_files.append(rel_path)
                total_found += 1

    return source_files, config_files


def _scan_patterns(target_dir, files):
    """Scan files for MongoDB/DocumentDB patterns."""
    patterns = {
        "connection_strings": [],
        "pool_config": [],
        "timeout_config": [],
        "retry_logic": [],
        "crud_ops": [],
        "cursor_usage": [],
        "lambda_handlers": [],
        "tls_config": [],
        "credentials": [],
        "monitoring": [],
        "indexes": [],
        "files_with_mongo": 0,
        "languages": set(),
        "has_lambda": False,
    }

    for rel_path in files:
        full_path = os.path.join(target_dir, rel_path)
        try:
            with open(full_path, "r", encoding="utf-8", errors="ignore") as fh:
                content = fh.read()
        except (IOError, OSError):
            continue

        ext = os.path.splitext(rel_path)[1].lower()
        if ext in (".js", ".ts", ".mjs", ".cjs", ".jsx", ".tsx"):
            patterns["languages"].add("node")
        elif ext == ".java":
            patterns["languages"].add("java")
        elif ext == ".py":
            patterns["languages"].add("python")
        elif ext == ".go":
            patterns["languages"].add("go")
        elif ext == ".cs":
            patterns["languages"].add("csharp")
        elif ext == ".rb":
            patterns["languages"].add("ruby")

        has_mongo = False

        # Connection strings
        if re.search(r'mongodb(\+srv)?://', content):
            has_mongo = True
            patterns["connection_strings"].append({
                "file": rel_path,
                "has_replica_set": "replicaSet=rs0" in content,
                "has_read_pref": "readPreference=" in content or "readPreference" in content,
                "has_tls": "tls=true" in content or "ssl=true" in content or "tls:" in content,
                "has_retry_writes_false": "retryWrites=false" in content,
                "uses_cluster_endpoint": bool(re.search(r'cluster-[\w]+\.[\w-]+\.docdb\.amazonaws\.com', content)),
            })

        # MongoClient / mongoose
        if re.search(r'MongoClient|mongoose\.connect|mongo\.Connect|MongoClients\.create', content):
            has_mongo = True

        # Pool config
        if re.search(r'maxPoolSize|minPoolSize|max_pool_size|MaxPoolSize', content):
            patterns["pool_config"].append(rel_path)

        # Timeouts
        if re.search(r'connectTimeoutMS|serverSelectionTimeoutMS|socketTimeoutMS|connect_timeout|server_selection_timeout', content):
            patterns["timeout_config"].append(rel_path)

        # Retry logic
        if re.search(r'retry|backoff|exponential.*back|MongoNetworkError|MongoNotPrimary|ConnectionFailure|NotPrimaryError', content, re.IGNORECASE):
            patterns["retry_logic"].append(rel_path)

        # CRUD with _id
        if re.search(r'\$set|\$inc|\$mul|\$push|insertOne|insertMany|insert_one|insert_many|InsertOne', content):
            has_mongo = True
            patterns["crud_ops"].append({
                "file": rel_path,
                "explicit_id": bool(re.search(r'_id\s*[=:]', content)),
                "uses_set": "$set" in content,
                "uses_inc": "$inc" in content,
                "uses_mul": "$mul" in content,
            })

        # Cursors
        if re.search(r'\.find\(|\.cursor\(|\.close\(|batchSize|batch_size', content):
            patterns["cursor_usage"].append({
                "file": rel_path,
                "has_close": bool(re.search(r'\.close\(|finally|defer\s+.*close', content)),
                "has_batch_size": "batchSize" in content or "batch_size" in content,
            })

        # Lambda
        if re.search(r'exports\.handler|module\.exports.*handler|lambda_handler|def handler|func HandleRequest', content):
            patterns["has_lambda"] = True
            patterns["lambda_handlers"].append(rel_path)

        # TLS
        if re.search(r'tls.*true|ssl.*true|tlsCAFile|global-bundle\.pem|rds-combined-ca', content, re.IGNORECASE):
            patterns["tls_config"].append(rel_path)

        # Hardcoded credentials
        if re.search(r'password\s*[=:]\s*["\'][^"\']+["\']|MONGO.*PASSWORD|DB_PASS', content):
            patterns["credentials"].append(rel_path)

        # Monitoring
        if re.search(r'commandStarted|commandSucceeded|commandFailed|SDAM|poolCreated|connectionCreated', content):
            patterns["monitoring"].append(rel_path)

        # Indexes
        if re.search(r'createIndex|ensureIndex|create_index|@Index|index\s*\(', content):
            patterns["indexes"].append(rel_path)

        if has_mongo:
            patterns["files_with_mongo"] += 1

    patterns["languages"] = list(patterns["languages"])
    patterns["_target"] = target_dir
    return patterns


def _evaluate_checklist(patterns):
    """Evaluate all 54 checklist items based on discovered patterns."""
    findings = {}
    conn = patterns["connection_strings"]
    has_conn = len(conn) > 0

    # 1. Connection Configuration
    findings["1.1"] = _eval_conn(conn, "uses_cluster_endpoint", "Uses cluster endpoint")
    findings["1.2"] = _eval_conn(conn, "has_replica_set", "replicaSet=rs0")
    findings["1.3"] = _eval_conn(conn, "has_read_pref", "readPreference=secondaryPreferred")
    findings["1.4"] = _eval_conn(conn, "has_tls", "tls=true")
    findings["1.5"] = _eval_conn(conn, "has_retry_writes_false", "retryWrites=false")
    findings["1.6"] = {"status": "⚠️" if has_conn else "☐ N/A",
                       "finding": "MongoClient singleton pattern needs manual verification"}
    findings["1.7"] = {
        "status": "❌" if patterns["credentials"] else "✅",
        "finding": f"Credentials found in: {', '.join(patterns['credentials'][:3])}" if patterns["credentials"]
                   else "No hardcoded credentials detected",
        "files": patterns["credentials"][:3],
    }

    # 2. Connection Pooling
    has_pool = len(patterns["pool_config"]) > 0
    findings["2.1"] = {"status": "✅" if has_pool else "❌",
                       "finding": f"Pool config in: {', '.join(patterns['pool_config'][:2])}" if has_pool
                                  else "No maxPoolSize configured"}
    findings["2.2"] = {"status": "☐ N/A" if "node" not in patterns["languages"] else
                       ("✅" if has_pool else "❌"),
                       "finding": "Node.js maxPoolSize " + ("configured" if has_pool else "not overridden")}
    findings["2.3"] = {"status": "⚠️" if has_pool else "❌",
                       "finding": "waitQueueTimeoutMS needs verification" if has_pool
                                  else "waitQueueTimeoutMS not configured"}
    findings["2.4"] = {"status": "⚠️", "finding": "maxIdleTimeMS not explicitly verified"}
    findings["2.5"] = {"status": "⚠️", "finding": "Pool sizing rationale not documented in code"}

    # 3. Timeout Settings
    has_timeout = len(patterns["timeout_config"]) > 0
    findings["3.1"] = {"status": "✅" if has_timeout else "❌",
                       "finding": f"Timeout config in: {', '.join(patterns['timeout_config'][:2])}" if has_timeout
                                  else "No connectTimeoutMS configured"}
    findings["3.2"] = {"status": "⚠️" if has_timeout else "❌",
                       "finding": "serverSelectionTimeoutMS " + ("found" if has_timeout else "not configured")}
    findings["3.3"] = {"status": "✅", "finding": "socketTimeoutMS left at default (not set)"}
    findings["3.4"] = {"status": "☐ N/A" if "ruby" not in patterns["languages"] else "⚠️",
                       "finding": "Ruby socketTimeoutMS" + (" needs review" if "ruby" in patterns["languages"] else " — not a Ruby project")}

    # 4. Failover & HA
    findings["4.1"] = {"status": findings["1.1"]["status"],
                       "finding": "See 1.1/1.2 — cluster endpoint + replica set"}
    findings["4.2"] = {"status": "✅" if patterns["retry_logic"] else "❌",
                       "finding": f"Retry logic in: {', '.join(patterns['retry_logic'][:2])}" if patterns["retry_logic"]
                                  else "No exponential backoff/retry logic found"}
    findings["4.3"] = {"status": "⚠️", "finding": "Failover testing not verifiable from source code"}

    # 5. Exception Handling
    has_retry = len(patterns["retry_logic"]) > 0
    findings["5.1"] = {"status": "⚠️" if has_retry else "❌",
                       "finding": "Retry logic exists but transient/persistent separation needs verification" if has_retry
                                  else "No error classification found"}
    findings["5.2"] = {"status": "⚠️" if has_retry else "❌",
                       "finding": "Retryable exceptions " + ("partially identified" if has_retry else "not identified")}
    findings["5.3"] = {"status": "⚠️" if has_retry else "❌",
                       "finding": "Write retry with backoff " + ("needs verification" if has_retry else "not implemented")}
    findings["5.4"] = {"status": "⚠️" if has_retry else "❌",
                       "finding": "Read retry " + ("needs verification" if has_retry else "not implemented")}
    findings["5.5"] = {"status": "⚠️", "finding": "Persistent error handling needs manual review"}
    findings["5.6"] = {"status": "⚠️" if has_retry else "❌",
                       "finding": "Retry scope " + ("needs verification" if has_retry else "not implemented")}

    # 6. Idempotency
    crud = patterns["crud_ops"]
    has_explicit_id = any(c["explicit_id"] for c in crud)
    findings["6.1"] = {"status": "✅" if has_explicit_id else ("❌" if crud else "☐ N/A"),
                       "finding": "Explicit _id " + ("found" if has_explicit_id else "not set on inserts") if crud
                                  else "No insert operations found"}
    findings["6.2"] = {"status": "⚠️" if crud and not has_explicit_id else ("✅" if has_explicit_id else "☐ N/A"),
                       "finding": "Auto-generated _id risk on retryable paths" if crud and not has_explicit_id
                                  else "OK" if has_explicit_id else "No inserts"}
    findings["6.3"] = {"status": "✅" if any(c["uses_set"] for c in crud) else ("❌" if crud else "☐ N/A"),
                       "finding": "$set usage " + ("found" if any(c["uses_set"] for c in crud) else "not found") if crud
                                  else "No update operations"}
    has_inc = any(c["uses_inc"] or c["uses_mul"] for c in crud)
    findings["6.4"] = {"status": "⚠️" if has_inc else "☐ N/A",
                       "finding": "$inc/$mul found — two-phase pattern needs verification" if has_inc
                                  else "No $inc/$mul operations"}
    findings["6.5"] = {"status": "⚠️" if crud else "☐ N/A",
                       "finding": "Delete predicates need manual verification" if crud else "No delete operations"}

    # 7. Cursor Management
    cursors = patterns["cursor_usage"]
    findings["7.1"] = {"status": "✅" if cursors and all(c["has_close"] for c in cursors) else
                       ("⚠️" if cursors else "☐ N/A"),
                       "finding": "Cursors " + ("properly closed" if cursors and all(c["has_close"] for c in cursors)
                                  else "some may not be explicitly closed" if cursors else "no cursor usage found")}
    findings["7.2"] = {"status": "✅" if any(c["has_batch_size"] for c in cursors) else
                       ("❌" if cursors else "☐ N/A"),
                       "finding": "batchSize " + ("configured" if any(c["has_batch_size"] for c in cursors)
                                  else "not configured") if cursors else "No cursor usage"}
    findings["7.3"] = {"status": "⚠️", "finding": "Long-running queries need manual verification"}
    findings["7.4"] = {"status": "❌" if not patterns["monitoring"] else "⚠️",
                       "finding": "Cursor timeout monitoring " + ("via driver events" if patterns["monitoring"] else "not found")}

    # 8. Lambda Integration
    if not patterns["has_lambda"]:
        for i in range(1, 7):
            findings[f"8.{i}"] = {"status": "☐ N/A", "finding": "No Lambda handlers detected"}
    else:
        findings["8.1"] = {"status": "⚠️", "finding": "Lambda MongoClient placement needs verification"}
        findings["8.2"] = {"status": "⚠️", "finding": "Lambda maxPoolSize needs verification"}
        findings["8.3"] = {"status": "☐ N/A", "finding": "VPC config not verifiable from source"}
        findings["8.4"] = {"status": "☐ N/A", "finding": "IAM role not verifiable from source"}
        findings["8.5"] = {"status": "☐ N/A", "finding": "Security group not verifiable from source"}
        findings["8.6"] = {"status": "☐ N/A", "finding": "CloudWatch alarm not verifiable from source"}

    # 9. Security
    findings["9.1"] = {"status": "✅" if patterns["tls_config"] else "❌",
                       "finding": f"TLS config in: {', '.join(patterns['tls_config'][:2])}" if patterns["tls_config"]
                                  else "No TLS configuration found"}
    findings["9.2"] = {"status": "✅" if any("global-bundle" in f or "rds-combined" in f for f in patterns["tls_config"]) else "❌",
                       "finding": "CA bundle " + ("referenced" if patterns["tls_config"] else "not found")}
    findings["9.3"] = {"status": "⚠️", "finding": "IAM auth needs manual verification"}
    findings["9.4"] = {"status": "⚠️", "finding": "Secrets Manager integration needs verification"}
    findings["9.5"] = findings["1.7"]

    # 10. Monitoring
    findings["10.1"] = {"status": "✅" if patterns["monitoring"] else "❌",
                        "finding": f"Monitoring in: {', '.join(patterns['monitoring'][:2])}" if patterns["monitoring"]
                                   else "No DB monitoring/logging found"}

    # 11. Query & Cost
    findings["11.1"] = {"status": "⚠️", "finding": "COLLSCAN detection requires explain() — manual review needed"}
    findings["11.2"] = {"status": "✅" if patterns["indexes"] else "⚠️",
                        "finding": "Index definitions " + ("found" if patterns["indexes"] else "not found in source")}
    findings["11.3"] = {"status": "⚠️", "finding": "Unused index detection requires runtime analysis"}
    findings["11.4"] = {"status": "⚠️", "finding": "Index count per collection requires runtime analysis"}
    findings["11.5"] = {"status": "⚠️" if patterns["indexes"] else "❌",
                        "finding": "ESR rule compliance needs manual review" if patterns["indexes"]
                                   else "No compound indexes defined"}
    findings["11.6"] = {"status": "⚠️", "finding": "Field mutability separation needs manual review"}
    findings["11.7"] = {"status": "⚠️", "finding": "Binary object storage needs manual review"}
    findings["11.8"] = {"status": "⚠️", "finding": "Time-series/TTL strategy needs manual review"}

    return findings


def _eval_conn(conn_list, key, label):
    """Evaluate a connection string property."""
    if not conn_list:
        return {"status": "❌", "finding": f"No connection string found — {label} not configured"}
    if all(c[key] for c in conn_list):
        return {"status": "✅", "finding": f"{label} — configured in all connection strings"}
    if any(c[key] for c in conn_list):
        return {"status": "⚠️", "finding": f"{label} — configured in some but not all connection strings"}
    return {"status": "❌", "finding": f"{label} — not configured"}


def _build_summary(findings):
    """Build compliance summary from findings."""
    counts = {"✅": 0, "⚠️": 0, "❌": 0, "☐ N/A": 0}
    for f in findings.values():
        counts[f["status"]] = counts.get(f["status"], 0) + 1
    applicable = 54 - counts["☐ N/A"]
    compliance_pct = round((counts["✅"] / applicable * 100), 1) if applicable > 0 else 0
    return {
        "total": 54,
        "compliant": counts["✅"],
        "warning": counts["⚠️"],
        "non_compliant": counts["❌"],
        "na": counts["☐ N/A"],
        "applicable": applicable,
        "compliance_pct": compliance_pct,
        "findings": findings,
    }


def _write_report(report_path, target_dir, findings, patterns):
    """Write the markdown report."""
    project_name = os.path.basename(target_dir.rstrip("/\\"))
    summary = _build_summary(findings)

    lines = [
        f"# {project_name} — DocumentDB Client Code Best Practices Review\n",
        f"*Reviewed against: DocumentDB Client Code Best Practices Checklist (54 items)*\n",
        f"*Languages: {', '.join(patterns['languages']) if patterns['languages'] else 'N/A'}*\n",
        f"*Source files: {patterns['files_with_mongo']} with MongoDB/DocumentDB patterns*\n",
        "\n---\n",
    ]

    categories = [
        ("1", "Connection Configuration", ["1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7"]),
        ("2", "Connection Pooling", ["2.1", "2.2", "2.3", "2.4", "2.5"]),
        ("3", "Timeout Settings", ["3.1", "3.2", "3.3", "3.4"]),
        ("4", "Failover & HA", ["4.1", "4.2", "4.3"]),
        ("5", "Exception Handling & Retry Logic", ["5.1", "5.2", "5.3", "5.4", "5.5", "5.6"]),
        ("6", "Idempotency Patterns", ["6.1", "6.2", "6.3", "6.4", "6.5"]),
        ("7", "Cursor Management", ["7.1", "7.2", "7.3", "7.4"]),
        ("8", "Lambda Integration", ["8.1", "8.2", "8.3", "8.4", "8.5", "8.6"]),
        ("9", "Security", ["9.1", "9.2", "9.3", "9.4", "9.5"]),
        ("10", "Monitoring & Observability", ["10.1"]),
        ("11", "Query & Cost Optimization", ["11.1", "11.2", "11.3", "11.4", "11.5", "11.6", "11.7", "11.8"]),
    ]

    for cat_num, cat_name, items in categories:
        lines.append(f"\n## {cat_num}. {cat_name}\n\n")
        lines.append("| # | Status | Finding |\n")
        lines.append("|---|--------|--------|\n")
        for item_id in items:
            f = findings.get(item_id, {"status": "☐ N/A", "finding": "Not evaluated"})
            lines.append(f"| {item_id} | {f['status']} | {f['finding']} |\n")
        lines.append("\n---\n")

    # Summary table
    lines.append("\n## Summary\n\n")
    lines.append(f"| Metric | Value |\n")
    lines.append(f"|--------|-------|\n")
    lines.append(f"| Total Items | {summary['total']} |\n")
    lines.append(f"| ✅ Compliant | {summary['compliant']} |\n")
    lines.append(f"| ⚠️ Needs Review | {summary['warning']} |\n")
    lines.append(f"| ❌ Non-Compliant | {summary['non_compliant']} |\n")
    lines.append(f"| ☐ N/A | {summary['na']} |\n")
    lines.append(f"| Applicable | {summary['applicable']} |\n")
    lines.append(f"| **Compliance** | **{summary['compliance_pct']}%** |\n")

    with open(report_path, "w", encoding="utf-8") as fh:
        fh.writelines(lines)
