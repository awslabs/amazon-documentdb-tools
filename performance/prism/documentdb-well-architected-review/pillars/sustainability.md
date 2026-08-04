# Sustainability Pillar

<!-- Check IDs must stay in sync with: wa_checks/sustainability.py -->
<!-- IDs are stable — use exactly what @register_check defines -->

## Pillar Definition

Evaluates Graviton processor adoption and compression enablement to reduce energy consumption and resource waste.

## Checks

| check_id | check_name | description | aws_api | pass_condition | warn_condition | fail_condition | remediation_cli | severity | per_instance |
|----------|-----------|-------------|---------|----------------|----------------|----------------|-----------------|----------|--------------|
| SUST1 | Graviton processor | Instance uses Graviton processor family (better energy efficiency + price-performance) | Derived from DBInstanceClass — extract family prefix after "db." | Family in GRAVITON_FAMILIES (r6g, r7g, r8g, t4g, r6gd) | Non-Graviton family (r5, t3, etc.) | — | `aws docdb modify-db-instance --db-instance-identifier {id} --db-instance-class db.r6g.{size}` | LOW | true |
| SUST2 | Compression enabled | All collections have compression enabled (reduces storage I/O and disk usage) | Derived from database analysis — per-collection compression.enable flag | All collections have compression enabled | 1+ collections without compression | — | Enable at cluster level: `aws docdb modify-db-cluster-parameter-group --parameters ParameterName=default_compression,ParameterValue=lz4` or per-collection at creation | LOW | false (requires analysis_data) |

## Evaluation Notes

- SUST1 extracts the instance family by: `instance_class.replace("db.", "").split(".")[0]` → check if result is in `("r6g", "r7g", "r8g", "t4g", "r6gd")`.
- SUST2 requires live database analysis data. In standalone mode (no DB connection), skip with "info" note.
- When recommending Graviton migration, maintain the same size class (e.g., r5.xlarge → r6g.xlarge). R8G is latest generation but requires engine 5.0+.
- For compression, check engine version first: ZSTD only available on 8.0+. Recommend LZ4 for 5.0 clusters.
