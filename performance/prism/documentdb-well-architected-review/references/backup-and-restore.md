# Backup and Restore in Amazon DocumentDB

Source: https://docs.aws.amazon.com/documentdb/latest/developerguide/backup_restore.html

## Overview

Amazon DocumentDB continuously backs up data to Amazon S3 with a configurable retention period of 1–35 days. Backups are:
- **Automatic**: No manual intervention required.
- **Incremental**: Only changes are backed up after the initial full backup.
- **Continuous**: Enables point-in-time recovery to any second within the retention period (up to last 5 minutes).
- **Zero performance impact**: Backup process does not affect cluster performance.

Storage is replicated **6 ways across 3 Availability Zones** — clusters are highly durable regardless of instance count.

## Key Concepts

| Concept | Description |
|---------|-------------|
| Backup retention period | 1–35 days. Defines the window for point-in-time restore. |
| Cluster storage volume | Highly available volume replicating data 6 ways across 3 AZs. |
| Backup window | Daily time period when automatic snapshots are taken. |
| Automatic snapshot | Daily full backups created automatically by continuous backup. |
| Manual snapshot | User-created snapshots that persist beyond the retention period. |

## Automatic vs Manual Snapshots

| Feature | Automatic | Manual |
|---------|-----------|--------|
| Creation | Automatic (daily) | User-initiated |
| Retention | Deleted after retention period (1–35 days) | Persist until explicitly deleted |
| Point-in-time restore | Yes (within retention period) | Restore from snapshot only |
| Cost | Included (up to cluster storage size) | Billed as backup storage |

## Point-in-Time Recovery

- Restore cluster to any second within the backup retention period.
- Creates a NEW cluster from the backup data.
- Useful for recovering from accidental data deletion or corruption.

## Operations

### Create Manual Snapshot
```
aws docdb create-db-cluster-snapshot \
    --db-cluster-identifier my-cluster \
    --db-cluster-snapshot-identifier my-snapshot
```

### Restore from Snapshot
```
aws docdb restore-db-cluster-from-snapshot \
    --db-cluster-identifier my-new-cluster \
    --snapshot-identifier my-snapshot
```

### Restore to Point in Time
```
aws docdb restore-db-cluster-to-point-in-time \
    --source-db-cluster-identifier my-cluster \
    --db-cluster-identifier my-restored-cluster \
    --restore-to-time 2024-01-15T10:30:00Z
```

### Copy Snapshot (cross-region)
```
aws docdb copy-db-cluster-snapshot \
    --source-db-cluster-snapshot-identifier arn:aws:rds:us-east-1:123456789:cluster-snapshot:my-snapshot \
    --target-db-cluster-snapshot-identifier my-snapshot-copy
```

## Best Practices

- Set backup retention to **7+ days** for production workloads.
- Take a **manual snapshot before deleting** any cluster.
- Use **cross-region snapshot copies** for disaster recovery.
- For data export/import, use `mongodump`/`mongorestore` (recommended tools version ≤100.6.1).
- Backup storage beyond the free tier (equal to cluster storage size) incurs additional costs at ~$0.095/GiB-month.
