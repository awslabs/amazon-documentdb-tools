# Amazon DocumentDB Serverless

Source: https://docs.aws.amazon.com/documentdb/latest/developerguide/docdb-serverless.html

## Overview

Amazon DocumentDB Serverless is an on-demand, auto-scaling configuration that dynamically adjusts database capacity based on application demand. You pay only for the resources your clusters consume.

## How Serverless Works

- Capacity is measured in **DocumentDB Capacity Units (DCUs)**.
- Scaling granularity: **0.5 DCU increments** (add or remove 0.5, 1, 1.5, 2, etc.).
- You set a **minimum** and **maximum** capacity range.
- Scaling is automatic with no disruption to client transactions.
- Supports both vertical scaling (within an instance) and horizontal scaling (reader instances).

## Use Cases

### Variable / Spiky Workloads
- Sudden, unpredictable increases in activity (e.g., traffic surges, flash sales).
- Database scales to peak load automatically, scales back when surge ends.
- Set upper capacity limit for worst-case; capacity isn't used unless needed.

### Multi-Tenant Applications
- Create a cluster per tenant.
- Each cluster scales independently based on tenant activity.
- Idle tenants incur minimal instance charges.

### New Applications
- Deploy without knowing the right instance size.
- Let the database auto-scale to actual capacity requirements.
- Determine appropriate min/max capacity by observing actual scaling behavior.

### Mixed-Use Applications (OLTP + Analytics)
- Configure reader instances with promotion tiers to scale independently of the writer.
- Reader instances handle query spikes; scale back when usage subsides.

### Capacity Planning
- Avoid manual capacity adjustments.
- Run workload and observe actual scaling to determine optimal capacity.
- Can modify existing instances from provisioned → serverless or serverless → provisioned.

### Development and Testing
- Set low minimum capacity instead of using burstable db.t* instances.
- Set maximum high enough for substantial workloads.
- Instances scale down when not in use to avoid unnecessary charges.

## Advantages Over Provisioned

| Aspect | Provisioned | Serverless |
|--------|-------------|------------|
| Capacity management | Manual instance sizing and resizing | Automatic |
| Scaling speed | Add instances or change class (minutes) | 0.5 DCU increments (seconds) |
| Cost during low activity | Pay for provisioned capacity | Pay only for consumed resources |
| Scaling granularity | Whole instances | 0.5 DCU increments |
| Billing | Per-hour (per-second minimum 10 min) | Per-second |

## Feature Parity

Serverless supports ALL DocumentDB features:
- Reader instances (horizontal scaling + failover)
- Multi-AZ clusters (business continuity)
- Global clusters (cross-region disaster recovery)
- IAM database authentication
- Performance Insights

## Migration Path

1. Add serverless reader instances to existing provisioned cluster.
2. Monitor scaling behavior.
3. Use DocumentDB failover to promote serverless instance to writer.
4. Minimal downtime, no endpoint changes.

## When to Choose Serverless vs Provisioned

| Signal | Recommendation |
|--------|---------------|
| CPU coefficient of variation >30% | Serverless |
| Idle periods >25% of time | Serverless |
| Spike frequency >8% | Serverless |
| Sustained CPU >15%, low variance | Provisioned |
| Predictable, steady workload | Provisioned |
| Need for NVMe local storage | Provisioned (R6GD) |
| Dev/test with intermittent use | Serverless |
| Cost-sensitive variable workload | Serverless |
