# Pricing, Instance Classes, and Cost Optimization

Sources:
- https://docs.aws.amazon.com/documentdb/latest/developerguide/what-is.html
- https://docs.aws.amazon.com/documentdb/latest/developerguide/db-instance-classes.html

## Billing Components

Amazon DocumentDB clusters are billed based on:
1. **Instance hours** (per hour, billed per second, minimum 10 minutes) — based on instance class.
2. **I/O requests** (per 1 million requests/month) — total storage I/O requests.
3. **Backup storage** (per GiB/month) — automated backups + active snapshots.
4. **Data transfer** (per GB) — in/out of instance from/to internet or other regions.

## Storage Configurations (Engine 5.0+)

### Standard Storage
- Storage: ~$0.10/GB-month
- I/O: ~$0.20 per million I/O requests
- Compute: standard instance pricing

### I/O Optimized Storage
- Storage: ~$0.30/GB-month (3x standard)
- I/O: $0.00 (no I/O charges)
- Compute: ~10% premium over standard
- **Best for**: High I/O workloads where I/O costs exceed the storage premium
- Only available on engine versions 5.0 and 8.0

### When to Choose I/O Optimized
- Calculate: `monthly_io_cost > (storage_gb * $0.20) + (compute_premium)`
- If I/O costs are >25% of total database bill, I/O Optimized likely saves money.

## Instance Class Specifications

### R8G — Latest Generation (Graviton4, engine 5.0 and 8.0 only)
| Instance | vCPU | Memory (GiB) | Bandwidth (Gbps) |
|----------|------|-------------|-------------------|
| db.r8g.large | 2 | 16 | 0.937 / 12.5 |
| db.r8g.xlarge | 4 | 32 | 1.875 / 12.5 |
| db.r8g.2xlarge | 8 | 64 | 3.75 / 15.0 |
| db.r8g.4xlarge | 16 | 128 | 7.5 / 15.0 |
| db.r8g.8xlarge | 32 | 256 | 15 |
| db.r8g.12xlarge | 48 | 384 | 22 |
| db.r8g.16xlarge | 64 | 512 | 30 |

Up to 30% better performance over R6G.

### R6G — Current Generation (Graviton2, engine 4.0+)
| Instance | vCPU | Memory (GiB) | Bandwidth (Gbps) |
|----------|------|-------------|-------------------|
| db.r6g.large | 2 | 16 | 0.75 / 10 |
| db.r6g.xlarge | 4 | 32 | 1.25 / 10 |
| db.r6g.2xlarge | 8 | 64 | 2.5 / 10 |
| db.r6g.4xlarge | 16 | 128 | 5.0 / 10 |
| db.r6g.8xlarge | 32 | 256 | 12 |
| db.r6g.12xlarge | 48 | 384 | 20 |
| db.r6g.16xlarge | 64 | 512 | 25 |

Up to 30% better performance over R5 at 5% less cost.

### R6GD — NVMe-backed (Graviton2, engine 5.0+)
| Instance | vCPU | Memory (GiB) | NVMe Cache (GiB) | Bandwidth (Gbps) |
|----------|------|-------------|-------------------|-------------------|
| db.r6gd.xlarge | 4 | 32 | 173 | 1.25 / 10 |
| db.r6gd.2xlarge | 8 | 64 | 346 | 2.5 / 10 |
| db.r6gd.4xlarge | 16 | 128 | 694 | 5.0 / 10 |
| db.r6gd.8xlarge | 32 | 256 | 1388 | 12 |
| db.r6gd.12xlarge | 48 | 384 | 2082 | 20 |
| db.r6gd.16xlarge | 64 | 512 | 2776 | 25 |

NVMe SSD provides tiered cache for ephemeral data. Best for I/O-bound workloads with low BufferCacheHitRatio.

### R5 — Previous Generation (engine 3.6+)
| Instance | vCPU | Memory (GiB) | Bandwidth (Gbps) |
|----------|------|-------------|-------------------|
| db.r5.large | 2 | 16 | 0.75 / 10 |
| db.r5.xlarge | 4 | 32 | 1.25 / 10 |
| db.r5.2xlarge | 8 | 64 | 2.5 / 10 |
| db.r5.4xlarge | 16 | 128 | 5.0 / 10 |
| db.r5.8xlarge | 32 | 256 | 10 |
| db.r5.12xlarge | 48 | 384 | 12 |
| db.r5.16xlarge | 64 | 512 | 20 |
| db.r5.24xlarge | 96 | 768 | 25 |

### Burstable Instances
| Instance | vCPU | Memory (GiB) | Engine |
|----------|------|-------------|--------|
| db.t4g.medium | 2 | 4 | 4.0+ (Graviton2) |
| db.t3.medium | 2 | 4 | 3.6+ |

T-series run in Unlimited CPU burst mode. Extra burst usage billed separately. Not supported for Global Clusters.

## Instance Sizing Decision Guide

1. **Start with BufferCacheHitRatio**: If <95%, working set doesn't fit in memory → scale up.
2. **Only 2/3 of RAM available for cache**: A db.r6g.xlarge (32 GiB) provides ~21 GiB usable cache.
3. **CPU-bound**: Scale up to more vCPUs or distribute reads to replicas.
4. **I/O-bound (low cache hit, low CPU)**: Consider NVMe instances (R6GD).
5. **Cost-sensitive**: Graviton instances (R6G, R8G, T4G) offer best price-performance.
6. **Variable workloads**: Consider Serverless (see serverless.md).

## Cost Optimization Checklist

- [ ] Remove unused indexes (saves storage + I/O + write overhead)
- [ ] Enable compression (reduces storage and I/O costs)
- [ ] Use rolling collections instead of TTL for time-series data
- [ ] Stop dev/test clusters when not in use
- [ ] Use single-instance clusters for non-production
- [ ] Set billing alerts at 50% and 75% of expected bill
- [ ] Disable TTL and change streams if not used (they incur I/Os)
- [ ] Use `secondaryPreferred` reads to distribute load across replicas
- [ ] Right-size instances based on actual BufferCacheHitRatio and CPU metrics
- [ ] Evaluate Standard vs I/O Optimized storage based on actual I/O costs
