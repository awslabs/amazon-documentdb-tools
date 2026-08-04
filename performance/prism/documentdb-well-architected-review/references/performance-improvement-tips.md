# Performance Improvement Tips

Source: https://docs.aws.amazon.com/documentdb/latest/developerguide/performance-improvement-tips.html

## 1. Use $match as First Stage in Aggregation Pipelines

Always place `$match` as the first stage for filtering. DocumentDB utilizes indexes effectively when `$match` leads the pipeline, filtering data early and reducing processing overhead.

```javascript
// Optimized approach
db.orders.aggregate([
  { $match: { status: "active", category: "electronics" } }, // Index utilization
  { $group: { _id: "$category", total: { $sum: "$price" } } },
  { $sort: { total: -1 } }
])
```

**Impact**: Early filtering reduces documents processed in subsequent stages → faster execution, lower resource consumption.

## 2. Use $project to Minimize Pipeline Data Size

Carry only essential fields through aggregation pipeline stages. Use `$project` strategically to include just the data you need.

```javascript
// Efficient pipeline design
db.orders.aggregate([
  { $match: { orderDate: { $gte: new Date("2024-01-01") } } },
  { $project: { customerId: 1, totalAmount: 1, status: 1 } }, // Only needed fields
  { $group: { _id: "$customerId", totalSpent: { $sum: "$totalAmount" } } }
])
```

**Impact**: Smaller documents reduce memory usage and improve pipeline processing efficiency.

## 3. Enable Document Compression

Enable document compression from cluster parameter group to lower storage costs, I/O costs, and boost query performance. DocumentDB stores compressed documents on disk AND in RAM.

**Benefits**:
- More documents fit in available memory
- Faster data access with reduced disk reads
- Lower storage costs, I/O costs, and improved query performance

**Version differences**:
- **DocumentDB 5.0**: Compression NOT enabled by default. Must enable at collection or cluster level.
- **DocumentDB 8.0**: Compression enabled by default (dictionary compression).

Use DocumentDB's compression review utility to analyze compression ratios for your collections.

## 4. Leverage Indexes for Optimal Query Performance

Every query should utilize an appropriate index.

### Index Types Available
- Single field indexes
- Compound indexes (most flexible — support various query shapes with a single index)
- Multikey indexes (for array fields)
- 2dsphere indexes (geospatial)
- Text indexes (5.0+)

### Understanding Index Prefixes

Compound indexes work through index prefixes — any left-to-right subset of fields.

Example index: `{ category: 1, price: -1, inStock: 1 }`

Usable prefixes:
- `{ category: 1 }` ✅
- `{ category: 1, price: -1 }` ✅
- `{ category: 1, price: -1, inStock: 1 }` ✅
- `{ price: -1 }` ❌ (doesn't start with first field)
- `{ inStock: 1 }` ❌

### Identifying Queries Not Using Indexes

Use `explain()` to check for `COLLSCAN`:
```javascript
db.collection.find({field: value}).explain()
```

If `winningPlan.stage` is `COLLSCAN`, the query needs an index.

**Impact**: Queries without indexes cause collection scans → increased memory/CPU pressure and elevated latency.

## 5. Optimize Data Models Based on Query Patterns

Align your data model with how your application queries and updates data.

### Embedding for Performance
- Store related data together when frequently accessed as a unit.
- Suitable for one-to-few relationships.

```javascript
// Embedded approach
{
  _id: ObjectId("..."),
  customerName: "John Doe",
  address: { street: "123 Main St", city: "Seattle", zipCode: "98101" },
  recentOrders: [
    { orderId: "ORD001", amount: 99.99, date: "2024-01-15" }
  ]
}
```

### Referencing for Flexibility
- Use references for large or infrequently accessed data.
- Recommended for one-to-many relationships with large datasets.
- Prevents document bloat and improves update performance.

### Collection Splitting Strategy

When large documents have mixed access patterns (some fields updated frequently, others rarely accessed), split into separate collections:

```javascript
// products collection (frequently accessed)
{
  _id: ObjectId("..."),
  productId: "PROD123",
  name: "Wireless Headphones",
  price: 99.99,
  inventory: 45,
  lastSold: "2024-01-15"
}

// product_details collection (infrequently accessed)
{
  _id: ObjectId("..."),
  productId: "PROD123",
  detailedSpecs: { /* large object */ },
  manualPDF: "base64...",
  reviewHistory: [/* large array */]
}
```

**Performance gain**: Smaller documents → faster updates, reduced memory usage, improved cache efficiency.

**Impact**: Inefficient data modeling → suboptimal queries, increased document sizes, elevated memory usage, degraded performance, higher costs.
