# Supported MongoDB APIs, Operations, and Data Types in Amazon DocumentDB

Source: https://docs.aws.amazon.com/documentdb/latest/developerguide/mongo-apis.html

Amazon DocumentDB is compatible with MongoDB 3.6, 4.0, 5.0, and 8.0 APIs.

## Database Commands

### Administrative Commands
| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|---------|-----|-----|-----|-----|---------|
| create | Yes | Yes | Yes | Yes | Yes |
| createView | No | No | No | Yes | No |
| createIndexes | Yes | Yes | Yes | Yes | Yes |
| currentOp | Yes | Yes | Yes | Yes | Yes |
| drop | Yes | Yes | Yes | Yes | Yes |
| dropDatabase | Yes | Yes | Yes | Yes | Yes |
| dropIndexes | Yes | Yes | Yes | Yes | Yes |
| killCursors | Yes | Yes | Yes | Yes | Yes |
| killOp | Yes | Yes | Yes | Yes | Yes |
| listCollections | Yes | Yes | Yes | Yes | Yes |
| listDatabases | Yes | Yes | Yes | Yes | Yes |
| listIndexes | Yes | Yes | Yes | Yes | Yes |
| reIndex | No | No | Yes | Yes | No |
| renameCollection | Yes | Yes | Yes | Yes | No |
| Capped Collections | No | No | No | No | No |

### Aggregation
| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|---------|-----|-----|-----|-----|---------|
| aggregate | Yes | Yes | Yes | Yes | Yes |
| count | Yes | Yes | Yes | Yes | Yes |
| distinct | Yes | Yes | Yes | Yes | Yes |
| mapReduce | No | No | No | Yes | No |

### Query and Write Operations
| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|---------|-----|-----|-----|-----|---------|
| find | Yes | Yes | Yes | Yes | Yes |
| findAndModify | Yes | Yes | Yes | Yes | Yes |
| insert | Yes | Yes | Yes | Yes | Yes |
| update | Yes | Yes | Yes | Yes | Yes |
| delete | Yes | Yes | Yes | Yes | Yes |
| getMore | Yes | Yes | Yes | Yes | Yes |
| Change streams | Yes | Yes | Yes | Yes | No |
| GridFS | Yes | Yes | Yes | Yes | No |

### Sessions
| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|---------|-----|-----|-----|-----|---------|
| abortTransaction | No | Yes | Yes | Yes | No |
| commitTransaction | No | Yes | Yes | Yes | No |
| startSession | No | Yes | Yes | Yes | No |

### Diagnostic Commands
| Command | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|---------|-----|-----|-----|-----|---------|
| collStats | Yes | Yes | Yes | Yes | Yes |
| dbStats | Yes | Yes | Yes | Yes | Yes |
| explain | Yes | Yes | Yes | Yes | Yes |
| explain: executionStats | Yes | Yes | Yes | Yes | Yes |
| serverStatus | Yes | Yes | Yes | Yes | Yes |
| profiler | Yes | Yes | Yes | Yes | No |

## Query and Projection Operators

### Comparison
| Operator | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|----------|-----|-----|-----|-----|---------|
| $eq | Yes | Yes | Yes | Yes | Yes |
| $gt | Yes | Yes | Yes | Yes | Yes |
| $gte | Yes | Yes | Yes | Yes | Yes |
| $in | Yes | Yes | Yes | Yes | Yes |
| $lt | Yes | Yes | Yes | Yes | Yes |
| $lte | Yes | Yes | Yes | Yes | Yes |
| $ne | Yes | Yes | Yes | Yes | Yes |
| $nin | Yes | Yes | Yes | Yes | Yes |

### Logical
| Operator | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|----------|-----|-----|-----|-----|---------|
| $and | Yes | Yes | Yes | Yes | Yes |
| $or | Yes | Yes | Yes | Yes | Yes |
| $not | Yes | Yes | Yes | Yes | Yes |
| $nor | Yes | Yes | Yes | Yes | Yes |

### Element
| Operator | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|----------|-----|-----|-----|-----|---------|
| $exists | Yes | Yes | Yes | Yes | Yes |
| $type | Yes | Yes | Yes | Yes | Yes |

### Evaluation
| Operator | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|----------|-----|-----|-----|-----|---------|
| $expr | No | Yes | Yes | Yes | No |
| $jsonSchema | No | Yes | Yes | Yes | No |
| $mod | Yes | Yes | Yes | Yes | Yes |
| $regex | Yes | Yes | Yes | Yes | Yes |
| $text | No | No | Yes | Yes | No |
| $where | No | No | No | No | No |

### Array
| Operator | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|----------|-----|-----|-----|-----|---------|
| $all | Yes | Yes | Yes | Yes | Yes |
| $elemMatch | Yes | Yes | Yes | Yes | Yes |
| $size | Yes | Yes | Yes | Yes | Yes |

## Update Operators

### Field Operators
| Operator | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|----------|-----|-----|-----|-----|---------|
| $set | Yes | Yes | Yes | Yes | Yes |
| $unset | Yes | Yes | Yes | Yes | Yes |
| $inc | Yes | Yes | Yes | Yes | Yes |
| $mul | Yes | Yes | Yes | Yes | Yes |
| $min | Yes | Yes | Yes | Yes | Yes |
| $max | Yes | Yes | Yes | Yes | Yes |
| $rename | Yes | Yes | Yes | Yes | Yes |
| $currentDate | Yes | Yes | Yes | Yes | Yes |
| $setOnInsert | Yes | Yes | Yes | Yes | Yes |

### Array Operators
| Operator | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|----------|-----|-----|-----|-----|---------|
| $push | Yes | Yes | Yes | Yes | Yes |
| $pull | Yes | Yes | Yes | Yes | Yes |
| $pop | Yes | Yes | Yes | Yes | Yes |
| $addToSet | Yes | Yes | Yes | Yes | Yes |
| $pullAll | Yes | Yes | Yes | Yes | Yes |

## Aggregation Pipeline Stage Operators

| Stage | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|-------|-----|-----|-----|-----|---------|
| $match | Yes | Yes | Yes | Yes | Yes |
| $project | Yes | Yes | Yes | Yes | Yes |
| $group | Yes | Yes | Yes | Yes | Yes |
| $sort | Yes | Yes | Yes | Yes | Yes |
| $limit | Yes | Yes | Yes | Yes | Yes |
| $skip | Yes | Yes | Yes | Yes | Yes |
| $unwind | Yes | Yes | Yes | Yes | Yes |
| $lookup | Yes | Yes | Yes | Yes | Yes |
| $addFields | Yes | Yes | Yes | Yes | Yes |
| $count | Yes | Yes | Yes | Yes | Yes |
| $sample | Yes | Yes | Yes | Yes | Yes |
| $redact | Yes | Yes | Yes | Yes | Yes |
| $replaceRoot | Yes | Yes | Yes | Yes | Yes |
| $out | Yes | Yes | Yes | Yes | No |
| $geoNear | Yes | Yes | Yes | Yes | Yes |
| $indexStats | Yes | Yes | Yes | Yes | Yes |
| $currentOp | Yes | Yes | Yes | Yes | Yes |
| $bucket | No | No | No | Yes | No |
| $merge | - | - | No | Yes | No |
| $set | - | - | No | Yes | No |
| $unset | - | - | No | Yes | No |
| $replaceWith | No | No | No | Yes | No |
| $vectorSearch | No | No | No | Yes | No |
| $facet | No | No | No | No | No |
| $graphLookup | No | No | No | No | No |
| $sortByCount | No | No | No | No | No |
| $unionWith | - | - | No | No | No |

## Indexes

| Type | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|------|-----|-----|-----|-----|---------|
| Single Field | Yes | Yes | Yes | Yes | Yes |
| Compound | Yes | Yes | Yes | Yes | Yes |
| Multikey | Yes | Yes | Yes | Yes | Yes |
| 2dsphere | Yes | Yes | Yes | Yes | Yes |
| Text | No | No | Yes | Yes | No |
| Hashed | No | No | No | No | No |
| Wildcard | No | No | No | No | No |

### Index Properties
| Property | 3.6 | 4.0 | 5.0 | 8.0 | Elastic |
|----------|-----|-----|-----|-----|---------|
| TTL | Yes | Yes | Yes | Yes | Yes |
| Unique | Yes | Yes | Yes | Yes | Yes |
| Sparse | Yes | Yes | Yes | Yes | Yes |
| Partial | No | No | Yes | Yes | No |
| Background | Yes | Yes | Yes | Yes | Yes |
| Case Insensitive | No | No | No | Yes | No |
| Vector | No | No | Yes | Yes | No |

## NOT Supported (across all versions)

- Capped Collections
- $where operator
- $graphLookup
- $facet
- Hashed indexes
- Wildcard indexes
- Retryable writes (disable with `retryWrites=false`)
- admin/local databases
- JavaScript execution (no JS data types)
