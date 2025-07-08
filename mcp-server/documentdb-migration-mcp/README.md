# DocumentDB Migration MCP Server

This MCP (Model Context Protocol) server provides tools for migrating data to DocumentDB. It wraps the existing DocumentDB migration tools into an MCP server interface, making them accessible through the MCP protocol.

## Features

- **Full Load Migration**: Migrate data from a source database to DocumentDB in a one-time operation
- **Filtered Full Load Migration**: Migrate data with filtering based on TTL
- **Change Data Capture (CDC)**: Continuously replicate changes from a source database to DocumentDB
- **Resume Token Management**: Get change stream resume tokens for CDC operations
- **Automatic Boundary Generation**: Automatically generate optimal boundaries for segmenting collections during migration
- **Index Management**: Export, restore, and check compatibility of indexes between MongoDB and DocumentDB

## Installation

### 1. Through your favorite AI Agentic tool (e.g., for Amazon Q Developer CLI, Cline, etc.) using uv package (Recommended)

```json
{
  "documentdb-migration-mcp-server": {
    "autoApprove": [],
    "disabled": false,
    "timeout": 60,
    "command": "uvx",
    "args": [
      "documentdb-migration-mcp-server@latest"
    ],
    "env": {
      "FASTMCP_LOG_LEVEL": "INFO",
      "AWS_PROFILE": "default",
      "AWS_REGION": "us-east-1"
    },
    "transportType": "stdio"
  }
}
```
We recommend that you also install **Amazon DocumentDB MCP Server** as well along with Migration MCP Server. The DocumentDB MCP server will help with DML operations. 

```json
{
    "awslabs.documentdb-mcp-server": {
      "command": "uvx",
      "args": [
        "awslabs.documentdb-mcp-server@latest",
      ],
      "env": {
        "AWS_PROFILE": "default",
        "AWS_REGION": "us-east-1",
        "FASTMCP_LOG_LEVEL": "ERROR"
      },
      "disabled": false,
      "autoApprove": []
    }
}
```
You can customize the AWS profile and region by changing the `AWS_PROFILE` and `AWS_REGION` environment variables.

### 2. Through your favorite AI Agentic tool using local file

First, download the source code:

```bash
# Clone the repository
git clone https://github.com/awslabs/documentdb-migration-mcp-server.git

# Install dependencies
pip install pymongo boto3 fastmcp
```

Then configure your AI Agentic tool with:

```json
{
  "documentdb-migration-mcp-server": {
    "autoApprove": [],
    "disabled": false,
    "timeout": 60,
    "command": "python3",
    "args": [
      "-m",
      "awslabs.documentdb_migration_mcp_server.server"
    ],
    "env": {
      "FASTMCP_LOG_LEVEL": "INFO",
      "AWS_PROFILE": "default",
      "AWS_REGION": "us-east-1",
      "PYTHONPATH": "/path/to/documentdb-migration-mcp-server"
    },
    "transportType": "stdio"
  }
}
```

> **Note:** Replace `/path/to/documentdb-migration-mcp-server` with the actual path to your local repository.

### 3. Using bash

```bash
# Install using uv package
uvx documentdb-migration-mcp-server@latest

# Or run from source
git clone https://github.com/awslabs/documentdb-migration-mcp-server.git
cd documentdb-migration-mcp-server
pip install pymongo boto3 mcp-server
python -m awslabs.documentdb_migration_mcp_server.server
```

## MCP Tools

### runFullLoad

Run a full load migration from source to target.

**Parameters:**
- `source_uri`: Source URI in MongoDB Connection String format
- `target_uri`: Target URI in MongoDB Connection String format
- `source_namespace`: Source Namespace as <database>.<collection>
- `target_namespace`: (Optional) Target Namespace as <database>.<collection>, defaults to source_namespace
- `boundaries`: (Optional) Comma-separated list of boundaries for segmenting. If not provided, boundaries will be auto-generated.
- `boundary_datatype`: (Optional) Datatype of boundaries (objectid, string, int). Auto-detected if boundaries are auto-generated.
- `max_inserts_per_batch`: Maximum number of inserts to include in a single batch
- `feedback_seconds`: Number of seconds between feedback output
- `dry_run`: Read source changes only, do not apply to target
- `verbose`: Enable verbose logging
- `create_cloudwatch_metrics`: Create CloudWatch metrics for monitoring
- `cluster_name`: Name of cluster for CloudWatch metrics

### runFilteredFullLoad

Run a filtered full load migration from source to target.

**Parameters:**
- `source_uri`: Source URI in MongoDB Connection String format
- `target_uri`: Target URI in MongoDB Connection String format
- `source_namespace`: Source Namespace as <database>.<collection>
- `target_namespace`: (Optional) Target Namespace as <database>.<collection>, defaults to source_namespace
- `boundaries`: (Optional) Comma-separated list of boundaries for segmenting. If not provided, boundaries will be auto-generated.
- `boundary_datatype`: (Optional) Datatype of boundaries (objectid, string, int). Auto-detected if boundaries are auto-generated.
- `max_inserts_per_batch`: Maximum number of inserts to include in a single batch
- `feedback_seconds`: Number of seconds between feedback output
- `dry_run`: Read source changes only, do not apply to target
- `verbose`: Enable verbose logging

### runCDC

Run a CDC (Change Data Capture) migration from source to target.

**Parameters:**
- `source_uri`: Source URI in MongoDB Connection String format
- `target_uri`: Target URI in MongoDB Connection String format
- `source_namespace`: Source Namespace as <database>.<collection>
- `target_namespace`: (Optional) Target Namespace as <database>.<collection>, defaults to source_namespace
- `start_position`: Starting position - 0 for all available changes, YYYY-MM-DD+HH:MM:SS in UTC, or change stream resume token
- `use_oplog`: Use the oplog as change data capture source (MongoDB only)
- `use_change_stream`: Use change streams as change data capture source (MongoDB or DocumentDB)
- `threads`: Number of threads (parallel processing)
- `duration_seconds`: Number of seconds to run before exiting, 0 = run forever
- `max_operations_per_batch`: Maximum number of operations to include in a single batch
- `max_seconds_between_batches`: Maximum number of seconds to await full batch
- `feedback_seconds`: Number of seconds between feedback output
- `dry_run`: Read source changes only, do not apply to target
- `verbose`: Enable verbose logging
- `create_cloudwatch_metrics`: Create CloudWatch metrics for monitoring
- `cluster_name`: Name of cluster for CloudWatch metrics

### getResumeToken

Get the current change stream resume token.

**Parameters:**
- `source_uri`: Source URI in MongoDB Connection String format
- `source_namespace`: Source Namespace as <database>.<collection>

### generateBoundaries

Generate boundaries for segmenting a collection during migration.

**Parameters:**
- `uri`: MongoDB Connection String format URI
- `database`: Database name
- `collection`: Collection name
- `num_segments`: Number of segments to divide the collection into
- `use_single_cursor`: (Optional) Use a single cursor to scan the collection (slower but more reliable), defaults to false

### dumpIndexes

Dump indexes from a MongoDB or DocumentDB instance.

**Parameters:**
- `uri`: URI to connect to MongoDB or Amazon DocumentDB
- `output_dir`: (Optional) Directory to export indexes to. If not provided, a temporary directory will be created.
- `dry_run`: (Optional) Perform processing, but do not actually export indexes
- `debug`: (Optional) Output debugging information

### restoreIndexes

Restore indexes to an Amazon DocumentDB instance.

**Parameters:**
- `uri`: URI to connect to Amazon DocumentDB
- `index_dir`: Directory containing index metadata to restore from
- `skip_incompatible`: (Optional) Skip incompatible indexes when restoring metadata, defaults to true
- `support_2dsphere`: (Optional) Support 2dsphere indexes creation, defaults to false
- `dry_run`: (Optional) Perform processing, but do not actually restore indexes
- `debug`: (Optional) Output debugging information
- `shorten_index_name`: (Optional) Shorten long index name to compatible length, defaults to true
- `skip_id_indexes`: (Optional) Do not create _id indexes, defaults to true

### showIndexCompatibilityIssues

Show compatibility issues with Amazon DocumentDB.

**Parameters:**
- `index_dir`: Directory containing index metadata to check
- `debug`: (Optional) Output debugging information

### showCompatibleIndexes

Show compatible indexes with Amazon DocumentDB.

**Parameters:**
- `index_dir`: Directory containing index metadata to check
- `debug`: (Optional) Output debugging information

## Requirements

- Python 3.10+
- PyMongo
- Boto3 (for CloudWatch metrics)
- MCP Server

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.