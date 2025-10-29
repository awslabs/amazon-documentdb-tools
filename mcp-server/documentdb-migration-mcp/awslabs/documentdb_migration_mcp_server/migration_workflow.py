# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
# with the License. A copy of the License is located at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES
# OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions
# and limitations under the License.

"""Migration workflow tools for DocumentDB Migration MCP Server."""

import os
import tempfile
import time
from typing import Annotated, Any, Dict, List, Optional
from pydantic import Field
from loguru import logger

from awslabs.documentdb_migration_mcp_server.index_tools import (
    dump_indexes, restore_indexes, show_compatibility_issues
)
from awslabs.documentdb_migration_mcp_server.full_load_tools import run_full_load
# CDC tools are not used in this workflow
from awslabs.documentdb_migration_mcp_server.boundary_tools import generate_boundaries


async def run_easy_migration(
    source_uri: Annotated[
        str,
        Field(
            description='Source URI in MongoDB Connection String format'
        ),
    ],
    target_uri: Annotated[
        str,
        Field(
            description='Target URI in MongoDB Connection String format'
        ),
    ],
    source_namespace: Annotated[
        str,
        Field(
            description='Source Namespace as <database>.<collection>'
        ),
    ],
    target_namespace: Annotated[
        str,
        Field(
            description='Target Namespace as <database>.<collection>, defaults to source_namespace'
        ),
    ] = None,
    max_inserts_per_batch: Annotated[
        int,
        Field(
            description='Maximum number of inserts to include in a single batch'
        ),
    ] = 100,
    feedback_seconds: Annotated[
        int,
        Field(
            description='Number of seconds between feedback output'
        ),
    ] = 60,
    dry_run: Annotated[
        bool,
        Field(
            description='Read source changes only, do not apply to target'
        ),
    ] = False,
    verbose: Annotated[
        bool,
        Field(
            description='Enable verbose logging'
        ),
    ] = False,
    create_cloudwatch_metrics: Annotated[
        bool,
        Field(
            description='Create CloudWatch metrics for monitoring'
        ),
    ] = False,
    cluster_name: Annotated[
        str,
        Field(
            description='Name of cluster for CloudWatch metrics'
        ),
    ] = None,
    skip_incompatible_indexes: Annotated[
        bool,
        Field(
            description='Skip incompatible indexes when restoring metadata'
        ),
    ] = True,
    support_2dsphere: Annotated[
        bool,
        Field(
            description='Support 2dsphere indexes creation (collections must use GeoJSON Point type for indexing)'
        ),
    ] = False,
    skip_id_indexes: Annotated[
        bool,
        Field(
            description='Do not create _id indexes'
        ),
    ] = True,
) -> Dict[str, Any]:
    """Run an easy migration workflow from source to target.
    
    This tool executes a complete migration workflow:
    1. Check compatibility of indexes
    2. Dump indexes from source
    3. Restore indexes to target
    4. Run full load migration
    
    After the migration is complete, you can use the getResumeToken and runCDC tools
    to set up Change Data Capture (CDC) for continuous replication.
    
    Returns:
        Dict[str, Any]: Status of the migration operation
    """
    results = {}
    
    # Set target_namespace to source_namespace if not provided
    if not target_namespace:
        target_namespace = source_namespace
    
    # Step 1: Create a temporary directory for index operations
    index_dir = tempfile.mkdtemp(prefix="migration_indexes_")
    logger.info(f"Created temporary directory for index operations: {index_dir}")
    results["index_dir"] = index_dir
    
    # Step 2: Dump indexes from source
    logger.info("Step 1/5: Dumping indexes from source...")
    dump_result = await dump_indexes(
        uri=source_uri,
        output_dir=index_dir,
        dry_run=dry_run,
        debug=verbose
    )
    # Only include essential information in the results
    results["dump_indexes"] = {
        "success": dump_result["success"],
        "message": dump_result["message"],
        "output_dir": dump_result.get("output_dir", "")
    }
    
    if not dump_result["success"]:
        logger.error("Failed to dump indexes from source")
        return results
    
    # Step 3: Check compatibility of indexes
    logger.info("Step 2/5: Checking compatibility of indexes...")
    compatibility_result = await show_compatibility_issues(
        index_dir=index_dir,
        debug=verbose
    )
    # Only include essential information in the results
    results["compatibility_check"] = {
        "success": compatibility_result["success"],
        "message": compatibility_result["message"]
    }
    # Include issues if they exist
    if "issues" in compatibility_result:
        results["compatibility_check"]["issues"] = compatibility_result["issues"]
    
    # Step 4: Restore indexes to target
    logger.info("Step 3/5: Restoring indexes to target...")
    restore_result = await restore_indexes(
        uri=target_uri,
        index_dir=index_dir,
        skip_incompatible=skip_incompatible_indexes,
        support_2dsphere=support_2dsphere,
        dry_run=dry_run,
        debug=verbose,
        shorten_index_name=True,
        skip_id_indexes=skip_id_indexes
    )
    # Only include essential information in the results
    results["restore_indexes"] = {
        "success": restore_result["success"],
        "message": restore_result["message"]
    }
    
    if not restore_result["success"]:
        logger.warning("Failed to restore some indexes to target, but continuing with migration")
    
    # Step 5: Run full load migration
    logger.info("Step 4/5: Running full load migration...")
    
    # Generate boundaries for segmenting the collection
    try:
        (db_name, collection_name) = source_namespace.split('.', 1)
        boundaries_result = await generate_boundaries(
            uri=source_uri,
            database=db_name,
            collection=collection_name,
            num_segments=4,  # Use 4 segments by default
            use_single_cursor=False
        )
        # Only include essential information in the results
        results["boundaries"] = {
            "success": boundaries_result["success"],
            "message": boundaries_result["message"]
        }
        boundaries = boundaries_result.get("boundaries_csv", None)
    except Exception as e:
        logger.warning(f"Failed to generate boundaries: {str(e)}")
        boundaries = None
    
    full_load_result = await run_full_load(
        source_uri=source_uri,
        target_uri=target_uri,
        source_namespace=source_namespace,
        target_namespace=target_namespace,
        boundaries=boundaries,
        max_inserts_per_batch=max_inserts_per_batch,
        feedback_seconds=feedback_seconds,
        dry_run=dry_run,
        verbose=verbose,
        create_cloudwatch_metrics=create_cloudwatch_metrics,
        cluster_name=cluster_name
    )
    # Only include essential information in the results
    results["full_load"] = {
        "success": full_load_result["success"],
        "message": full_load_result["message"],
        "log_file": full_load_result.get("log_file", "")
    }
    
    if not full_load_result["success"]:
        logger.error("Failed to run full load migration")
        return results
    
    # Step 5: Add message about CDC
    logger.info("Step 5/5: Full load migration completed")
    logger.info("To start CDC (Change Data Capture), use the getResumeToken tool to get a resume token, then use the runCDC tool with that token")
    
    logger.info("Migration workflow completed successfully")
    results["success"] = True
    results["message"] = "Migration workflow completed successfully"
    
    return results
