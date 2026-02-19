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

"""Full Load migration tools for DocumentDB Migration MCP Server."""

import os
import sys
import time
import subprocess
import tempfile
from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional
from pydantic import Field
from loguru import logger
from awslabs.documentdb_migration_mcp_server.boundary_tools import generate_boundaries


async def run_full_load(
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
        Optional[str],
        Field(
            description='Target Namespace as <database>.<collection>, defaults to source_namespace'
        ),
    ] = None,
    boundaries: Annotated[
        str,
        Field(
            description='Comma-separated list of boundaries for segmenting'
        ),
    ] = None,
    boundary_datatype: Annotated[
        str,
        Field(
            description='Datatype of boundaries (objectid, string, int)'
        ),
    ] = 'objectid',
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
        Optional[str],
        Field(
            description='Name of cluster for CloudWatch metrics'
        ),
    ] = None,
) -> Dict[str, Any]:
    """Run a full load migration from source to target.
    
    This tool executes a full load migration from a source DocumentDB/MongoDB database
    to a target DocumentDB database. It uses the fl-multiprocess.py script to perform
    the migration with the specified parameters.
    
    Returns:
        Dict[str, Any]: Status of the migration operation
    """
    logger.info(f"Starting full load migration from {source_namespace} to {target_namespace or source_namespace}")
    
    # Validate parameters
    if create_cloudwatch_metrics and not cluster_name:
        raise ValueError("Must supply cluster_name when capturing CloudWatch metrics")
    
    # Auto-generate boundaries if not provided
    if not boundaries:
        logger.info("No boundaries provided, auto-generating boundaries")
        
        # Parse source namespace to get database and collection
        db_name, coll_name = source_namespace.split('.', 1)
        
        # Default to 4 segments if not specified
        num_segments = 4
        
        # Generate boundaries
        boundary_result = await generate_boundaries(
            uri=source_uri,
            database=db_name,
            collection=coll_name,
            num_segments=num_segments,
            use_single_cursor=False
        )
        
        if not boundary_result["success"]:
            raise ValueError(f"Failed to auto-generate boundaries: {boundary_result['message']}")
        
        boundaries = boundary_result["boundaries_csv"]
        boundary_datatype = boundary_result["boundary_datatype"]
        
        logger.info(f"Auto-generated boundaries: {boundaries}")
        logger.info(f"Boundary datatype: {boundary_datatype}")
    
    # Build command
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "fl-multiprocess.py")
    
    cmd = [
        "python3",
        script_path,
        "--source-uri", source_uri,
        "--target-uri", target_uri,
        "--source-namespace", source_namespace,
        "--boundaries", boundaries,
        "--boundary-datatype", boundary_datatype,
        "--max-inserts-per-batch", str(max_inserts_per_batch),
        "--feedback-seconds", str(feedback_seconds),
    ]
    
    if target_namespace:
        cmd.extend(["--target-namespace", target_namespace])
    
    if dry_run:
        cmd.append("--dry-run")
    
    if verbose:
        cmd.append("--verbose")
    
    if create_cloudwatch_metrics:
        cmd.append("--create-cloudwatch-metrics")
        cmd.extend(["--cluster-name", cluster_name])
    
    # Execute command
    try:
        logger.info(f"Executing command: {' '.join(cmd)}")
        
        # Create a log file for the output
        try:
            # Try to use a directory in the user's home directory
            log_dir = os.path.join(os.path.expanduser("~"), ".documentdb-migration", "logs")
            os.makedirs(log_dir, exist_ok=True)
        except Exception as e:
            # Fall back to a temporary directory if home directory is not accessible
            logger.warning(f"Could not create log directory in home directory: {str(e)}")
            log_dir = tempfile.mkdtemp(prefix="documentdb_migration_logs_")
            logger.info(f"Using temporary directory for logs: {log_dir}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(log_dir, f"full_load_{timestamp}.log")
        
        logger.info(f"Logging output to: {log_file_path}")
        
        # Open the log file
        log_file = open(log_file_path, "w")
        
        # Start the process with stdout and stderr redirected to the log file
        process = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=log_file,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Process is running in the background
        return {
            "success": True,
            "message": f"Full load migration started successfully. Logs are being written to {log_file_path}",
            "process_id": process.pid,
            "command": " ".join(cmd),
            "log_file": log_file_path,
        }
    except Exception as e:
        logger.error(f"Error starting full load migration: {str(e)}")
        raise ValueError(f"Failed to start full load migration: {str(e)}")


async def run_filtered_full_load(
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
        Optional[str],
        Field(
            description='Target Namespace as <database>.<collection>, defaults to source_namespace'
        ),
    ] = None,
    boundaries: Annotated[
        str,
        Field(
            description='Comma-separated list of boundaries for segmenting'
        ),
    ] = None,
    boundary_datatype: Annotated[
        str,
        Field(
            description='Datatype of boundaries (objectid, string, int)'
        ),
    ] = 'objectid',
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
) -> Dict[str, Any]:
    """Run a filtered full load migration from source to target.
    
    This tool executes a filtered full load migration from a source DocumentDB/MongoDB database
    to a target DocumentDB database. It uses the fl-multiprocess-filtered.py script to perform
    the migration with the specified parameters. This version filters documents based on TTL.
    
    Returns:
        Dict[str, Any]: Status of the migration operation
    """
    logger.info(f"Starting filtered full load migration from {source_namespace} to {target_namespace or source_namespace}")
    
    # Validate parameters
    
    # Auto-generate boundaries if not provided
    if not boundaries:
        logger.info("No boundaries provided, auto-generating boundaries")
        
        # Parse source namespace to get database and collection
        db_name, coll_name = source_namespace.split('.', 1)
        
        # Default to 4 segments if not specified
        num_segments = 4
        
        # Generate boundaries
        boundary_result = await generate_boundaries(
            uri=source_uri,
            database=db_name,
            collection=coll_name,
            num_segments=num_segments,
            use_single_cursor=False
        )
        
        if not boundary_result["success"]:
            raise ValueError(f"Failed to auto-generate boundaries: {boundary_result['message']}")
        
        boundaries = boundary_result["boundaries_csv"]
        boundary_datatype = boundary_result["boundary_datatype"]
        
        logger.info(f"Auto-generated boundaries: {boundaries}")
        logger.info(f"Boundary datatype: {boundary_datatype}")
    
    # Build command
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "fl-multiprocess-filtered.py")
    
    cmd = [
        "python3",
        script_path,
        "--source-uri", source_uri,
        "--target-uri", target_uri,
        "--source-namespace", source_namespace,
        "--boundaries", boundaries,
        "--boundary-datatype", boundary_datatype,
        "--max-inserts-per-batch", str(max_inserts_per_batch),
        "--feedback-seconds", str(feedback_seconds),
    ]
    
    if target_namespace:
        cmd.extend(["--target-namespace", target_namespace])
    
    if dry_run:
        cmd.append("--dry-run")
    
    if verbose:
        cmd.append("--verbose")
    
    # Execute command
    try:
        logger.info(f"Executing command: {' '.join(cmd)}")
        
        # Create a log file for the output
        try:
            # Try to use a directory in the user's home directory
            log_dir = os.path.join(os.path.expanduser("~"), ".documentdb-migration", "logs")
            os.makedirs(log_dir, exist_ok=True)
        except Exception as e:
            # Fall back to a temporary directory if home directory is not accessible
            logger.warning(f"Could not create log directory in home directory: {str(e)}")
            log_dir = tempfile.mkdtemp(prefix="documentdb_migration_logs_")
            logger.info(f"Using temporary directory for logs: {log_dir}")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(log_dir, f"filtered_full_load_{timestamp}.log")
        
        logger.info(f"Logging output to: {log_file_path}")
        
        # Open the log file
        log_file = open(log_file_path, "w")
        
        # Start the process with stdout and stderr redirected to the log file
        process = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=log_file,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Process is running in the background
        return {
            "success": True,
            "message": f"Filtered full load migration started successfully. Logs are being written to {log_file_path}",
            "process_id": process.pid,
            "command": " ".join(cmd),
            "log_file": log_file_path,
        }
    except Exception as e:
        logger.error(f"Error starting filtered full load migration: {str(e)}")
        raise ValueError(f"Failed to start filtered full load migration: {str(e)}")
