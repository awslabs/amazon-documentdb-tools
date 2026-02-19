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

"""CDC migration tools for DocumentDB Migration MCP Server."""

import os
import sys
import time
import subprocess
import tempfile
from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional
from pydantic import Field
from loguru import logger


async def run_cdc(
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
    start_position: Annotated[
        str,
        Field(
            description='Starting position - 0 for all available changes, YYYY-MM-DD+HH:MM:SS in UTC, or change stream resume token'
        ),
    ] = "0",
    use_oplog: Annotated[
        bool,
        Field(
            description='Use the oplog as change data capture source (MongoDB only)'
        ),
    ] = False,
    use_change_stream: Annotated[
        bool,
        Field(
            description='Use change streams as change data capture source (MongoDB or DocumentDB)'
        ),
    ] = False,
    threads: Annotated[
        int,
        Field(
            description='Number of threads (parallel processing)'
        ),
    ] = 1,
    duration_seconds: Annotated[
        int,
        Field(
            description='Number of seconds to run before exiting, 0 = run forever'
        ),
    ] = 0,
    max_operations_per_batch: Annotated[
        int,
        Field(
            description='Maximum number of operations to include in a single batch'
        ),
    ] = 100,
    max_seconds_between_batches: Annotated[
        int,
        Field(
            description='Maximum number of seconds to await full batch'
        ),
    ] = 5,
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
    """Run a CDC (Change Data Capture) migration from source to target.
    
    This tool executes a CDC migration from a source DocumentDB/MongoDB database
    to a target DocumentDB database. It uses the cdc-multiprocess.py script to perform
    the migration with the specified parameters.
    
    Returns:
        Dict[str, Any]: Status of the migration operation
    """
    logger.info(f"Starting CDC migration from {source_namespace} to {target_namespace or source_namespace}")
    
    # Validate parameters
    if create_cloudwatch_metrics and not cluster_name:
        raise ValueError("Must supply cluster_name when capturing CloudWatch metrics")
    
    if not use_oplog and not use_change_stream:
        raise ValueError("Must supply either use_oplog=True or use_change_stream=True")
    
    if use_oplog and use_change_stream:
        raise ValueError("Cannot supply both use_oplog=True and use_change_stream=True")
    
    if use_change_stream and start_position == "0":
        raise ValueError("start_position must be supplied as YYYY-MM-DD+HH:MM:SS in UTC or resume token when using change streams")
    
    # Build command
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "cdc-multiprocess.py")
    
    cmd = [
        "python3",
        script_path,
        "--source-uri", source_uri,
        "--target-uri", target_uri,
        "--source-namespace", source_namespace,
        "--start-position", start_position,
        "--threads", str(threads),
        "--duration-seconds", str(duration_seconds),
        "--max-operations-per-batch", str(max_operations_per_batch),
        "--max-seconds-between-batches", str(max_seconds_between_batches),
        "--feedback-seconds", str(feedback_seconds),
    ]
    
    if target_namespace:
        cmd.extend(["--target-namespace", target_namespace])
    
    if use_oplog:
        cmd.append("--use-oplog")
    
    if use_change_stream:
        cmd.append("--use-change-stream")
    
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
        log_file_path = os.path.join(log_dir, f"cdc_{timestamp}.log")
        
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
            "message": f"CDC migration started successfully. Logs are being written to {log_file_path}",
            "process_id": process.pid,
            "command": " ".join(cmd),
            "log_file": log_file_path,
        }
    except Exception as e:
        logger.error(f"Error starting CDC migration: {str(e)}")
        raise ValueError(f"Failed to start CDC migration: {str(e)}")


async def get_resume_token(
    source_uri: Annotated[
        str,
        Field(
            description='Source URI in MongoDB Connection String format'
        ),
    ],
    source_namespace: Annotated[
        str,
        Field(
            description='Source Namespace as <database>.<collection>'
        ),
    ],
) -> Dict[str, Any]:
    """Get the current change stream resume token.
    
    This tool retrieves the current change stream resume token from the source database.
    The resume token can be used as the start_position for a CDC migration.
    
    Returns:
        Dict[str, Any]: The resume token
    """
    logger.info(f"Getting resume token for {source_namespace}")
    
    # Build command
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "cdc-multiprocess.py")
    
    cmd = [
        "python3",
        script_path,
        "--source-uri", source_uri,
        "--target-uri", "mongodb://localhost:27017",  # Dummy target URI, not used
        "--source-namespace", source_namespace,
        "--start-position", "NOW",
        "--get-resume-token",
    ]
    
    # Execute command
    try:
        logger.info(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True
        )
        
        # Parse output to extract resume token
        output = result.stdout
        resume_token = None
        
        for line in output.splitlines():
            if "Change stream resume token is" in line:
                resume_token = line.split("Change stream resume token is")[1].strip()
                break
        
        if resume_token:
            return {
                "success": True,
                "resume_token": resume_token,
            }
        else:
            raise ValueError("Failed to extract resume token from output")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error getting resume token: {e.stderr}")
        raise ValueError(f"Failed to get resume token: {e.stderr}")
    except Exception as e:
        logger.error(f"Error getting resume token: {str(e)}")
        raise ValueError(f"Failed to get resume token: {str(e)}")
