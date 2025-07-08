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

"""Index tools for DocumentDB Migration MCP Server."""

import os
import sys
import time
import subprocess
import json
import tempfile
from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional
from pydantic import Field
from loguru import logger


async def dump_indexes(
    uri: Annotated[
        str,
        Field(
            description='URI to connect to MongoDB or Amazon DocumentDB'
        ),
    ],
    output_dir: Annotated[
        str,
        Field(
            description='Directory to export indexes to'
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        Field(
            description='Perform processing, but do not actually export indexes'
        ),
    ] = False,
    debug: Annotated[
        bool,
        Field(
            description='Output debugging information'
        ),
    ] = False,
) -> Dict[str, Any]:
    """Dump indexes from a MongoDB or DocumentDB instance.
    
    This tool exports indexes metadata from a running MongoDB or Amazon DocumentDB deployment.
    
    Returns:
        Dict[str, Any]: Status of the index dump operation
    """
    logger.info(f"Starting index dump from {uri}")
    
    # Create a temporary directory if output_dir is not provided
    if not output_dir:
        output_dir = tempfile.mkdtemp(prefix="index_dump_")
        logger.info(f"Created temporary directory for index dump: {output_dir}")
    
    # Build command
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "documentdb_index_tool.py")
    
    cmd = [
        "python3",
        script_path,
        "--dump-indexes",
        "--dir", output_dir,
        "--uri", uri,
    ]
    
    if dry_run:
        cmd.append("--dry-run")
    
    if debug:
        cmd.append("--debug")
    
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
        
        return {
            "success": True,
            "message": "Index dump completed successfully",
            "output_dir": output_dir,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.CalledProcessError as e:
        logger.error(f"Error dumping indexes: {e.stderr}")
        return {
            "success": False,
            "message": f"Failed to dump indexes: {e.stderr}",
            "output_dir": output_dir,
            "stdout": e.stdout,
            "stderr": e.stderr,
        }
    except Exception as e:
        logger.error(f"Error dumping indexes: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to dump indexes: {str(e)}",
            "output_dir": output_dir,
        }


async def restore_indexes(
    uri: Annotated[
        str,
        Field(
            description='URI to connect to Amazon DocumentDB'
        ),
    ],
    index_dir: Annotated[
        str,
        Field(
            description='Directory containing index metadata to restore from'
        ),
    ],
    skip_incompatible: Annotated[
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
    dry_run: Annotated[
        bool,
        Field(
            description='Perform processing, but do not actually restore indexes'
        ),
    ] = False,
    debug: Annotated[
        bool,
        Field(
            description='Output debugging information'
        ),
    ] = False,
    shorten_index_name: Annotated[
        bool,
        Field(
            description='Shorten long index name to compatible length'
        ),
    ] = True,
    skip_id_indexes: Annotated[
        bool,
        Field(
            description='Do not create _id indexes'
        ),
    ] = True,
) -> Dict[str, Any]:
    """Restore indexes to an Amazon DocumentDB instance.
    
    This tool restores indexes from metadata to an Amazon DocumentDB instance.
    
    Returns:
        Dict[str, Any]: Status of the index restore operation
    """
    logger.info(f"Starting index restore to {uri}")
    
    # Build command
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "documentdb_index_tool.py")
    
    cmd = [
        "python3",
        script_path,
        "--restore-indexes",
        "--dir", index_dir,
        "--uri", uri,
    ]
    
    if skip_incompatible:
        cmd.append("--skip-incompatible")
    
    if support_2dsphere:
        cmd.append("--support-2dsphere")
    
    if dry_run:
        cmd.append("--dry-run")
    
    if debug:
        cmd.append("--debug")
    
    if shorten_index_name:
        cmd.append("--shorten-index-name")
    
    if skip_id_indexes:
        cmd.append("--skip-id-indexes")
    
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
        
        return {
            "success": True,
            "message": "Index restore completed successfully",
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.CalledProcessError as e:
        logger.error(f"Error restoring indexes: {e.stderr}")
        return {
            "success": False,
            "message": f"Failed to restore indexes: {e.stderr}",
            "stdout": e.stdout,
            "stderr": e.stderr,
        }
    except Exception as e:
        logger.error(f"Error restoring indexes: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to restore indexes: {str(e)}",
        }


async def show_compatibility_issues(
    index_dir: Annotated[
        str,
        Field(
            description='Directory containing index metadata to check'
        ),
    ],
    debug: Annotated[
        bool,
        Field(
            description='Output debugging information'
        ),
    ] = False,
) -> Dict[str, Any]:
    """Show compatibility issues with Amazon DocumentDB.
    
    This tool checks index metadata for compatibility issues with Amazon DocumentDB.
    
    Returns:
        Dict[str, Any]: Compatibility issues found
    """
    logger.info(f"Checking compatibility issues in {index_dir}")
    
    # Build command
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "documentdb_index_tool.py")
    
    cmd = [
        "python3",
        script_path,
        "--show-issues",
        "--dir", index_dir,
    ]
    
    if debug:
        cmd.append("--debug")
    
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
        
        # Try to parse the JSON output
        try:
            issues = json.loads(result.stdout)
        except json.JSONDecodeError:
            issues = {"raw_output": result.stdout}
        
        return {
            "success": True,
            "message": "Compatibility check completed successfully",
            "issues": issues,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.CalledProcessError as e:
        logger.error(f"Error checking compatibility: {e.stderr}")
        return {
            "success": False,
            "message": f"Failed to check compatibility: {e.stderr}",
            "stdout": e.stdout,
            "stderr": e.stderr,
        }
    except Exception as e:
        logger.error(f"Error checking compatibility: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to check compatibility: {str(e)}",
        }


async def show_compatible_indexes(
    index_dir: Annotated[
        str,
        Field(
            description='Directory containing index metadata to check'
        ),
    ],
    debug: Annotated[
        bool,
        Field(
            description='Output debugging information'
        ),
    ] = False,
) -> Dict[str, Any]:
    """Show compatible indexes with Amazon DocumentDB.
    
    This tool shows all indexes that are compatible with Amazon DocumentDB.
    
    Returns:
        Dict[str, Any]: Compatible indexes
    """
    logger.info(f"Checking compatible indexes in {index_dir}")
    
    # Build command
    script_path = os.path.join(os.path.dirname(__file__), "scripts", "documentdb_index_tool.py")
    
    cmd = [
        "python3",
        script_path,
        "--show-compatible",
        "--dir", index_dir,
    ]
    
    if debug:
        cmd.append("--debug")
    
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
        
        # Try to parse the JSON output
        try:
            compatible_indexes = json.loads(result.stdout)
        except json.JSONDecodeError:
            compatible_indexes = {"raw_output": result.stdout}
        
        return {
            "success": True,
            "message": "Compatible indexes check completed successfully",
            "compatible_indexes": compatible_indexes,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except subprocess.CalledProcessError as e:
        logger.error(f"Error checking compatible indexes: {e.stderr}")
        return {
            "success": False,
            "message": f"Failed to check compatible indexes: {e.stderr}",
            "stdout": e.stdout,
            "stderr": e.stderr,
        }
    except Exception as e:
        logger.error(f"Error checking compatible indexes: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to check compatible indexes: {str(e)}",
        }
