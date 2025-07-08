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

"""AWS Labs DocumentDB Migration MCP Server implementation for migrating data to AWS DocumentDB."""

import argparse
import os
import sys
from awslabs.documentdb_migration_mcp_server.full_load_tools import run_full_load, run_filtered_full_load
from awslabs.documentdb_migration_mcp_server.cdc_tools import run_cdc, get_resume_token
from awslabs.documentdb_migration_mcp_server.boundary_tools import generate_boundaries
from awslabs.documentdb_migration_mcp_server.index_tools import (
    dump_indexes, restore_indexes, show_compatibility_issues, show_compatible_indexes
)
from loguru import logger
from mcp.server.fastmcp import FastMCP


# Create the FastMCP server
mcp = FastMCP(
    'awslabs.documentdb-migration-mcp-server',
    instructions="""DocumentDB Migration MCP Server provides tools to migrate data to AWS DocumentDB.

    Usage pattern:
    1. For full load migrations, use the `runFullLoad` or `runFilteredFullLoad` tools
       - Boundaries will be auto-generated if not provided
    2. For CDC (Change Data Capture) migrations, use the `runCDC` tool
    3. To get a change stream resume token for CDC, use the `getResumeToken` tool
    4. To generate boundaries for segmenting collections, use the `generateBoundaries` tool
    5. For index management:
       - To dump indexes from a source database, use the `dumpIndexes` tool
       - To restore indexes to a target database, use the `restoreIndexes` tool
       - To check index compatibility with DocumentDB, use the `showIndexCompatibilityIssues` tool
       - To show compatible indexes, use the `showCompatibleIndexes` tool

    Server Configuration:
    - The server requires access to the migration scripts in the scripts directory.""",
    dependencies=[
        'pydantic',
        'loguru',
        'pymongo',
        'boto3',
    ],
)


# Register all tools

# Full Load tools
mcp.tool(name='runFullLoad')(run_full_load)
mcp.tool(name='runFilteredFullLoad')(run_filtered_full_load)

# CDC tools
mcp.tool(name='runCDC')(run_cdc)
mcp.tool(name='getResumeToken')(get_resume_token)

# Boundary tools
mcp.tool(name='generateBoundaries')(generate_boundaries)

# Index tools
mcp.tool(name='dumpIndexes')(dump_indexes)
mcp.tool(name='restoreIndexes')(restore_indexes)
mcp.tool(name='showIndexCompatibilityIssues')(show_compatibility_issues)
mcp.tool(name='showCompatibleIndexes')(show_compatible_indexes)


def main():
    """Run the MCP server with CLI argument support."""
    parser = argparse.ArgumentParser(
        description='An AWS Labs Model Context Protocol (MCP) server for DocumentDB Migration'
    )
    parser.add_argument('--sse', action='store_true', help='Use SSE transport')
    parser.add_argument('--port', type=int, default=8889, help='Port to run the server on')
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Host to bind the server to')
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['TRACE', 'DEBUG', 'INFO', 'SUCCESS', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the logging level',
    )
    parser.add_argument(
        '--scripts-dir',
        type=str,
        default=None,
        help='Directory containing the migration scripts (default: scripts subdirectory)',
    )
    parser.add_argument(
        '--aws-profile',
        type=str,
        default=None,
        help='AWS profile to use for AWS services including DocumentDB and CloudWatch',
    )
    parser.add_argument(
        '--aws-region',
        type=str,
        default=None,
        help='AWS region to use for AWS services including DocumentDB and CloudWatch',
    )

    args = parser.parse_args()

    # Configure logging
    logger.remove()
    logger.add(
        lambda msg: print(msg),
        level=args.log_level,
        format='<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>',
    )

    logger.info(f'Starting DocumentDB Migration MCP Server on {args.host}:{args.port}')
    logger.info(f'Log level: {args.log_level}')

    # Set up scripts directory
    if args.scripts_dir:
        scripts_dir = args.scripts_dir
    else:
        scripts_dir = os.path.join(os.path.dirname(__file__), "scripts")
    
    # Create scripts directory if it doesn't exist
    os.makedirs(scripts_dir, exist_ok=True)
    logger.info(f'Scripts directory: {scripts_dir}')

    # Set AWS profile and region if provided
    if args.aws_profile:
        os.environ['AWS_PROFILE'] = args.aws_profile
        logger.info(f'Using AWS profile: {args.aws_profile}')
    
    if args.aws_region:
        os.environ['AWS_REGION'] = args.aws_region
        logger.info(f'Using AWS region: {args.aws_region}')

    try:
        # Run server with appropriate transport
        if args.sse:
            mcp.settings.port = args.port
            mcp.settings.host = args.host
            mcp.run(transport='sse')
        else:
            mcp.settings.port = args.port
            mcp.settings.host = args.host
            mcp.run()
    except Exception as e:
        logger.critical(f'Failed to start server: {str(e)}')


if __name__ == '__main__':
    main()
