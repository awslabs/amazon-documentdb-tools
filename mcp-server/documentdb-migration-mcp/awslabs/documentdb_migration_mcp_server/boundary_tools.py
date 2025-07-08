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

"""Boundary generation tools for DocumentDB Migration MCP Server."""

import os
import sys
import time
import pymongo
import warnings
from datetime import datetime
from typing import Annotated, Any, Dict, List, Optional
from pydantic import Field
from loguru import logger


async def generate_boundaries(
    uri: Annotated[
        str,
        Field(
            description='MongoDB Connection String format URI'
        ),
    ],
    database: Annotated[
        str,
        Field(
            description='Database name'
        ),
    ],
    collection: Annotated[
        str,
        Field(
            description='Collection name'
        ),
    ],
    num_segments: Annotated[
        int,
        Field(
            description='Number of segments to divide the collection into'
        ),
    ],
    use_single_cursor: Annotated[
        bool,
        Field(
            description='Use a single cursor to scan the collection (slower but more reliable)'
        ),
    ] = False,
) -> Dict[str, Any]:
    """Generate boundaries for segmenting a collection during migration.
    
    This tool analyzes a collection and generates boundary values that can be used
    to divide the collection into segments for parallel migration. It uses the
    DMS Segment Analyzer approach to find optimal boundaries.
    
    Returns:
        Dict[str, Any]: Generated boundaries and related information
    """
    logger.info(f"Generating boundaries for {database}.{collection} with {num_segments} segments")
    
    # Suppress DocumentDB connection warnings
    warnings.filterwarnings("ignore", "You appear to be connected to a DocumentDB cluster.")
    
    # Check for mixed or unsupported ID types
    supported_id_types = ['int', 'string', 'objectId']
    
    try:
        client = pymongo.MongoClient(host=uri, appname='mcp-boundary-gen')
        db = client[database]
        col = db[collection]
        
        # Check collection stats
        coll_stats = db.command("collStats", collection)
        num_documents = coll_stats['count']
        
        if num_documents == 0:
            return {
                "success": False,
                "message": f"Collection {database}.{collection} is empty",
                "boundaries": [],
                "boundary_datatype": None
            }
        
        # Check ID types
        id_type_first = col.aggregate([
            {"$sort": {"_id": pymongo.ASCENDING}},
            {"$project": {"_id": False, "idType": {"$type": "$_id"}}},
            {"$limit": 1}
        ]).next()['idType']
        
        id_type_last = col.aggregate([
            {"$sort": {"_id": pymongo.DESCENDING}},
            {"$project": {"_id": False, "idType": {"$type": "$_id"}}},
            {"$limit": 1}
        ]).next()['idType']
        
        if id_type_first not in supported_id_types:
            return {
                "success": False,
                "message": f"Unsupported data type '{id_type_first}' for _id field. Only {supported_id_types} are supported.",
                "boundaries": [],
                "boundary_datatype": None
            }
        
        if id_type_last not in supported_id_types:
            return {
                "success": False,
                "message": f"Unsupported data type '{id_type_last}' for _id field. Only {supported_id_types} are supported.",
                "boundaries": [],
                "boundary_datatype": None
            }
        
        if id_type_first != id_type_last:
            return {
                "success": False,
                "message": f"Mixed data types '{id_type_first}' and '{id_type_last}' for _id field.",
                "boundaries": [],
                "boundary_datatype": None
            }
        
        # Map MongoDB type names to our boundary datatypes
        boundary_datatype = "objectid" if id_type_first == "objectId" else id_type_first.lower()
        
        # Generate boundaries
        boundary_list = []
        num_boundaries = num_segments - 1
        docs_per_segment = int(num_documents / num_segments)
        
        logger.info(f"Collection contains {num_documents} documents")
        logger.info(f"Generating {num_boundaries} boundaries with ~{docs_per_segment} documents per segment")
        
        query_start_time = time.time()
        
        if use_single_cursor:
            # Use cursor method (slower but more reliable)
            cursor = col.find(
                filter=None,
                projection={"_id": True},
                sort=[("_id", pymongo.ASCENDING)]
            )
            
            docs_processed = 0
            docs_in_segment = 0
            boundary_count = 0
            
            for doc in cursor:
                docs_processed += 1
                docs_in_segment += 1
                
                if docs_in_segment >= docs_per_segment:
                    docs_in_segment = 0
                    boundary_count += 1
                    boundary_list.append(doc["_id"])
                    logger.info(f"Found boundary {boundary_count}: {doc['_id']}")
                    
                    if boundary_count >= num_boundaries:
                        break
        else:
            # Use skip method (faster but may timeout on large collections)
            # Get the first _id
            first_doc = col.find_one(
                filter=None,
                projection={"_id": True},
                sort=[("_id", pymongo.ASCENDING)]
            )
            
            current_id = first_doc["_id"]
            
            for i in range(num_boundaries):
                current_doc = col.find_one(
                    filter={"_id": {"$gt": current_id}},
                    projection={"_id": True},
                    sort=[("_id", pymongo.ASCENDING)],
                    skip=docs_per_segment
                )
                
                if current_doc is None:
                    # We've reached the end of the collection
                    break
                    
                current_id = current_doc["_id"]
                boundary_list.append(current_id)
                logger.info(f"Found boundary {i+1}: {current_id}")
        
        query_elapsed_secs = int(time.time() - query_start_time)
        logger.info(f"Boundary generation completed in {query_elapsed_secs} seconds")
        
        # Convert boundaries to strings for consistent return format
        boundary_strings = [str(b) for b in boundary_list]
        boundaries_csv = ",".join(boundary_strings)
        
        client.close()
        
        return {
            "success": True,
            "message": f"Successfully generated {len(boundary_list)} boundaries",
            "boundaries": boundary_strings,
            "boundaries_csv": boundaries_csv,
            "boundary_datatype": boundary_datatype,
            "num_documents": num_documents,
            "docs_per_segment": docs_per_segment,
            "execution_time_seconds": query_elapsed_secs
        }
        
    except Exception as e:
        logger.error(f"Error generating boundaries: {str(e)}")
        return {
            "success": False,
            "message": f"Failed to generate boundaries: {str(e)}",
            "boundaries": [],
            "boundary_datatype": None
        }
