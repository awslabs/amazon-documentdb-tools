import pymongo
import time
from math import ceil
import humanize
import argparse
import warnings
import json
import os
import configparser

warnings.filterwarnings("ignore")

def get_partition_count(doc_count):
    """Determine optimal number of partitions for DMS full load based on document count."""
    if doc_count <= 100000:
        return 2
    elif doc_count <= 1000000:
        return 4
    elif doc_count <= 100000000:
        return 8
    else:
        return 16

def get_instance_type(bandwidth_required_mbps):
    """Determine appropriate AWS DMS instance type based on bandwidth requirements."""
    if bandwidth_required_mbps <= 630:
        return "dms.r5.large"
    elif bandwidth_required_mbps <= 2500:
        return "dms.r5.2xlarge"
    elif bandwidth_required_mbps <= 5000:
        return "dms.r5.4xlarge"
    else:
        return "dms.r5.8xlarge"

def calculate_operations_per_second(uri, db_name, collection_name, monitor_minutes=10):
    """Calculate operations per second by monitoring database for specified period."""
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
    db = client[db_name]

    try:
        if collection_name and collection_name not in db.list_collection_names():
            print(f"Warning: Collection '{collection_name}' not found in database '{db_name}'")
            return 0
            
        initial_status = db.command("serverStatus")
        initial_ops = initial_status['opcounters']['insert'] + initial_status['opcounters']['update'] + initial_status['opcounters']['delete']

        print(f"\nMonitoring database operations for {monitor_minutes} minutes...")
        print("Please wait while we collect data...")
        
        monitoring_time = monitor_minutes * 60
        
        for i in range(monitoring_time):
            if i % 60 == 0:
                minutes_left = (monitoring_time - i) // 60
                print(f"{minutes_left} minutes remaining...")
            time.sleep(1)

        final_status = db.command("serverStatus")
        final_ops = final_status['opcounters']['insert'] + final_status['opcounters']['update'] + final_status['opcounters']['delete']

        ops_per_second = (final_ops - initial_ops) / monitoring_time
        return ops_per_second

    except pymongo.errors.ServerSelectionTimeoutError:
        print(f"Error: Could not connect to MongoDB server at {uri}")
        return 0
    except pymongo.errors.OperationFailure as e:
        print(f"Error: Authentication failed or insufficient permissions: {str(e)}")
        return 0
    except Exception as e:
        print(f"Warning: Error calculating operations rate: {str(e)}")
        return 0
    finally:
        client.close()

def calculate_parallel_apply_threads(ops_per_second):
    """Calculate parallel apply threads based on operations per second."""
    threads = ceil(ops_per_second / 250)
    return max(2, threads)

def calculate_storage_size(collection_size_bytes, avg_doc_size, ops_per_second):
    """Calculate required storage size for AWS DMS replication instance."""
    daily_ops = ops_per_second * 86400
    daily_change_bytes = daily_ops * avg_doc_size

    base_storage = collection_size_bytes * 0.5
    required_storage_bytes = base_storage + (daily_change_bytes * 1.2)

    required_storage_gb = ceil(required_storage_bytes / (1024 * 1024 * 1024))
    required_storage_gb = ceil(required_storage_gb / 100) * 100

    if required_storage_gb < 100:
        return 100, daily_change_bytes
    elif required_storage_gb > 1000:
        return 1000, daily_change_bytes
    else:
        return required_storage_gb, daily_change_bytes

def format_change_rate(bytes_per_day):
    """Format change rate in human readable format."""
    if bytes_per_day == 0:
        return "0 B/day"

    sizes = ['B', 'KB', 'MB', 'GB', 'TB']
    scale = 1024

    size = abs(bytes_per_day)
    unit_index = 0
    while size >= scale and unit_index < len(sizes) - 1:
        size /= scale
        unit_index += 1

    return f"{size:.2f} {sizes[unit_index]}/day"

def get_eligible_collections(client, db_name, min_doc_count=10000):
    """Get all collections in database that have document count >= min_doc_count."""
    db = client[db_name]
    eligible_collections = []
    small_collections_count = 0
    
    print(f"\nScanning collections in database '{db_name}' for collections with >= {humanize.intcomma(min_doc_count)} documents...")
    
    collection_names = db.list_collection_names()
    for collection_name in collection_names:
        try:
            stats = db.command("collStats", collection_name)
            doc_count = stats['count']
            
            if doc_count >= min_doc_count:
                collection_size = stats['size']
                avg_doc_size = stats['avgObjSize'] if doc_count > 0 else 0
                
                eligible_collections.append({
                    'name': collection_name,
                    'doc_count': doc_count,
                    'size': collection_size,
                    'avg_doc_size': avg_doc_size
                })
                
                print(f"  ✓ {collection_name}: {humanize.intcomma(doc_count)} documents ({humanize.naturalsize(collection_size)})")
            else:
                small_collections_count += 1
                
        except Exception as e:
            print(f"  ! Error analyzing {collection_name}: {str(e)}")
            continue
    
    if small_collections_count > 0:
        print(f"  Found {small_collections_count} smaller collection(s) that will use default DMS settings")
    
    return eligible_collections

def read_config_file(config_file="dms_buddy.cfg"):
    """Read configuration from file if it exists."""
    config = {}
    if os.path.exists(config_file):
        print(f"Reading configuration from {config_file}")
        parser = configparser.ConfigParser()
        parser.read(config_file)
        
        if 'DMS' in parser:
            dms_section = parser['DMS']
            config_params = [
                'VpcId', 'SubnetIds', 'MultiAZ', 'SourceDBHost', 'SourceDBPort',
                'SourceDatabase', 'SourceUsername', 'SourcePassword',
                'TargetHost', 'TargetPort', 'TargetDatabase', 'TargetUsername',
                'TargetPassword', 'TargetCertificateArn', 'MigrationType', 'CollectionNameForParallelLoad'
            ]
            
            for param in config_params:
                if param in dms_section:
                    config[param] = dms_section[param]
    
    return config

def main():
    parser = argparse.ArgumentParser(
        description="""
DMS Buddy - AWS DMS Configuration Recommender for MongoDB

This tool analyzes your MongoDB collection and provides recommendations for AWS DMS configuration including:
1. Appropriate DMS instance type based on data transfer requirements
2. Required storage size based on current size and operation rate
3. Optimal number of partitions for parallel full load (for Full Load migrations)
4. Number of threads needed for CDC phase (for CDC migrations)

The analysis monitors database operations to calculate the rate of change.
You can specify the monitoring time with --monitor-time (default: 10 minutes).
You can also specify the migration type with --migration-type to get targeted recommendations.

Parameters can also be provided in a dms_buddy.cfg file in the current directory.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument("--source-uri", required=True, help="MongoDB connection URI")
    parser.add_argument("--monitor-time", type=int, default=10, help="Monitoring time in minutes (default: 10)")
    parser.add_argument("--migration-type", choices=["full-load", "cdc", "full-load-and-cdc"], 
                       default="full-load-and-cdc", help="Migration type (default: full-load-and-cdc)")
    parser.add_argument("--vpc-id", help="VPC ID for DMS replication instance")
    parser.add_argument("--subnet-ids", help="Subnet IDs for DMS replication instance (comma-separated)")
    parser.add_argument("--multi-az", choices=["true", "false"], default="false", 
                       help="Whether to use Multi-AZ for DMS replication instance")
    parser.add_argument("--source-host", help="Source database host")
    parser.add_argument("--source-port", default="27017", help="Source database port")
    parser.add_argument("--source-database", help="Source database name to analyze")
    parser.add_argument("--source-username", help="Source database username")
    parser.add_argument("--source-password", help="Source database password")
    parser.add_argument("--target-host", help="Target database host")
    parser.add_argument("--target-port", default="27017", help="Target database port")
    parser.add_argument("--target-database", help="Target database name")
    parser.add_argument("--target-username", help="Target database username")
    parser.add_argument("--target-password", help="Target database password")
    parser.add_argument("--target-certificate-arn", help="Target database SSL certificate ARN for DocumentDB connections")
    parser.add_argument("--collection-name-for-parallel-load", help="Collection name to analyze and use for parallel load")
    parser.add_argument("--engine-version", default="3.5.4", help="AWS DMS engine version (default: 3.5.4)")

    args = parser.parse_args()
    
    config = read_config_file()
    
    migration_type = args.migration_type if args.migration_type != "full-load-and-cdc" else config.get('MigrationType', args.migration_type)
    source_database = args.source_database if args.source_database else config.get('SourceDatabase')
    collection_name = args.collection_name_for_parallel_load if args.collection_name_for_parallel_load else config.get('CollectionNameForParallelLoad')
    
    if not source_database:
        print("Error: Source database name is required. Provide it via --source-database or in dms_buddy.cfg")
        return
    
    analyze_all_collections = not collection_name
    if analyze_all_collections:
        print("No specific collection provided. Will analyze all collections with >= 10,000 documents.")
    
    param_mapping = {
        'VpcId': args.vpc_id if args.vpc_id else config.get('VpcId', ''),
        'SubnetIds': args.subnet_ids if args.subnet_ids else config.get('SubnetIds', ''),
        'MultiAZ': args.multi_az if args.multi_az != 'false' else config.get('MultiAZ', 'false'),
        'SourceDBHost': args.source_host if args.source_host else config.get('SourceDBHost', ''),
        'SourceDBPort': args.source_port if args.source_port != '27017' else config.get('SourceDBPort', '27017'),
        'SourceDatabase': source_database,
        'SourceUsername': args.source_username if args.source_username else config.get('SourceUsername', ''),
        'SourcePassword': args.source_password if args.source_password else config.get('SourcePassword', ''),
        'TargetHost': args.target_host if args.target_host else config.get('TargetHost', ''),
        'TargetPort': args.target_port if args.target_port != '27017' else config.get('TargetPort', '27017'),
        'TargetDatabase': args.target_database if args.target_database else config.get('TargetDatabase', ''),
        'TargetUsername': args.target_username if args.target_username else config.get('TargetUsername', ''),
        'TargetPassword': args.target_password if args.target_password else config.get('TargetPassword', ''),
        'TargetCertificateArn': args.target_certificate_arn if args.target_certificate_arn else config.get('TargetCertificateArn', ''),
        'MigrationType': migration_type,
        'CollectionNameForParallelLoad': collection_name,
        'EngineVersion': args.engine_version if args.engine_version != '3.5.4' else config.get('EngineVersion', '3.5.4')
    }
    
    required_params = ['VpcId', 'SubnetIds', 'SourceDBHost', 'SourceUsername', 'SourcePassword', 
                      'TargetHost', 'TargetDatabase', 'TargetUsername', 'TargetPassword', 'TargetCertificateArn']
    
    missing_params = [param for param in required_params if not param_mapping[param]]
    
    if missing_params:
        print(f"\nError: The following required parameters are missing:")
        for param in missing_params:
            print(f"  - {param}")
        print("\nThese parameters must be provided via command line arguments or in dms_buddy.cfg")
        print("Example:")
        print("  Command line: --vpc-id vpc-12345 --subnet-ids subnet-a,subnet-b --target-certificate-arn arn:aws:dms:...")
        print("  Config file: Add the missing parameters to dms_buddy.cfg")
        return
    
    print("\nStarting DMS configuration analysis...")
    print(f"Migration type: {migration_type.upper()}")
    print(f"This will take approximately {args.monitor_time} minutes to complete.")

    try:
        client = pymongo.MongoClient(args.source_uri, serverSelectionTimeoutMS=5000)
        db = client[source_database]
        
        if source_database not in client.list_database_names():
            print(f"Error: Database '{source_database}' not found")
            return
        
        if analyze_all_collections:
            eligible_collections = get_eligible_collections(client, source_database)
            
            if not eligible_collections:
                print(f"\nNo collections found with >= 10,000 documents in database '{source_database}'")
                print("Will generate empty table settings and use default values for DMS recommendations.")
                
                collection_name = "no-eligible-collections"
                doc_count = 0
                collection_size = 0
                avg_doc_size = 0
                eligible_collections = []
            else:
                print(f"\nFound {len(eligible_collections)} large collection(s) that will get optimized parallel processing.")
            
        else:
            if collection_name not in db.list_collection_names():
                print(f"Error: Collection '{collection_name}' not found in database '{source_database}'")
                return
                
            stats = db.command("collStats", collection_name)
            doc_count = stats['count']
            collection_size = stats['size']
            avg_doc_size = stats['avgObjSize'] if doc_count > 0 else 0
            eligible_collections = [{'name': collection_name, 'doc_count': doc_count, 'size': collection_size, 'avg_doc_size': avg_doc_size}]

        ops_per_second = 0
        if migration_type != "full-load":
            print("\nMonitoring database-level operations for CDC configuration...")
            ops_per_second = calculate_operations_per_second(args.source_uri, source_database, None, args.monitor_time)
        else:
            print("\nSkipping operations monitoring for FULL-LOAD migration type...")

        if eligible_collections:
            print(f"\nCalculating DMS recommendations based on all {len(eligible_collections)} collection(s)...")
            
            min_storage_size = 100
            min_bandwidth_mbps = 0
            min_partitions = 2
            
            for coll in eligible_collections:
                coll_partitions = get_partition_count(coll['doc_count'])
                coll_storage, _ = calculate_storage_size(coll['size'], coll['avg_doc_size'], ops_per_second)
                coll_bandwidth = (coll['avg_doc_size'] * 10000 * coll_partitions * 8) / (1024 * 1024)
                
                max_storage_size = max(min_storage_size, coll_storage)
                max_bandwidth_mbps = max(min_bandwidth_mbps, coll_bandwidth)
                max_partitions = max(min_partitions, coll_partitions)
                
                print(f"  {coll['name']}: {coll_partitions} partitions, {coll_storage} GB storage, {round(coll_bandwidth, 2)} Mbps")
            
            partitions = max_partitions
            storage_size = max_storage_size
            bandwidth_required_mbps = max_bandwidth_mbps
            parallel_threads = calculate_parallel_apply_threads(ops_per_second)
            
        else:
            partitions = get_partition_count(doc_count)
            storage_size, _ = calculate_storage_size(collection_size, avg_doc_size, ops_per_second)
            parallel_threads = calculate_parallel_apply_threads(ops_per_second)
            bandwidth_required_mbps = (avg_doc_size * 10000 * partitions * 8) / (1024 * 1024)

        instance_type = get_instance_type(bandwidth_required_mbps)

        print("\nDMS Configuration Recommendations:")
        print("---------------------------------")
        print(f"1. DMS Instance Type: {instance_type}")
        print(f"2. DMS Storage Size: {storage_size} GB")
        
        # Only show parallel apply threads for CDC-related migration types
        if migration_type == "cdc" or migration_type == "full-load-and-cdc":
            print(f"3. Parallel Apply Threads: {parallel_threads} (for CDC)")

        table_settings_json_parts = []
        rule_id = 10
        
        print(f"\nGenerating optimized collection settings for {len(eligible_collections)} large collection(s):")
        for coll in eligible_collections:
            coll_partitions = get_partition_count(coll['doc_count'])
            table_setting = {
                "rule-type": "table-settings",
                "rule-id": str(rule_id),
                "rule-name": str(rule_id),
                "rule-action": "include",
                "filters": [],
                "object-locator": {
                    "schema-name": source_database,
                    "table-name": coll['name']
                },
                "parallel-load": {
                    "number-of-partitions": coll_partitions,
                    "type": "partitions-auto"
                }
            }
            table_settings_json_parts.append(json.dumps(table_setting))
            print(f"  ✓ {coll['name']}: {coll_partitions} partitions ({humanize.intcomma(coll['doc_count'])} docs)")
            rule_id += 1
        
        table_settings_string = ",".join(table_settings_json_parts)
        
        parameters = []
        parameters.append({"ParameterKey": "ReplicationInstanceClass", "ParameterValue": instance_type})
        parameters.append({"ParameterKey": "AllocatedStorage", "ParameterValue": str(storage_size)})
        parameters.append({"ParameterKey": "NumberOfPartitions", "ParameterValue": str(partitions)})
        parameters.append({"ParameterKey": "ParallelApplyThreads", "ParameterValue": str(parallel_threads)})
        
        if partitions > 8:
            parameters.append({"ParameterKey": "MaxFullLoadSubTasks", "ParameterValue": str(partitions)})
        
        parameters.append({"ParameterKey": "TableSettings", "ParameterValue": table_settings_string})
        
        # Set CollectionNameForParallelLoad based on analysis mode
        if analyze_all_collections:
            # For all collections analysis, use % wildcard
            param_mapping['CollectionNameForParallelLoad'] = "%"
        else:
            # For single collection analysis, use the specific collection name
            param_mapping['CollectionNameForParallelLoad'] = collection_name if collection_name else ""
        
        for key, value in param_mapping.items():
            parameters.append({"ParameterKey": key, "ParameterValue": value})
        
        with open('parameter.json', 'w') as f:
            json.dump(parameters, f, indent=2)
            
        print(f"\nParameters written to parameter.json")
        print(f"Table settings generated for {len(eligible_collections)} collection(s)")
        
        print(f"\nMigration Summary:")
        if analyze_all_collections:
            # Summary for all collections analysis
            total_collections = len(db.list_collection_names())
            print(f"- Total collections to be migrated: {total_collections}")
            print(f"- Collections with optimized parallel settings: {len(eligible_collections)}")
            print(f"- Collections using default settings: {total_collections - len(eligible_collections)}")
            print(f"\nNote: ALL collections in the database will be migrated. Large collections (≥10K docs)")
            print(f"get optimized parallel processing, while smaller collections use efficient default settings.")
        else:
            # Summary for single collection analysis
            if eligible_collections:
                coll = eligible_collections[0]
                coll_partitions = get_partition_count(coll['doc_count'])
                print(f"- Collection to be migrated: {collection_name}")
                print(f"- Document count: {humanize.intcomma(coll['doc_count'])}")
                print(f"- Collection size: {humanize.naturalsize(coll['size'])}")
                print(f"- Parallel load partitions: {coll_partitions}")
                print(f"\nNote: Only the specified collection '{collection_name}' will be migrated with optimized settings.")
            else:
                print(f"- Collection to be migrated: {collection_name}")
                print(f"- Collection will use default DMS settings")
                print(f"\nNote: Only the specified collection '{collection_name}' will be migrated.")

    except pymongo.errors.ServerSelectionTimeoutError:
        print(f"Error: Could not connect to MongoDB server at {args.source_uri}")
        print("Please check that the server is running and the URI is correct")
    except pymongo.errors.OperationFailure as e:
        print(f"Error: Authentication failed or insufficient permissions: {str(e)}")
    except pymongo.errors.ConfigurationError as e:
        print(f"Error: Invalid MongoDB URI format: {str(e)}")
    except Exception as e:
        print(f"\nError: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main()
