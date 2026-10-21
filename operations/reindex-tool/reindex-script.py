import argparse
import pymongo
import json


def get_secret(secret_name, region_name):
    """Retrieve MongoDB connection URI from AWS Secrets Manager.
    
    boto3 is imported here lazily so it's only required when using --secret-name.
    """
    try:
        import boto3
    except ImportError:
        raise ImportError("boto3 is required when using --secret-name. Install it with: pip install boto3")

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        raise Exception(f"Error retrieving secret: {str(e)}")


def ensureDirect(uri, appname):
    """Parse the MongoDB URI and build a direct connection config.

    Strips replicaset and readpreference options to force a direct connection
    to the primary node, which is required for reIndex commands.
    """
    connInfo = {}
    parsedUri = pymongo.uri_parser.parse_uri(uri)

    for thisKey in sorted(parsedUri['options'].keys()):
        if thisKey.lower() not in ['replicaset', 'readpreference']:
            connInfo[thisKey] = parsedUri['options'][thisKey]

    connInfo['directconnection'] = True
    connInfo['username'] = parsedUri['username']
    connInfo['password'] = parsedUri['password']
    connInfo['host'] = parsedUri['nodelist'][0][0]
    connInfo['port'] = parsedUri['nodelist'][0][1]
    connInfo['appname'] = appname

    if parsedUri.get('database') is not None:
        connInfo['authSource'] = parsedUri['database']

    return connInfo


def getData(appConfig):
    """Connect to DocumentDB and scan collections for bloated indexes."""
    print('connecting to server')
    try:
        client = pymongo.MongoClient(**ensureDirect(appConfig['connectionString'], 'indxrev'))
        # Force a connection to verify the server is reachable
        client.admin.command('ping')
    except pymongo.errors.ConnectionFailure as e:
        raise SystemExit(f"Error: unable to connect to server — {e}")
    except pymongo.errors.OperationFailure as e:
        raise SystemExit(f"Error: authentication failed — {e}")
    except Exception as e:
        raise SystemExit(f"Error: failed to connect — {e}")

    getCollectionStats(client, appConfig)
    client.close()


def getCollectionStats(client, appConfig):
    """Iterate all user databases and collections, printing reIndex commands
    for every eligible index.
    """
    output_file = appConfig.get('outputFile')
    results = []

    minUnusedPercent = appConfig['minUnusedPercent']

    # Exclude system databases
    dbDict = client.admin.command("listDatabases", nameOnly=True, filter={"name": {"$nin": ['admin', 'config', 'local', 'system']}})['databases']
    for thisDb in dbDict:
        dbName = thisDb['name']
        collCursor = client[dbName].list_collections()
        # Commands for this database, collected so we only emit a `use <db>`
        # header when the database actually has at least one bloated index.
        dbCommands = []
        for thisColl in collCursor:
            collName = thisColl['name']
            # Skip views and system collections
            if thisColl.get('type', 'NOT-FOUND') == 'view':
                continue
            if collName in ['system.profile']:
                continue

            # Index key definitions, used to skip index types reIndex can't rebuild.
            indexes = {idx['name']: idx for idx in client[dbName][collName].list_indexes()}

            # $indexStats reports per-index unused (reclaimable) storage, which is
            # the bloat. On DocumentDB 5.0+ each entry has an unusedStorageSize
            # block; older engines omit it, in which case we can't measure bloat.
            try:
                indexStats = list(client[dbName][collName].aggregate([{'$indexStats': {}}]))
            except Exception as e:
                print(f'  warning: could not read $indexStats for {dbName}.{collName}: {e}')
                continue

            for stat in indexStats:
                index_name = stat.get('name')
                idx = indexes.get(index_name, {})

                # Skip partial indexes — reIndex is not supported for them
                if idx.get('partialFilterExpression'):
                    continue
                # Skip text, geospatial, and vector indexes — reIndex is not supported for them
                skip_index = False
                for key, value in idx.get('key', {}).items():
                    if value in ['text', '2d', '2dsphere', 'geoHaystack'] or idx.get('vectorOptions'):
                        skip_index = True
                        break
                if skip_index:
                    continue

                # Decide based on measured bloat. Skip indexes with no
                # unusedStorageSize reported (pre-5.0 engines) since we can't
                # tell whether reindexing would reclaim anything.
                unused = stat.get('unusedStorageSize')
                if not unused:
                    print(f'  skipping {dbName}.{collName} index "{index_name}": no unusedStorageSize reported (requires DocumentDB 5.0+)')
                    continue

                unusedPercent = unused.get('unusedSizePercent', 0)
                unusedBytes = unused.get('unusedSizeBytes', 0)

                if unusedPercent < minUnusedPercent:
                    continue

                # DocumentDB's reIndex takes the bare collection name and
                # operates on the current database, so emit `use <db>`
                # once per database followed by collection-only commands.
                comment = '// {}.{} index "{}" — unused {}% ({} bytes)'.format(
                    dbName, collName, index_name, unusedPercent, unusedBytes)
                cmd = 'db.runCommand({{ reIndex: "{}", index: "{}", workers: {} }})'.format(
                    collName, index_name, appConfig['workers'])
                dbCommands.append(comment)
                dbCommands.append(cmd)

        if dbCommands:
            useStmt = 'use {}'.format(dbName)
            results.append(useStmt)
            print(useStmt)
            for cmd in dbCommands:
                results.append(cmd)
                print(cmd)

    # Write results to file if --output-file was specified
    if output_file and results:
        with open(output_file, 'w') as f:
            f.write('\n'.join(results) + '\n')
        print(f'\nResults written to {output_file}')
    elif output_file and not results:
        print('\nNo bloated indexes found. No output file written.')


def main():
    parser = argparse.ArgumentParser(description='Generate reIndex commands for all eligible indexes')
    
    parser.add_argument('--uri',
                        required=False,
                        type=str,
                        help='MongoDB Connection URI')
    
    parser.add_argument('--secret-name',
                        required=False,
                        type=str,
                        help='AWS Secrets Manager secret name containing MongoDB URI')
    
    parser.add_argument('--region',
                        required=False,
                        type=str,
                        default='us-east-1',
                        help='AWS region for Secrets Manager')

    parser.add_argument('--workers',
                        required=False,
                        type=int,
                        default=2,
                        help='Number of workers for reindex operation. Defaults to 2.')

    parser.add_argument('--output-file',
                        required=False,
                        type=str,
                        help='Path to write the reindex commands to a file.')

    parser.add_argument('--min-unused-percent',
                        required=False,
                        type=float,
                        default=20.0,
                        help='Only generate a reIndex command for an index whose unused (reclaimable) '
                             'storage is at or above this percent, as reported by $indexStats. '
                             'Defaults to 20. Set to 0 to include every eligible index. '
                             'Requires DocumentDB 5.0+.')

    args = parser.parse_args()
    if args.uri is None and args.secret_name is None:
        parser.error("must provide either --uri or --secret-name")

    appConfig = {}
    
    if args.secret_name:
        secret = get_secret(args.secret_name, args.region)
        appConfig['connectionString'] = secret.get('uri') or secret.get('connectionString')
    else:
        appConfig['connectionString'] = args.uri

    appConfig['workers'] = args.workers
    appConfig['outputFile'] = args.output_file
    appConfig['minUnusedPercent'] = args.min_unused_percent

    getData(appConfig)


if __name__ == "__main__":
    main()
