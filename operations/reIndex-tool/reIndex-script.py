import argparse
import pymongo
import boto3
import json


def get_secret(secret_name, region_name):
    """Retrieve MongoDB connection URI from AWS Secrets Manager."""
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
    client = pymongo.MongoClient(**ensureDirect(appConfig['connectionString'], 'indxrev'))
    getCollectionStats(client, appConfig)
    client.close()


def getCollectionStats(client, appConfig):
    """Iterate all user databases and collections, printing reIndex commands
    for indexes that exceed the unused storage percent threshold.
    """
    # Exclude system databases
    dbDict = client.admin.command("listDatabases", nameOnly=True, filter={"name": {"$nin": ['admin', 'config', 'local', 'system']}})['databases']
    for thisDb in dbDict:
        collCursor = client[thisDb['name']].list_collections()
        for thisColl in collCursor:
            # Skip views and system collections
            if thisColl.get('type', 'NOT-FOUND') == 'view':
                pass
            elif thisColl['name'] in ['system.profile']:
                pass
            else:
                collStats = client[thisDb['name']].command("collStats", thisColl['name'])
                # Check if collection bloat exceeds the threshold
                if 'unusedStorageSize' in collStats and collStats['unusedStorageSize']['unusedPercent'] >= int(appConfig['unusedCollectionSizePercent']):
                    indexes = list(client[thisDb['name']][thisColl['name']].list_indexes())
                    for index_name in collStats['indexSizes'].keys():
                        skip_index = False
                        for idx in indexes:
                            if idx['name'] == index_name:
                                # Skip partial indexes — reIndex is not supported for them
                                if idx.get('partialFilterExpression'):
                                    skip_index = True
                                    break
                                # Skip text, geospatial, and vector indexes — reIndex is not supported for them
                                for key, value in idx['key'].items():
                                    if value in ['text', '2d', '2dsphere', 'geoHaystack'] or idx.get('vectorOptions'):
                                        skip_index = True
                                        break
                                break
                        if not skip_index:
                            print('db.runCommand({{ reIndex: "{}", index: "{}", workers: {} }})'.format(collStats['ns'], index_name, appConfig['workers']))


def main():
    parser = argparse.ArgumentParser(description='Check indexes requiring reindex based on unused size percent')
    
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

    parser.add_argument('--unusedCollectionSizePercent',
                        required=False,
                        type=int,
                        default=0,
                        help='Minimum unused size in percent to consider for reindexing.')

    parser.add_argument('--workers',
                        required=False,
                        type=int,
                        default=2,
                        help='Number of workers for reindex operation. Defaults to 2.')

    args = parser.parse_args()
    if args.uri is None and args.secret_name is None:
        parser.error("must provide either --uri or --secret-name")

    appConfig = {}
    
    if args.secret_name:
        secret = get_secret(args.secret_name, args.region)
        appConfig['connectionString'] = secret.get('uri') or secret.get('connectionString')
    else:
        appConfig['connectionString'] = args.uri
    
    appConfig['unusedCollectionSizePercent'] = args.unusedCollectionSizePercent
    appConfig['workers'] = args.workers

    getData(appConfig)


if __name__ == "__main__":
    main()
