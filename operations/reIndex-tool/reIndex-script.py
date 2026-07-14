import argparse
import sys
import pymongo
import boto3
import json
from collections import OrderedDict


def get_secret(secret_name, region_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        raise Exception(f"Error retrieving secret: {str(e)}")


def ensureDirect(uri, appname):
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
    print('connecting to server')
    client = pymongo.MongoClient(**ensureDirect(appConfig['connectionString'], 'indxrev'))
    getCollectionStats(client, appConfig)
    client.close()


def getCollectionStats(client, appConfig):
    dbDict = client.admin.command("listDatabases", nameOnly=True, filter={"name": {"$nin": ['admin', 'config', 'local', 'system']}})['databases']
    for thisDb in dbDict:
        collCursor = client[thisDb['name']].list_collections()
        for thisColl in collCursor:
            if thisColl.get('type', 'NOT-FOUND') == 'view':
                pass
            elif thisColl['name'] in ['system.profile']:
                pass
            else:
                collStats = client[thisDb['name']].command("collStats", thisColl['name'])
                if 'unusedStorageSize' in collStats and collStats['unusedStorageSize']['unusedBytes'] >= int(appConfig['unusedCollectionSizeMB']) * 1024 * 1024 and collStats['unusedStorageSize']['unusedPercent'] >= int(appConfig['unusedCollectionSizePercent']):
                    indexes = list(client[thisDb['name']][thisColl['name']].list_indexes())
                    for index_name in collStats['indexSizes'].keys():
                        if index_name in ['_id', '_id_']:
                            continue
                        skip_index = False
                        for idx in indexes:
                            if idx['name'] == index_name:
                                if idx.get('partialFilterExpression'):
                                    skip_index = True
                                    break
                                for key, value in idx['key'].items():
                                    if value in ['text', '2d', '2dsphere', 'geoHaystack'] or idx.get('vectorOptions'):
                                        skip_index = True
                                        break
                                break
                        if not skip_index:
                            print('db.runCommand({{ reIndex: "{}", index: "{}" }})'.format(collStats['ns'], index_name))


def main():
    parser = argparse.ArgumentParser(description='Check indexes requiring reindex based on unused size percent')
    
    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')
    
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

    parser.add_argument('--unusedCollectionSizeMB',
                        required=False,
                        type=int,
                        default=0,
                        help='Minimum unused size in MB to consider for reindexing.')

    parser.add_argument('--unusedCollectionSizePercent',
                        required=False,
                        type=int,
                        default=0,
                        help='Minimum unused size in percent to consider for reindexing.')

    args = parser.parse_args()
    
    MIN_PYTHON = (3, 8)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    if args.uri is None and args.secret_name is None:
        parser.error("must provide either --uri or --secret-name")

    appConfig = {}
    
    if args.secret_name:
        secret = get_secret(args.secret_name, args.region)
        appConfig['connectionString'] = secret.get('uri') or secret.get('connectionString')
    else:
        appConfig['connectionString'] = args.uri
    
    appConfig['unusedCollectionSizeMB'] = args.unusedCollectionSizeMB
    appConfig['unusedCollectionSizePercent'] = args.unusedCollectionSizePercent

    getData(appConfig)


if __name__ == "__main__":
    main()
