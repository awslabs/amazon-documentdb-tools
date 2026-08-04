import os
import logging
from functools import lru_cache
from urllib.parse import quote

import boto3

logger = logging.getLogger(__name__)

# Amazon RDS/DocumentDB CA bundle, fetched into the app root by the setup scripts.
_CA_BUNDLE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "global-bundle.pem")


def _tls_ca_param():
    """Return the '&tlsCAFile=<bundle>' fragment for a direct-mode TLS connection.

    Direct mode connects to the real cluster/instance hostname, so certificate
    hostname validation works normally — we only need to supply the Amazon CA
    bundle so pymongo can build the trust chain (the system trust store does not
    include the DocumentDB CA). Fails closed with a warning if the bundle is
    missing so the resulting handshake failure is diagnosable.
    """
    if not os.path.isfile(_CA_BUNDLE):
        logger.warning("TLS requested but CA bundle not found at %s — connection will "
                       "fail certificate validation until it is present (run setup to "
                       "fetch global-bundle.pem)", _CA_BUNDLE)
    return f"&tlsCAFile={quote(_CA_BUNDLE, safe='')}"


@lru_cache(maxsize=32)
def discover_documentdb_clusters(region='us-east-1'):
    """Discover all DocumentDB clusters and their instances in the region."""
    try:
        client = boto3.client('docdb', region_name=region)
        clusters_resp = client.describe_db_clusters()
        # Fetch all instances once — cheaper than one call per cluster
        try:
            all_insts = client.describe_db_instances()["DBInstances"]
        except Exception:
            all_insts = []

        clusters = []
        for cluster in clusters_resp['DBClusters']:
            if cluster['Engine'] != 'docdb':
                continue
            cid = cluster['DBClusterIdentifier']
            members = {m['DBInstanceIdentifier']: m.get('IsClusterWriter', False)
                       for m in cluster.get('DBClusterMembers', [])}

            instances = []
            for inst in all_insts:
                iid = inst['DBInstanceIdentifier']
                if iid not in members:
                    continue
                ep = inst.get('Endpoint', {})
                instances.append({
                    'id':       iid,
                    'endpoint': ep.get('Address', ''),
                    'port':     ep.get('Port', 27017),
                    'role':     'Writer' if members[iid] else 'Reader',
                    'type':     inst.get('DBInstanceClass', ''),
                })
            # Writer first
            instances.sort(key=lambda i: 0 if i['role'] == 'Writer' else 1)

            clusters.append({
                'cluster_id':     cid,
                'endpoint':       cluster['Endpoint'],
                'port':           cluster['Port'],
                'status':         cluster['Status'],
                'engine_version': cluster['EngineVersion'],
                'vpc_id':         cluster.get('DbClusterResourceId', 'N/A'),
                'log_group':      f"/aws/docdb/{cid}/profiler",
                'instances':      instances,
            })
        return clusters
    except Exception:
        return []


def get_cluster_databases(connection_string):
    """Get list of databases in a cluster"""
    try:
        import pymongo
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        databases = client.list_database_names()
        client.close()
        return [db for db in databases if db not in ['admin', 'local']]
    except Exception:
        return []


def build_connection_string(cluster, username, password, use_ssl=True):
    """Build DocumentDB connection string from cluster info and credentials.

    Uses directConnection=true (single-node topology) rather than
    replicaSet=rs0. Prism is a read-only analysis tool, and pymongo pins
    replica-set discovery operations (listDatabases, listCollections, generic
    admin commands, ping) to the PRIMARY regardless of readPreference. If the
    cluster has no primary — e.g. an all-secondary state during/after a
    failover — those operations fail with 'No replica set members match
    selector "Primary()"' even though the secondaries are readable.

    directConnection=true removes the primary-election requirement entirely:
    the client talks to whichever node the cluster endpoint resolves to (the
    primary when one exists, otherwise a reachable secondary) and every read
    works against it. This mirrors SSH-tunnel mode, which already connects with
    directConnection=true. Per-instance reader offload is unaffected — those
    builders (index_usage_cluster / current_activity) construct their own
    directConnection strings per node.
    """
    if use_ssl:
        return (f"mongodb://{username}:{password}@{cluster['endpoint']}:{cluster['port']}/"
                f"?tls=true{_tls_ca_param()}&directConnection=true"
                f"&retryWrites=false&appName=DocDB-Prism")
    else:
        return (f"mongodb://{username}:{password}@{cluster['endpoint']}:{cluster['port']}/"
                f"?directConnection=true&retryWrites=false&appName=DocDB-Prism")


def get_aws_regions():
    """Get list of AWS regions that support DocumentDB"""
    return [
        'us-east-1', 'us-east-2', 'us-west-2',
        'eu-west-1', 'eu-west-2', 'eu-central-1',
        'ap-southeast-1', 'ap-southeast-2', 'ap-northeast-1'
    ]
