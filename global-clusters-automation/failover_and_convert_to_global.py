import boto3
from botocore.exceptions import ClientError
from datetime import datetime

session = boto3.Session()
client = session.client('docdb')
now = datetime.now()
dt_string = now.strftime("%H%M%S")


# Retrieve all cluster members for the global cluster
def get_global_cluster_members(global_cluster_id):
    try:
        response = client.describe_global_clusters(
            GlobalClusterIdentifier=global_cluster_id
        )
        global_cluster_members = response['GlobalClusters'][0]['GlobalClusterMembers']
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError
    return global_cluster_members


def prepare_to_convert(global_cluster_members, global_cluster_id, secondary_cluster_arn, io_optimized_storage, enable_performance_insights):
    try:
        # populate the list of clusters in the global cluster and remove the secondary cluster to be promoted from
        # the list
        regional_clusters = get_regional_clusters(global_cluster_members)
        for each_cluster in regional_clusters:
            if each_cluster == secondary_cluster_arn:
                new_primary_cluster_arn = each_cluster
                regional_clusters.remove(each_cluster)
                break

        secondary_clusters = []
        for each_cluster in regional_clusters:
            cluster_details = get_cluster_details(each_cluster)
            if io_optimized_storage:
                cluster_details["StorageType"] = "iopt1"
            secondary_clusters.append(cluster_details)

        convert_to_global_request = {
            "global_cluster_id": global_cluster_id,
            "primary_cluster_arn": new_primary_cluster_arn,
            "secondary_clusters": secondary_clusters,
            "io_optimized_storage": io_optimized_storage,
            "enable_performance_insights": enable_performance_insights
        }
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError
    return convert_to_global_request


def get_regional_clusters(global_cluster_members):
    try:
        regional_clusters = []
        for each_item in global_cluster_members:

            if each_item['IsWriter']:
                regional_clusters = each_item['Readers']
                regional_clusters.append(each_item['DBClusterArn'])
                break
            # Raise Error if no secondary clusters are available
            if len(regional_clusters) == 0:
                print('No clusters found for provided global cluster',
                      '.Please check provided input.')
                raise RuntimeError

    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError
    return regional_clusters


def get_cluster_details(cluster):
    try:
        cluster_id = cluster.split(":")[-1]
        region = cluster.split(":")[3]
        client = session.client('docdb', region_name=region)
        response = client.describe_db_clusters(
            DBClusterIdentifier=cluster
        )
        cluster_response = response['DBClusters'][0]

        vpc_group_ids = []
        for each_item in cluster_response['VpcSecurityGroups']:
            vpc_group_ids.append(each_item['VpcSecurityGroupId'])
        
        if "-flipped" in cluster_id:
            last_index = cluster_id.rfind("-")
            cluster_id = cluster_id[:last_index]
        else:
            cluster_id = cluster_id + "-flipped"

        cluster_details = {
            # When converting the cluster to global cluster and adding clusters from the prior global
            # cluster, we append the timestamp to keep the cluster ID unique. This is needed so that the
            # function does not wait for the older clusters to be deleted. Also helps to differentiate
            # between clusters created by script.
            "secondary_cluster_id": cluster_id + "-" + dt_string,
            "region": region,
            "number_of_instances": len(cluster_response['DBClusterMembers']),
            "subnet_group": cluster_response['DBSubnetGroup'],
            "security_group_id": vpc_group_ids,
            "backup_retention_period": cluster_response['BackupRetentionPeriod'],
            "cluster_parameter_group": cluster_response['DBClusterParameterGroup'],
            "preferred_back_up_window": cluster_response['PreferredBackupWindow'],
            "preferred_maintenance_window": cluster_response['PreferredMaintenanceWindow'],
            "storage_encryption": cluster_response['StorageEncrypted'],
            "deletion_protection": cluster_response['DeletionProtection'],
            "engine_version": cluster_response['EngineVersion']
        }
        # add KmsKeyId to cluster_details dictionary only if it exists in the deleted cluster
        if 'KmsKeyId' in cluster_response:
            cluster_details["kms_key_id"] = cluster_response['KmsKeyId']
        return cluster_details

    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError
