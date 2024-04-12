import time

import boto3
from botocore.exceptions import ClientError

session = boto3.Session()


def convert_regional_to_global(primary_cluster_arn, global_cluster_id, secondary_clusters,enable_performance_insights, io_optimized_storage):
    try:
        start_time = time.time()
        primary_cluster_id = primary_cluster_arn.split(":")[-1]
        region = primary_cluster_arn.split(":")[3]
        client_local = session.client('docdb', region_name=region)
        instance_class = ""
        # Identify the instance class of the primary cluster and use the same for secondary clusters.
        while not instance_class:
            print('Checking for instance class for primary cluster ', primary_cluster_id, ' ...')
            instance_class = identify_instance_class(primary_cluster_id, client_local)
            time.sleep(1)
        print('Instance class for the current primary cluster is ', instance_class)
        cluster_status = ""
        while cluster_status != 'available':
            print('Checking for cluster and instance status before converting to global cluster...')
            cluster_status = get_cluster_status(primary_cluster_arn)
            time.sleep(1)
        print('Cluster and instances for primary cluster ', primary_cluster_arn, ' is in available status. '
                                                                                 'Start conversion process')
        print('Begin STEP 1 of 2 in convert to global cluster: Create global cluster ', global_cluster_id)
        print('Converting primary cluster ', primary_cluster_arn, ' to global cluster ', global_cluster_id)
        create_global_cluster(global_cluster_id, primary_cluster_arn)
        print('Created global cluster with id ', global_cluster_id)
        current_time = time.time()
        print('Completed STEP 1 of 2 in convert to global cluster process in ', current_time - start_time, ' seconds')
        # For each secondary clusters in input, add the cluster as a region to the global cluster created above.
        print('Begin STEP 2 of 2 in convert to global cluster: Create Secondary Clusters')
        for each_item in secondary_clusters:
            client_local = session.client('docdb', region_name=each_item['region'])
            create_secondary_cluster(each_item, global_cluster_id, client_local, io_optimized_storage)
            print('Created secondary cluster with id ', each_item['secondary_cluster_id'])
            # For each secondary cluster in the global cluster, add instances as indicated in the input and use
            # instance class identified earlier from primary
            for instance_count in range(0, each_item['number_of_instances']):
                add_instance_to_cluster(each_item, instance_class, instance_count, client_local, enable_performance_insights)
                print('Created instance ', each_item['secondary_cluster_id'] + str(instance_count),
                      'for secondary cluster ', each_item['secondary_cluster_id'])
        current_time = time.time()
        print('Completed STEP 2 of 2 in convert to global cluster process in ', current_time - start_time, ' seconds')
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError


def identify_instance_class(primary_cluster_id, client_local):
    try:
        response = client_local.describe_db_clusters(
            DBClusterIdentifier=primary_cluster_id
        )
        instance_class = ""
        cluster_instances = response['DBClusters'][0]['DBClusterMembers']
        for each_item in cluster_instances:
            if each_item['IsClusterWriter']:
                instance_id = each_item['DBInstanceIdentifier']

                primary_instance = client_local.describe_db_instances(
                    DBInstanceIdentifier=instance_id
                )
                instance_class = primary_instance['DBInstances'][0]['DBInstanceClass']
                break
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError
    return instance_class


def add_instance_to_cluster(each_item, instance_class, instance_count, client_local, enable_performance_insights):
    try:
        response = client_local.create_db_instance(
            DBClusterIdentifier=each_item['secondary_cluster_id'],
            DBInstanceIdentifier=each_item['secondary_cluster_id'] + str(instance_count),
            DBInstanceClass=instance_class,
            Engine='docdb',
            EnablePerformanceInsights=enable_performance_insights
        )
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError

# create secondary cluster using create_db_cluster API and pass a unpacked dictioanry as parameters
# omit values that are None
def create_secondary_cluster(each_item, global_cluster_id, client_local, io_optimized_storage):
    try:
        cluster_map = get_cluster_args(global_cluster_id, each_item)
        if io_optimized_storage:
                cluster_map["StorageType"] = "iopt1"
        response = client_local.create_db_cluster(
            **{k: v for k, v in cluster_map.items() if v is not None})
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError

# creating a dictionary of args to be used with create_db_cluster API
def get_cluster_args(global_cluster_id, each_item):
    cluster_map = {}
    cluster_map['GlobalClusterIdentifier']=global_cluster_id
    cluster_map['SourceRegion']=each_item['region']
    cluster_map['DBClusterIdentifier']=each_item['secondary_cluster_id']
    cluster_map['DBSubnetGroupName']=each_item['subnet_group']
    cluster_map['VpcSecurityGroupIds']=each_item['security_group_id']
    cluster_map['KmsKeyId']=fetch_kms_key(each_item)
    cluster_map['Engine']='docdb'
    cluster_map['EngineVersion']=each_item['engine_version']
    cluster_map['DBClusterParameterGroupName']=each_item['cluster_parameter_group']
    cluster_map['BackupRetentionPeriod']=each_item['backup_retention_period']
    cluster_map['PreferredBackupWindow']=each_item['preferred_back_up_window']
    cluster_map['PreferredMaintenanceWindow']=each_item['preferred_maintenance_window']
    cluster_map['StorageEncrypted']=each_item['storage_encryption']
    cluster_map['DeletionProtection']=each_item['deletion_protection']
    cluster_map['StorageType'] = fetch_storage_type(each_item)
    return cluster_map

# retrieve KMS key if exists else return None
def fetch_kms_key(each_item):  
    if 'kms_key_id' in each_item:
        KmsKeyId=each_item['kms_key_id']
        return KmsKeyId
    else:
        return None

def fetch_storage_type(each_item):
    if 'StorageType' in each_item:
        return each_item['StorageType']
    else:
        return None

def create_global_cluster(global_cluster_id, primary_cluster_arn):
    try:
        region = primary_cluster_arn.split(":")[3]
        client_local = session.client('docdb', region_name=region)
        response = client_local.create_global_cluster(
            GlobalClusterIdentifier=global_cluster_id,
            SourceDBClusterIdentifier=primary_cluster_arn
        )

    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError


def get_instance_status(instance_id, client):
    response_instance = client.describe_db_instances(
        DBInstanceIdentifier=instance_id
    )
    return response_instance['DBInstances'][0]['DBInstanceStatus']


def get_cluster_status(cluster_arn):
    cluster_id = cluster_arn.split(":")[-1]
    region = cluster_arn.split(":")[3]
    client = session.client('docdb', region_name=region)
    response = client.describe_db_clusters(
        DBClusterIdentifier=cluster_id
    )
    cluster_members = response['DBClusters'][0]['DBClusterMembers']
    for each_instance in cluster_members:
        instance_id = each_instance['DBInstanceIdentifier']
        instance_status = ''
        while instance_status != 'available':
            instance_status = get_instance_status(instance_id, client)
            time.sleep(1)
    return response['DBClusters'][0]['Status']
