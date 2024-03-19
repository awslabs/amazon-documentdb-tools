import boto3
import time

from botocore.exceptions import ClientError

import route53_endpoint_management

session = boto3.Session()
client = session.client('docdb')


# Execution begins here...
def failover(global_cluster_id, secondary_cluster_arn, primary_cluster_cname, hosted_zone_id, is_delete_global_cluster):
    try:
        start_time = time.time()
        print('Retrieving secondary clusters for global cluster ', global_cluster_id)
        secondary_clusters = get_secondary_clusters(global_cluster_id)
        is_secondary_cluster_promoted = False
        # To minimize failover time, identify and promote the secondary cluster provided as input.
        for each_secondary_cluster in secondary_clusters:
            if each_secondary_cluster == secondary_cluster_arn:
                print('Found secondary cluster to be promoted.')
                print('Begin STEP 1 of 3 in failover process: Failover secondary cluster ', secondary_cluster_arn)
                # Cluster should be in available status before removing from global cluster
                cluster_status = ""
                while cluster_status != 'available':
                    print('Checking for cluster and instance status before promotion...')
                    cluster_status = get_cluster_status(each_secondary_cluster)
                    time.sleep(1)
                print('Cluster and instances are in available status. Promoting secondary cluster ***',
                      each_secondary_cluster, '*** to a standalone cluster.')
                remove_from_global_cluster(each_secondary_cluster, global_cluster_id)
                print('Initiated process to promote and remove secondary cluster ', secondary_cluster_arn,
                      ' from global cluster ', global_cluster_id)
                print('Waiting for secondary cluster ,', secondary_cluster_arn, 'to be removed from global cluster ',
                      global_cluster_id)
                wait_for_promotion_to_complete(global_cluster_id, secondary_cluster_arn)
                current_time = time.time()
                is_secondary_cluster_promoted = True
                print('Completed STEP 1 of 3 in failover process in ', current_time - start_time, ' seconds')
                # Call Describe cluster on each_secondary_cluster to get cluster endpoint
                print('Retrieving cluster endpoint for the recently promoted cluster ', each_secondary_cluster)
                cluster_end_point = get_cluster_endpoint(each_secondary_cluster)
                print('Begin STEP 2 of 3 in failover process : Updating route 53 CNAME ', primary_cluster_cname,
                      ' to route to new cluster endpoint ', cluster_end_point)
                # TODO : Check for explicit event for failover completion
                route53_endpoint_management.manage_application_endpoint(hosted_zone_id, cluster_end_point,
                                                                        primary_cluster_cname)
                current_time = time.time()
                print('Successfully Updated CNAME ', primary_cluster_cname, ' with record value ', cluster_end_point)
                print('Completed STEP 2 of 3 in failover process in ', current_time - start_time, ' seconds')

                break
        if not is_secondary_cluster_promoted:
            print("No matching secondary cluster found for *** ", secondary_cluster_arn, "***.Please check provided "
                                                                                         "input.")
            raise RuntimeError

        # Proceed to delete the global cluster
        if is_delete_global_cluster:
            print('Begin STEP 3 of 3 in failover process : '
                  'Process to delete other secondary clusters and the global cluster ', global_cluster_id)
            delete_global_cluster(global_cluster_id, secondary_cluster_arn, secondary_clusters,
                                  is_delete_global_cluster)
            current_time = time.time()
            print('Completed STEP 3 of 3 in failover process in ', current_time - start_time, ' seconds')

        else:
            print('Optional STEP 3 to delete cluster not chosen per provided input. '
                  'Process will complete now.')
            print('********* SUCCESS : Secondary cluster ', secondary_cluster_arn,
                  'has been promoted to standalone '
                  'primary. *********')

    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError


# Retrieve all secondary clusters for the global cluster
def get_secondary_clusters(global_cluster_id):
    try:
        response = client.describe_global_clusters(
            GlobalClusterIdentifier=global_cluster_id
        )
        global_cluster_members = response['GlobalClusters'][0]['GlobalClusterMembers']
        secondary_clusters = []
        for each_item in global_cluster_members:

            if each_item['IsWriter']:
                secondary_clusters = each_item['Readers']
                break
            # Raise error  if no secondary clusters are available
            if not secondary_clusters:
                print('No secondary clusters found for provided cluster', global_cluster_id,
                      '.Please check provided input.')
                raise RuntimeError

    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError
    return secondary_clusters


def remove_from_global_cluster(cluster_to_remove, global_cluster_id):
    try:
        response = client.remove_from_global_cluster(
            GlobalClusterIdentifier=global_cluster_id,
            DbClusterIdentifier=cluster_to_remove
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


def delete_global_cluster(global_cluster_id, secondary_cluster_arn, secondary_clusters, is_delete_global_cluster):
    # Remove and promote all clusters to standalone clusters
    for each_secondary_cluster in secondary_clusters:
        # each_secondary_cluster_id = each_secondary_cluster.split(":")[-1]
        if each_secondary_cluster != secondary_cluster_arn:
            print('Removing secondary cluster ', each_secondary_cluster, ' from global cluster ', global_cluster_id)
            remove_from_global_cluster(each_secondary_cluster, global_cluster_id)
            # Wait until all standalone clusters are promoted to stand alone clusters and removed from global cluster
            print('Waiting till all secondary clusters are removed from global cluster ', global_cluster_id)
            wait_for_promotion_to_complete(global_cluster_id, each_secondary_cluster)

    # Delete secondary clusters
    print('All secondary clusters are promoted to standalone cluster. Begin deleting each cluster.')
    for each_secondary_cluster in secondary_clusters:
        if each_secondary_cluster != secondary_cluster_arn:
            print('Deleting secondary cluster ', each_secondary_cluster, ' within global cluster ', global_cluster_id)
            delete_cluster(each_secondary_cluster)
    # delete primary cluster
    delete_primary_cluster(global_cluster_id)
    print('Deleting global cluster... ', global_cluster_id)
    # delete global cluster
    deleted_global_cluster = client.delete_global_cluster(
        GlobalClusterIdentifier=global_cluster_id
    )

    print('********* SUCCESS : Secondary cluster ', secondary_cluster_arn, ' has been promoted to standalone '
                                                                           'primary and global cluster ',
          global_cluster_id, 'has been deleted. *********')


def delete_primary_cluster(global_cluster_id):
    try:
        print('Retrieving primary cluster to delete from global cluster ', global_cluster_id)
        primary_cluster = get_primary_cluster(global_cluster_id)

        # Primary cluster state will change to 'modifying' a few seconds after the secondary cluster is removed
        # Wait until cluster status is 'modifying' and then wait for it to become 'available' again
        primary_cluster_status = ""
        wait_start = time.time()
        while primary_cluster_status != 'modifying' and (time.time() - wait_start < 30):
            print('primary cluster status is not modifying')
            primary_cluster_status = get_cluster_status(primary_cluster)
            time.sleep(1)

        # Cluster should be in available status before removing from global cluster
        primary_cluster_status = ""
        while primary_cluster_status != 'available':
            print('Checking for primary cluster ', primary_cluster, ' and its instance status before deletion...')
            primary_cluster_status = get_cluster_status(primary_cluster)
            time.sleep(1)

        print('Removing primary cluster... ', primary_cluster, ' from global cluster ', global_cluster_id)
        remove_from_global_cluster(primary_cluster, global_cluster_id)

        # Wait until all standalone clusters are promoted
        wait_for_promotion_to_complete(global_cluster_id, primary_cluster)

        # Wait until primary cluster status becomes available
        primary_cluster_status = get_cluster_status(primary_cluster)
        while primary_cluster_status != 'available':
            time.sleep(10)
            primary_cluster_status = get_cluster_status(primary_cluster)

        print('Deleting primary cluster, ', primary_cluster, ' from global cluster ', global_cluster_id)
        delete_cluster(primary_cluster)
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        print('If the primary cluster deletion is interrupted due to regional outage, you can manually delete the '
              'primary cluster later. The failover of secondary cluster to standalone primary will not be impacted '
              'due to this error  ')
        raise ClientError


def wait_for_promotion_to_complete(global_cluster_id, cluster_arn):
    remaining_secondary_clusters = get_secondary_clusters(global_cluster_id)
    while len(remaining_secondary_clusters) > 0:
        time.sleep(10)
        remaining_secondary_clusters = get_secondary_clusters(global_cluster_id)
        print('Waiting for cluster promotion to complete...')
        # The below code is to check if the cluster provided as input has been deleted. This is to be used
        # when global cluster delete indicator is 'N'
        is_cluster_promotion_in_progress = False
        for each_cluster in remaining_secondary_clusters:
            # each_secondary_cluster_id = each_cluster.split(":")[-1]
            if each_cluster == cluster_arn:
                is_cluster_promotion_in_progress = True
                break
        if len(remaining_secondary_clusters) == 0 or (not is_cluster_promotion_in_progress):
            print('Cluster promotion process completed')
            break


def delete_cluster(cluster_to_delete):
    try:
        cluster_to_delete_id = cluster_to_delete.split(":")[-1]
        region = cluster_to_delete.split(":")[3]
        client = session.client('docdb', region_name=region)
        response = client.describe_db_clusters(
            DBClusterIdentifier=cluster_to_delete
        )
        cluster_instances = response['DBClusters'][0]['DBClusterMembers']

        # Delete instances in a cluster
        for each_item in cluster_instances:
            instance_to_delete = each_item['DBInstanceIdentifier']
            print('Deleting instance... ', instance_to_delete, ' within cluster ', cluster_to_delete)
            delete_instance = client.delete_db_instance(
                DBInstanceIdentifier=instance_to_delete
            )
        # Disable deletion protection
        disable_deletion_protection = client.modify_db_cluster(
            DBClusterIdentifier=cluster_to_delete_id,
            ApplyImmediately=True,
            DeletionProtection=False
        )

        # Delete cluster
        deleted_cluster = client.delete_db_cluster(
            DBClusterIdentifier=cluster_to_delete_id,
            SkipFinalSnapshot=True
        )

    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError


def get_primary_cluster(global_cluster_id):
    try:
        response = client.describe_global_clusters(
            GlobalClusterIdentifier=global_cluster_id
        )
        global_cluster_members = response['GlobalClusters'][0]['GlobalClusterMembers']

        for each_item in global_cluster_members:
            if each_item['IsWriter']:
                primary_cluster = each_item['DBClusterArn']
                break
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError
    return primary_cluster


def get_cluster_endpoint(cluster):
    try:
        region = cluster.split(":")[3]
        regional_client = session.client('docdb', region_name=region)
        response = regional_client.describe_db_clusters(
            DBClusterIdentifier=cluster
        )
        endpoint = response['DBClusters'][0]['Endpoint']
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError
    return endpoint
