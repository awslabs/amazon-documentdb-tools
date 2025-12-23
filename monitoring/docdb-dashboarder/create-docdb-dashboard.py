import json
import boto3
import argparse
import widgets as w


def create_dashboard(widgets, region, instanceList, clusterList, monitoring_type=None):
    tempWidgets = []
    widthX = 24

    dashboardY = 0
    for thisRow in widgets:
        dashboardX = 0
        incrementX = widthX // len(thisRow['panels'])

        for widget in thisRow['panels']:
            widget["properties"]['region'] = region
            widget["height"] = thisRow["height"]
            widget["width"] = incrementX
            widget["x"] = dashboardX
            widget["y"] = dashboardY

            if 'metrics' in widget["properties"]:
                if monitoring_type == 'dms' and 'AWS/DMS' in widget["properties"]["metrics"][0]:
                    # DMS metrics already have their task IDs set
                    pass
                elif 'DBInstanceIdentifier' in widget["properties"]["metrics"][0]:
                    for i, DBInstanceIdentifier in enumerate(instanceList):
                        if DBInstanceIdentifier['IsClusterWriter']:
                            instanceType = '|PRIMARY'
                        else:
                            instanceType = '|REPLICA'

                        if (i == 0):
                            widget["properties"]["metrics"][i].append(DBInstanceIdentifier['DBInstanceIdentifier'])
                            widget["properties"]["metrics"][i].append({"label":DBInstanceIdentifier['DBInstanceIdentifier']+instanceType})
                        else:
                            widget["properties"]["metrics"].append([".",".",".",DBInstanceIdentifier['DBInstanceIdentifier'],{"label":DBInstanceIdentifier['DBInstanceIdentifier']+instanceType}])

                else:
                    # Check if this is a CustomDocDB metric with Cluster dimension
                    is_custom_metric = ('CustomDocDB' in widget["properties"]["metrics"][0][0])
                    
                    for i, DBClusterIdentifier in enumerate(clusterList):
                        if (i == 0):
                            widget["properties"]["metrics"][i].append(DBClusterIdentifier)
                            widget["properties"]["metrics"][i].append({"label":DBClusterIdentifier})
                        else:
                            if is_custom_metric:
                                widget["properties"]["metrics"].append([".",".",".",".",DBClusterIdentifier,{"label":DBClusterIdentifier}])
                            else:
                                widget["properties"]["metrics"].append([".",".",".",DBClusterIdentifier,{"label":DBClusterIdentifier}])

            tempWidgets.append(widget)
            dashboardX += incrementX                

        dashboardY += thisRow["height"]

    return tempWidgets


# Main method
def main():
    # Command line arguments for user to pass
    parser = argparse.ArgumentParser()
    parser.add_argument('--name', type=str, required=True, help="Name of CloudWatch dashboard to create")
    parser.add_argument('--region', type=str, required=True, help="Region of Amazon DocumentDB cluster(s)")
    parser.add_argument('--clusterID', type=str, required=True, help="Single Amazon DocumentDB cluster ID or comma separated list of cluster IDs")
    parser.add_argument('--monitor-migration',required=False,action='store_true',help='Include MongoDB to DocumentDB migration metrics')
    parser.add_argument('--endpoint-url',type=str,required=False,help='Override default endpoint URL')
    parser.add_argument('--monitor-dms', required=False, action='store_true', help='Include AWS DMS task metrics')
    parser.add_argument('--dms-task-id', type=str, required=False, help='DMS Replication Task ID')
    args = parser.parse_args()

    if args.monitor_migration and args.monitor_dms:
        print("\u2717 Error: Only one monitoring option can be selected. Use either --monitor-migration OR --monitor-dms, not both.")
        return
    
    # Validate DMS task ID is provided when monitoring DMS
    if args.monitor_dms and not args.dms_task_id:
        print("\u2717 Error: --dms-task-id is required when using --monitor-dms")
        return
    # DocumentDB Configurations
    if args.endpoint_url is not None:
        docdbclient = boto3.client('docdb', region_name=args.region, endpoint_url=args.endpoint_url)
    else:
        docdbclient = boto3.client('docdb', region_name=args.region)

    burstableInstances = ["db.t3.medium", "db.t4g.medium"]
    nvmeInstances = ['db.r6gd.xlarge','db.r6gd.2xlarge','db.r6gd.4xlarge','db.r6gd.8xlarge','db.r6gd.12xlarge','db.r6gd.16xlarge']
    serverlessInstances = ['db.serverless']

    clusterList = args.clusterID.split(',')
    instanceList = []
    foundBurstable = False
    foundNvme = False
    foundServerless = False
    for thisCluster in clusterList:
        response = docdbclient.describe_db_clusters(DBClusterIdentifier=thisCluster,Filters=[{'Name': 'engine','Values': ['docdb']}])
        for thisInstance in response["DBClusters"][0]["DBClusterMembers"]:
            instanceList.append(thisInstance)
            dbInstanceResponse = docdbclient.describe_db_instances(DBInstanceIdentifier=thisInstance["DBInstanceIdentifier"])
            if dbInstanceResponse["DBInstances"][0]["DBInstanceClass"] in burstableInstances:
                foundBurstable = True
            if dbInstanceResponse["DBInstances"][0]["DBInstanceClass"] in nvmeInstances:
                foundNvme = True
            if dbInstanceResponse["DBInstances"][0]["DBInstanceClass"] in serverlessInstances:
                foundServerless = True

    # CloudWatch client
    client = boto3.client('cloudwatch', region_name=args.region)

    # All widgets to be displayed on the dashboard
    widgets = [
        {"height":1,"panels":[w.ClusterHeading]},
        {"height":7,"panels":[w.DBClusterReplicaLagMaximum,w.DatabaseCursorsTimedOut,w.VolumeWriteIOPS,w.VolumeReadIOPS]},
        {"height":7,"panels":[w.OpscountersInsert,w.OpscountersUpdate,w.OpscountersDelete,w.OpscountersQuery]},
        {"height":1,"panels":[w.InstanceHeading]},
        {"height":7,"panels":[w.CPUUtilization,w.DatabaseConnections,w.DatabaseCursors]},
        {"height":7,"panels":[w.BufferCacheHitRatio,w.IndexBufferCacheHitRatio,w.FreeableMemory,w.FreeLocalStorage]},
        {"height":7,"panels":[w.NetworkTransmitThroughput,w.NetworkReceiveThroughput,w.StorageNetworkTransmitThroughput,w.StorageNetworkReceiveThroughput]},
        {"height":7,"panels":[w.DocsInserted,w.DocsDeleted,w.DocsUpdated,w.DocsReturned]},
        {"height":7,"panels":[w.ReadLatency,w.WriteLatency,w.DiskQueueDepth,w.DBInstanceReplicaLag]},
        {"height":7,"panels":[w.WriteIops,w.WriteThroughput,w.ReadIops,w.ReadThroughput]},
    ]

    # NVMe Metrics
    if foundNvme:
        print("{}".format("\u2713 Adding NVMe-backed instance metrics"))
        widgets.append({"height":1,"panels":[w.NVMeHeading]})
        widgets.append({"height":7,"panels":[w.FreeNVMeStorage,w.NVMeStorageCacheHitRatio]})
        widgets.append({"height":7,"panels":[w.ReadIopsNVMeStorage,w.ReadLatencyNVMeStorage,w.ReadThroughputNVMeStorage]})
        widgets.append({"height":7,"panels":[w.WriteIopsNVMeStorage,w.WriteLatencyNVMeStorage,w.WriteThroughputNVMeStorage]})

    # Serverless Metrics
    if foundServerless:
        print("{}".format("\u2713 Adding serverless instance metrics"))
        widgets.append({"height":1,"panels":[w.ServerlessHeading]})
        widgets.append({"height":7,"panels":[w.ServerlessDatabaseCapacity,w.DCUUtilization,w.TempStorageIops,w.TempStorageThroughput]})

    # Burstable Metrics
    if foundBurstable:
        print("{}".format("\u2713 Adding burstable instance metrics"))
        widgets.append({"height":1,"panels":[w.BurstableHeading]})
        widgets.append({"height":7,"panels":[w.CPUCreditUsage,w.CPUCreditBalance,w.CPUSurplusCreditsCharged,w.CPUSurplusCreditBalance]})

    # Determine monitoring type
    monitoring_type = None
    if args.monitor_migration:
        monitoring_type = 'migration'
        print("{}".format("\u2713 Adding MongoDB to DocumentDB Migration Monitoring metrics"))
        widgets.append({"height":1,"panels":[w.MigrationMonitoringHeading]})
        widgets.append({"height":1,"panels":[w.FullLoadMigrationHeading]})
        widgets.append({"height":7,"panels":[w.MigratorFLInsertsPerSecond,w.MigratorFLRemainingSeconds]})
        widgets.append({"height":1,"panels":[w.CDCReplicationHeading]})
        widgets.append({"height":7,"panels":[w.MigratorCDCNumSecondsBehind,w.MigratorCDCOperationsPerSecond]})
    
    elif args.monitor_dms:
        monitoring_type = 'dms'
        print("{}".format("\u2713 Adding AWS DMS Task metrics"))
        # Get the task ID
        task_id = args.dms_task_id
        #  Retrieve DMS task information and update widgets with task and instance identifiers
        update_dms_widgets(task_id, args.region, w)
        # Add DMS widgets to dashboard
        widgets.append({"height":1,"panels":[w.DMSHeading]})
        widgets.append({"height":7,"panels":[w.DMSFullLoadThroughputRowsTarget,w.DMSCDCLatencyTarget, w.DMSCDCThroughputRowsTarget]})

    # Backups
    widgets.append({"height":1,"panels":[w.BackupStorageHeading]})
    widgets.append({"height":7,"panels":[w.VolumeBytesUsed,w.BackupRetentionPeriodStorageUsed,w.TotalBackupStorageBilled]})

    # Create the CW data
    dashboardWidgets = create_dashboard(widgets, args.region, instanceList, clusterList, monitoring_type=monitoring_type)
    # Converting to json
    dashBody = json.dumps({"widgets":dashboardWidgets})
    # Create dashboard
    client.put_dashboard(DashboardName=args.name, DashboardBody=dashBody)

    print("\u2713 Dashboard {} deployed to CloudWatch".format(args.name))


def update_dms_widgets(task_id, region, w):
    """
    Retrieve DMS task information and update widgets with task and instance identifiers.
    
    Args:
        task_id: The DMS task ID
        region: AWS region
        w: Widget definitions module
    """
    try:
        dms_client = boto3.client('dms', region_name=region)
        response = dms_client.describe_replication_tasks(
            Filters=[
                {
                    'Name': 'replication-task-id',
                    'Values': [task_id]
                }
            ]
        )
        if response['ReplicationTasks']:
            # Get the full ARN of the task
            task_arn = response['ReplicationTasks'][0]['ReplicationTaskArn']
            # Extract the task ID from the ARN (last part)
            task_id = task_arn.split(':')[-1]
            
            # Get the replication instance ARN
            replication_instance_arn = response['ReplicationTasks'][0]['ReplicationInstanceArn']
            
            # Try to get all replication instances and find the one with matching ARN
            try:
                all_instances = dms_client.describe_replication_instances()
                instance_name = None
                
                for instance in all_instances.get('ReplicationInstances', []):
                    if instance.get('ReplicationInstanceArn') == replication_instance_arn:
                        instance_name = instance.get('ReplicationInstanceIdentifier')
                        break
                
                if instance_name:
                    # Update the widgets with task ID and instance name
                    for widget in [w.DMSFullLoadThroughputRowsTarget, w.DMSCDCLatencyTarget, w.DMSCDCThroughputRowsTarget]:
                        for i, metric in enumerate(widget["properties"]["metrics"]):
                            # Update task ID
                            task_id_index = metric.index("ReplicationTaskIdentifier") + 1 if "ReplicationTaskIdentifier" in metric else -1
                            if task_id_index != -1:
                                widget["properties"]["metrics"][i][task_id_index] = task_id
                            
                            # Update instance name
                            instance_id_index = metric.index("ReplicationInstanceIdentifier") + 1 if "ReplicationInstanceIdentifier" in metric else -1
                            if instance_id_index != -1:
                                widget["properties"]["metrics"][i][instance_id_index] = instance_name
                else:
                    print("\u2717 Warning: Could not find replication instance name. Using instance ID from ARN.")
                    
            except Exception as e:
                print(f"\u2717 Error getting replication instances: {str(e)}. Using instance ID from ARN.")
        else:
            print("\u2717 Warning: Could not find DMS task with ID '{}'.".format(task_id))
            
    except Exception as e:
        print("\u2717 Error retrieving DMS task: {}".format(str(e)))


if __name__ == "__main__":
    main()
