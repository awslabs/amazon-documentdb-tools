import json
import boto3
import argparse
import widgets as w


def create_dashboard(widgets, region, instanceList, clusterList):
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
                if 'DBInstanceIdentifier' in widget["properties"]["metrics"][0]:
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
                    for i, DBClusterIdentifier in enumerate(clusterList):
                        if (i == 0):
                            widget["properties"]["metrics"][i].append(DBClusterIdentifier)
                            widget["properties"]["metrics"][i].append({"label":DBClusterIdentifier})
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
    parser.add_argument('--include-nvme',required=False,action='store_true',help='Include NVMe-backed instance metrics')
    args = parser.parse_args()

    # DocumentDB Configurations
    if args.endpoint_url is not None:
        docdbclient = boto3.client('docdb', region_name=args.region, endpoint_url=args.endpoint_url)
    else:
        docdbclient = boto3.client('docdb', region_name=args.region)

    clusterList = args.clusterID.split(',')
    instanceList = []
    for thisCluster in clusterList:
        response = docdbclient.describe_db_clusters(DBClusterIdentifier=thisCluster,Filters=[{'Name': 'engine','Values': ['docdb']}])
        for thisInstance in response["DBClusters"][0]["DBClusterMembers"]:
            instanceList.append(thisInstance)

    # CloudWatch client
    client = boto3.client('cloudwatch', region_name=args.region)

    # All widgets to be displayed on the dashboard
    widgets = [
        {"height":2,"panels":[w.ClusterHeading]},
        {"height":2,"panels":[w.metricHelp,w.bestPractices]},
        {"height":7,"panels":[w.DBClusterReplicaLagMaximum,w.DatabaseCursorsTimedOut,w.VolumeWriteIOPS,w.VolumeReadIOPS]},
        {"height":7,"panels":[w.OpscountersInsert,w.OpscountersUpdate,w.OpscountersDelete,w.OpscountersQuery]},
        {"height":2,"panels":[w.InstanceHeading]},
        {"height":7,"panels":[w.CPUUtilization,w.DatabaseConnections,w.DatabaseCursors]},
        {"height":7,"panels":[w.BufferCacheHitRatio,w.IndexBufferCacheHitRatio,w.FreeableMemory,w.FreeLocalStorage]},
        {"height":7,"panels":[w.NetworkTransmitThroughput,w.NetworkReceiveThroughput]},
        {"height":7,"panels":[w.StorageNetworkTransmitThroughput,w.StorageNetworkReceiveThroughput]},
        {"height":7,"panels":[w.DocsInserted,w.DocsDeleted,w.DocsUpdated,w.DocsReturned]},
        {"height":7,"panels":[w.ReadLatency,w.WriteLatency,w.DiskQueueDepth,w.DBInstanceReplicaLag]},
        {"height":7,"panels":[w.WriteIops,w.WriteThroughput,w.ReadIops,w.ReadThroughput]},
        {"height":2,"panels":[w.BackupStorageHeading]},
        {"height":7,"panels":[w.VolumeBytesUsed,w.BackupRetentionPeriodStorageUsed,w.TotalBackupStorageBilled]},
    ]

    # Optional NVMe Metrics
    if args.include_nvme:
        print("{}".format("Adding NVMe-backed instance metrics"))
        widgets.append({"height":2,"panels":[w.NVMeHeading]})
        widgets.append({"height":7,"panels":[w.FreeNVMeStorage,w.NVMeStorageCacheHitRatio]})
        widgets.append({"height":7,"panels":[w.ReadIopsNVMeStorage,w.ReadLatencyNVMeStorage,w.ReadThroughputNVMeStorage]})
        widgets.append({"height":7,"panels":[w.WriteIopsNVMeStorage,w.WriteLatencyNVMeStorage,w.WriteThroughputNVMeStorage]})

    # Create the CW data
    dashboardWidgets = create_dashboard(widgets, args.region, instanceList, clusterList)

    # Converting to json
    dashBody = json.dumps({"widgets":dashboardWidgets})

    # Create dashboard
    client.put_dashboard(DashboardName=args.name, DashboardBody=dashBody)

    print("Dashboard {} deployed to CloudWatch".format(args.name))


if __name__ == "__main__":
    main()
