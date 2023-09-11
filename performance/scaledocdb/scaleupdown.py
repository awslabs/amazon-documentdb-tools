import boto3
import json
import os
import time
import logging
import sys
import argparse,traceback

logging.basicConfig(format='%(levelname)s - %(asctime)s - %(message)s',datefmt='%d-%b-%y %H:%M:%S',level=logging.INFO)
LOGGER = logging.getLogger()
docdb = boto3.client('docdb')


def isClusterStatusAvailable(cluster_id):
    r = docdb.describe_db_clusters( DBClusterIdentifier=cluster_id)
    cluster_info = r['DBClusters'][0]
    if cluster_info['Status'] != 'available':
        return False
    return True
    
def getInstanceStatus(cluster_id):
    if not isClusterStatusAvailable(cluster_id):
        return 
    istatus={}
    num_available = 0
    num_pending = 0
    num_failed = 0
    r = docdb.describe_db_instances(Filters=[{'Name':'db-cluster-id','Values': [cluster_id]}])
    instances = r['DBInstances']
    for i in instances:
        instance_id = i['DBInstanceIdentifier']
        instance_status = i['DBInstanceStatus']
        if instance_status == 'available':
            num_available = num_available + 1
        if instance_status in ['creating', 'deleting', 'starting', 'stopping']:
            num_pending = num_pending + 1
        if instance_status == 'failed':
                num_failed = num_failed + 1
    istatus["num_instances"]=len(instances)
    istatus["num_available"]=num_available
    istatus["num_pending"]=num_pending
    istatus["num_failed"]=num_failed
    return istatus
    
def getClusterInstances(cluster_id):
    r = docdb.describe_db_clusters( DBClusterIdentifier=cluster_id)
    cluster_info = r['DBClusters'][0]
    if cluster_info['Status'] != 'available':
        return False
    else:
        existing_instances = {}
        for member in cluster_info['DBClusterMembers']:
            member_id = member['DBInstanceIdentifier']
            if member['IsClusterWriter']==True:
                existing_instances[member_id]='WRITER'
            else:
                existing_instances[member_id]='READER'
    
    return existing_instances

def getWriterInstance(cluster_id):
    instances=getClusterInstances(cluster_id=cluster_id)
    reversed_dict = {}
    for key, value in instances.items():
        reversed_dict.setdefault(value, [])
        reversed_dict[value].append(key)
    return reversed_dict["WRITER"][0]

def isClusterHealthy(cluster_id):
    istatus=getInstanceStatus(cluster_id)
    LOGGER.info("cluster instance status: " + str(istatus))
    if istatus==None:
        LOGGER.error("Make sure cluster is in started state")
        return False
    if istatus["num_available"]==istatus["num_instances"] and isClusterStatusAvailable(cluster_id) :
        return True
    return False
    
def addInstances(cluster_id,desired_size,count):
    ninstances=[]
    for idx in range(count):
        instance_iden=cluster_id + '-' + str(idx) + '-' + str(int(time.time()))
        docdb.create_db_instance(DBInstanceIdentifier=instance_iden,DBInstanceClass=desired_size,Engine="docdb",DBClusterIdentifier=cluster_id)
        ninstances.append(instance_iden)
    return ninstances
        

def deleteInstances(instanceIdList):
    for instance in instanceIdList:
        docdb.delete_db_instance(DBInstanceIdentifier=instance)
        
def perfromFailover(cluster_id,failover_target):
    docdb.failover_db_cluster(DBClusterIdentifier=cluster_id,TargetDBInstanceIdentifier=failover_target)
    
def checkExistingInstanceClass(cluster_id,itarget):
    r = docdb.describe_db_instances(Filters=[{'Name':'db-cluster-id','Values': [cluster_id]}])
    instances = r['DBInstances']
    iclist=[]
    for i in instances:
        iclist.append(i['DBInstanceClass'])
    if itarget in iclist:
        return True
    else:
        return False

def checkClusterHealth(cluster_id,mins=15):
    count=0
    while not isClusterHealthy(cluster_id=cluster_id):
        count=count+1
        if count==mins:
            LOGGER.error("Cluster status is not healthy after " + str(mins) +  " mins. Please check the cluster health .Exiting.. ")
            return False
        LOGGER.info("will be checking cluster health in 60 seconds")
        time.sleep(60)
    return True

def main():
    
    parser = argparse.ArgumentParser(description='Amazon DocumentDB Cluster Scale up/down Tool.')
    parser.add_argument('--cluster-id',
                        required=True,
                        help='Amazon DocumentDB Cluster Identifer')
    
    parser.add_argument('--target-instance-type',
                        required=True,
                        type=str,
                        help='target instance type to perform scale up/down operation on given cluster id')

                        
    parser.add_argument('--ignore-instance-check',
                       required=False,
                       action='store_false',
                       help='ignore check for existing instances with target instance type (Optional)')
    
    args = parser.parse_args()

    cluster_id=args.cluster_id
    itarget=args.target_instance_type
    monitor_deletion=False
    target_instance_check=args.ignore_instance_check
    valid_instance_classes=["db.r6g.large","db.r6g.xlarge","db.r6g.2xlarge","db.r6g.4xlarge","db.r6g.8xlarge","db.r6g.12xlarge","db.r6g.16xlarge","db.r5.large","db.r5.xlarge","db.r5.2xlarge","db.r5.4xlarge","db.r5.8xlarge","db.r5.12xlarge","db.r5.16xlarge","db.r5.24xlarge","db.t4g.medium","db.t3.medium"]
   
    if itarget not in valid_instance_classes:
        LOGGER.error("Target instance type "+ itarget +" is invalid. Exiting..")
        sys.exit(1)
   
    if target_instance_check and checkExistingInstanceClass(cluster_id=cluster_id,itarget=itarget):
        LOGGER.error("Cluster already has one of the instance with target instance type.If you want to still continue,please run with --ignore-instance-check.Exiting..")
        sys.exit(1)

    if not isClusterHealthy(cluster_id):
        LOGGER.error("Cluster is not healthy. Exiting..")
        sys.exit(1)
        return False 
    
    num_instances=getInstanceStatus(cluster_id)["num_instances"]
    oinstances=list((getClusterInstances(cluster_id)).keys())
    
    if num_instances > 8:
        LOGGER.error("Number of instances can not be more than 8 for this script.")
        sys.exit(1)
        return False
    
    ## Add instances 
    LOGGER.info("Adding "+ str(len(oinstances))+  " instances with instance type " + itarget)
    ninstances=addInstances(cluster_id=cluster_id,desired_size=itarget,count=num_instances)
    LOGGER.info("checking  cluster health after instance addition" )
    count=0
    if checkClusterHealth(cluster_id=cluster_id):
        LOGGER.info("Cluster is healthy post instance addition,performing failover")
    else:
        LOGGER.info("Cluster is not healthy post instance addition,existing")
        sys.exit(1)
    
    failover_target=ninstances[0]
    old_writer=getWriterInstance(cluster_id=cluster_id)
    LOGGER.info("Failover target - " + failover_target)
    
    ## Perform failover 
    perfromFailover(cluster_id=cluster_id,failover_target=failover_target)
    LOGGER.info("checking failover status")
    for count in range(4):
        nwriter=getWriterInstance(cluster_id=cluster_id)
        if  nwriter in ninstances and old_writer!=nwriter:
            break
        LOGGER.info("sleeping for 30 sec to check failover status")
        time.sleep(30)

    ## Delete old instances 
    LOGGER.info("deleting old instances.")
    deleteInstances(oinstances)
    if monitor_deletion and checkClusterHealth(cluster_id=cluster_id):
        LOGGER.info("monitoring deleteion process")
        LOGGER.info("Cluster is healthy post instance deletion")
    elif monitor_deletion:
        LOGGER.info("Cluster is not healthy post instance deletion")
    LOGGER.info("scaling process completed.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        trc = traceback.format_exc()
        LOGGER.error(str(e))
