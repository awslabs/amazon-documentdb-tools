#!/usr/bin/env python3
 
import boto3
import datetime
import argparse
import requests
import json
import sys
import os


def deleteLog(appConfig):
    if os.path.exists(appConfig['logFileName']):
        os.remove(appConfig['logFileName'])


def printLog(thisMessage,appConfig):
    with open(appConfig['logFileName'], 'a') as fp:
        fp.write("{}\n".format(thisMessage))


def get_cw_metric_daily_average(appConfig, cwClient, cwMetric, cwMath, cwCluster):
    namespace = "AWS/DocDB"
    metric = cwMetric
    period = 87600 # Seconds in a day
    dimensions = [{"Name":"DBClusterIdentifier","Value":cwCluster}]
     
    startTime = appConfig['startTime']
    endTime = appConfig['endTime']
     
    response = cwClient.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric,
        StartTime=startTime,
        EndTime=endTime,
        Period=period,
        Statistics=[cwMath],
        Dimensions=dimensions,
    )

    metricValues = {}

    cwMetricTotal = 0
    cwMetricValues = 0
    cwMetricAverage = 0
    
    for cw_metric in response['Datapoints']:
        metricValues[cw_metric['Timestamp']] = cw_metric.get(cwMath)
        cwMetricTotal += cw_metric.get(cwMath)
        cwMetricValues += 1
        #if cwMetric == 'CPUSurplusCreditsCharged':
        #    print("{}".format(cw_metric))
        
    if cwMetricValues == 0:
        cwMetricAverage = int(0)
    else:
        cwMetricAverage = int(cwMetricTotal // cwMetricValues)
        
    return cwMetricAverage


def get_docdb_instance_based_clusters(appConfig, pricingDict):
    gbBytes = 1000 * 1000 * 1000
    gibBytes = 1024 * 1024 * 1024
    
    client = boto3.client('docdb',region_name=appConfig['region'])
    cwClient = boto3.client('cloudwatch',region_name=appConfig['region'])
    
    printLog("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format('cluster','io-type','version','num-instances','standard-compute','standard-io','standard-storage','standard-backup','standard-total','io-optimized-compute','io-optimized-io','io-optimized-storage','io-optimized-backup','io-optimized-total','recommendation','estimated-potential-savings'),appConfig)
    
    #response = client.describe_db_clusters()
    response = client.describe_db_clusters(Filters=[{'Name': 'engine','Values': ['docdb']}])
    
    for thisCluster in response['DBClusters']:
        monthlyStandard = 0.00
        monthlyIoOptimized = 0.00
        
        ioType = thisCluster.get('StorageType','standard')
        #print("{}".format(thisCluster.get('StorageType','missing')))
        thisMonthlyStandardIoCompute = 0.00
        thisMonthlyOptimizedIoCompute = 0.00
        numInstances = 0
        engineVersionFull = thisCluster['EngineVersion']
        engineVersionMajor = int(engineVersionFull.split('.')[0])
        clusterContainsServerless = False
        for thisInstance in thisCluster['DBClusterMembers']:
            # get instance type
            responseInstance = client.describe_db_instances(DBInstanceIdentifier=thisInstance['DBInstanceIdentifier'])
            numInstances += 1
            dbInstanceClass = responseInstance['DBInstances'][0]['DBInstanceClass']
            if dbInstanceClass == 'db.serverless':
                clusterContainsServerless = True
                continue

            thisStandardIoCompute = round(float(pricingDict['compute|'+appConfig['region']+'|'+dbInstanceClass+'|standard']['price'])*30*24,0)
            thisOptimizedIoCompute = round(float(pricingDict['compute|'+appConfig['region']+'|'+dbInstanceClass+'|iopt1']['price'])*30*24,0)
            thisMonthlyStandardIoCompute += thisStandardIoCompute
            thisMonthlyOptimizedIoCompute += thisOptimizedIoCompute

        if clusterContainsServerless:
            print("")
            print("cluster = {} | contains one or more serverless instances, this utility does not support this instance type".format(thisCluster['DBClusterIdentifier']))
            continue
            
        print("")
        print("cluster = {} | IO type = {} | version = {} | instances = {:d}".format(thisCluster['DBClusterIdentifier'],ioType,engineVersionFull,numInstances))
        print("  ESTIMATED standard storage costs      | ESTIMATED io optimized storage costs")

        monthlyStandard += thisMonthlyStandardIoCompute
        monthlyIoOptimized += thisMonthlyOptimizedIoCompute
            
        # get historical cloudwatch information
        avgReadIopsMonth = get_cw_metric_daily_average(appConfig, cwClient, 'VolumeReadIOPs', 'Sum', thisCluster['DBClusterIdentifier'])
        avgWriteIopsMonth = get_cw_metric_daily_average(appConfig, cwClient, 'VolumeWriteIOPs', 'Sum', thisCluster['DBClusterIdentifier'])
        totStorageBytes = get_cw_metric_daily_average(appConfig, cwClient, 'VolumeBytesUsed', 'Maximum', thisCluster['DBClusterIdentifier'])
        totBackupStorageBilledBytes = get_cw_metric_daily_average(appConfig, cwClient, 'TotalBackupStorageBilled', 'Maximum', thisCluster['DBClusterIdentifier'])
        totCPUCredits = get_cw_metric_daily_average(appConfig, cwClient, 'CPUSurplusCreditsCharged', 'Sum', thisCluster['DBClusterIdentifier'])
        
        totIopsMonth = (avgReadIopsMonth * 30) + (avgWriteIopsMonth * 30)
        
        # estimated CPU credits
        thisCPUCreditCost = round(totCPUCredits * float(pricingDict['cpu-credits|'+appConfig['region']]['price']) / 60 * 30,0)
        thisMonthlyStandardIoCompute += thisCPUCreditCost
        thisMonthlyOptimizedIoCompute += thisCPUCreditCost

        monthlyStandard += thisCPUCreditCost
        monthlyIoOptimized += thisCPUCreditCost

        # estimated io cost
        thisStandardIopsCost = round(totIopsMonth * float(pricingDict['io|'+appConfig['region']+'|standard']['price']),0)
        thisOptimizedIopsCost = round(totIopsMonth * float(pricingDict['io|'+appConfig['region']+'|iopt1']['price']),0)
        
        monthlyStandard += thisStandardIopsCost
        monthlyIoOptimized += thisOptimizedIopsCost
        
        # estimated storage cost
        thisStandardStorageCost = round(totStorageBytes * float(pricingDict['storage|'+appConfig['region']+'|standard']['price']) / gbBytes,0)
        thisOptimizedStorageCost = round(totStorageBytes * float(pricingDict['storage|'+appConfig['region']+'|iopt1']['price']) / gbBytes,0)

        monthlyStandard += thisStandardStorageCost
        monthlyIoOptimized += thisOptimizedStorageCost
        
        # estimated backup cost
        thisBackupCost = round(totStorageBytes * float(pricingDict['storage-snapshot|'+appConfig['region']]['price']) / gbBytes,0)

        monthlyStandard += thisBackupCost
        monthlyIoOptimized += thisBackupCost

        print("  compute                 = ${:10,.0f} | compute                 = ${:10,.0f}".format(thisMonthlyStandardIoCompute,thisMonthlyOptimizedIoCompute))
        print("  io                      = ${:10,.0f} | io                      = ${:10,.0f}".format(thisStandardIopsCost,thisOptimizedIopsCost))
        print("  storage                 = ${:10,.0f} | storage                 = ${:10,.0f}".format(thisStandardStorageCost,thisOptimizedStorageCost))
        print("  backup storage          = ${:10,.0f} | backup storage          = ${:10,.0f}".format(thisBackupCost,thisBackupCost))
        print("  ESTIMATED monthly total = ${:10,.0f} | Estimated monthly total = ${:10,.0f}".format(monthlyStandard,monthlyIoOptimized))
        
        recommendationString = ""
        estimatedMonthlySavings = 0.00
        if (ioType == "standard") and (monthlyIoOptimized < monthlyStandard) and (engineVersionMajor < 5):
            estimatedMonthlySavings = monthlyStandard-monthlyIoOptimized
            recommendationString = "  **** recommendation - consider switching to IO optimized to potentially save ${:.0f} per month but requires upgrading to DocumentDB v5+".format(estimatedMonthlySavings)
            print("")
            print(recommendationString)
        elif (ioType == "standard") and (monthlyIoOptimized < monthlyStandard):
            estimatedMonthlySavings = monthlyStandard-monthlyIoOptimized
            recommendationString = "  **** recommendation - consider switching to IO optimized to potentially save ${:.0f} per month".format(estimatedMonthlySavings)
            print("")
            print(recommendationString)
        elif (ioType != "standard") and (monthlyStandard < monthlyIoOptimized):
            estimatedMonthlySavings = monthlyIoOptimized-monthlyStandard
            recommendationString = "  **** recommendation - consider switching to standard IO to potentially save ${:.0f} per month".format(estimatedMonthlySavings)
            print("")
            print(recommendationString)
        else:
            estimatedMonthlySavings = 0.00
            if (ioType == "standard"):
                ioTypeText = "standard"
            else:
                ioTypeText = "io optimized"

            recommendationString = "  **** current {} storage configuration achieves the lowest possible price point".format(ioTypeText)
            print("")
            print(recommendationString)

        printLog("{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(thisCluster['DBClusterIdentifier'],ioType,engineVersionFull,numInstances,
          thisMonthlyStandardIoCompute,thisStandardIopsCost,thisStandardStorageCost,thisBackupCost,monthlyStandard,
          thisMonthlyOptimizedIoCompute,thisOptimizedIopsCost,thisOptimizedStorageCost,thisBackupCost,monthlyIoOptimized,
          recommendationString,estimatedMonthlySavings),appConfig)
    
    client.close()
    cwClient.close()
    
    
def get_pricing(appConfig):
    pd = {}

    print("retrieving pricing...")
    pricingUrl = 'https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonDocDB/current/index.json'
    response = requests.get(pricingUrl)
    pricingDict = json.loads(response.text)
    
    # get the terms
    terms = {}
    for thisTermKey in pricingDict['terms']['OnDemand']:
        for thisTerm in pricingDict['terms']['OnDemand'][thisTermKey].values():
            # find the price
            thisTermSku = thisTerm['sku']
            thisTermPrice = list(thisTerm['priceDimensions'].values())[0]['pricePerUnit']['USD']
            terms[thisTermSku] = thisTermPrice
    
    # get the pricing
    for thisProductKey in pricingDict['products']:
        thisProduct = pricingDict['products'][thisProductKey]

        if 'productFamily' not in thisProduct and thisProduct['attributes']['group'] == 'Global Cluster I/O Operation':
            # Global Cluster IO cost
            thisSku = thisProduct['sku']
            thisRegion = thisProduct["attributes"]["regionCode"]
            thisIoType = 'standard'
            thisPrice = terms[thisSku]
            thisPricingDictKey = "{}|{}|{}".format('io',thisRegion,thisIoType)
            pd[thisPricingDictKey] = {'type':'global-cluster-io','region':thisRegion,'ioType':thisIoType,'price':thisPrice}
            # no charge for IO iopt1
            thisIoType = 'iopt1'
            thisPrice = 0.00
            thisPricingDictKey = "{}|{}|{}".format('io',thisRegion,thisIoType)
            pd[thisPricingDictKey] = {'type':'global-cluster-io','region':thisRegion,'ioType':thisIoType,'price':thisPrice}

        elif 'productFamily' not in thisProduct:
            print("*** missing productFamily *** | {}".format(thisProduct))
            sys.exit(1)
            
        elif (thisProduct["productFamily"] == "System Operation"):
            # IO cost
            thisSku = thisProduct['sku']
            thisRegion = thisProduct["attributes"]["regionCode"]
            thisIoType = 'standard'
            thisPrice = terms[thisSku]
            thisPricingDictKey = "{}|{}|{}".format('io',thisRegion,thisIoType)
            pd[thisPricingDictKey] = {'type':'io','region':thisRegion,'ioType':thisIoType,'price':thisPrice}
            # no charge for IO iopt1
            thisIoType = 'iopt1'
            thisPrice = 0.00
            thisPricingDictKey = "{}|{}|{}".format('io',thisRegion,thisIoType)
            pd[thisPricingDictKey] = {'type':'io','region':thisRegion,'ioType':thisIoType,'price':thisPrice}

        elif (thisProduct["productFamily"] == "Database Instance"):
            # Database Instance
            thisSku = thisProduct['sku']
            thisRegion = thisProduct["attributes"]["regionCode"]
            thisInstanceType = thisProduct["attributes"]["instanceType"]
            thisPrice = terms[thisSku]
            if thisProduct["attributes"]["volumeType"] in ["IO-Optimized-DocDB","NVMe SSD IO-Optimized"]:
                volumeType = 'iopt1'
            elif thisProduct["attributes"]["volumeType"] in ["General Purpose","NVMe SSD"]:
                volumeType = 'standard'
            else:
                print("*** Unknown volumeType {}, exiting".format(thisProduct["attributes"]["volumeType"]))
                sys.exit(1)
            thisPricingDictKey = "{}|{}|{}|{}".format('compute',thisRegion,thisInstanceType,volumeType)
            pd[thisPricingDictKey] = {'type':'compute','region':thisRegion,'instanceType':thisInstanceType,'price':thisPrice,'volumeType':volumeType}
             
        elif (thisProduct["productFamily"] == "Database Storage"):
            # Database Storage
            # volumeType in ['General Purpose','IO-Optimized-DocDB']
            # skip elastic clusters storage
            thisStorageUsage = thisProduct["attributes"].get('usagetype','UNKNOWN')
            if (thisProduct["attributes"].get('volumeType','UNKNOWN') in ['General Purpose','IO-Optimized-DocDB','NVMe SSD','NVMe SSD IO-Optimized']) and ('StorageUsage' in thisStorageUsage) and ('Elastic' not in thisStorageUsage):
                thisSku = thisProduct['sku']
                thisRegion = thisProduct["attributes"]["regionCode"]
                if thisProduct["attributes"]["volumeType"] in ["IO-Optimized-DocDB","NVMe SSD IO-Optimized"]:
                    thisIoType = 'iopt1'
                elif thisProduct["attributes"]["volumeType"] in ["General Purpose","NVMe SSD"]:
                    thisIoType = 'standard'
                thisPrice = terms[thisSku]
                thisPricingDictKey = "{}|{}|{}".format('storage',thisRegion,thisIoType)
                pd[thisPricingDictKey] = {'type':'storage','region':thisRegion,'ioType':thisIoType,'price':thisPrice}
            elif thisProduct["attributes"].get('volumeType','UNKNOWN') not in ['General Purpose','IO-Optimized-DocDB','NVMe SSD','NVMe SSD IO-Optimized']:
                print("*** Unknown volumeType {}, exiting".format(thisProduct["attributes"].get('volumeType','UNKNOWN')))
                sys.exit(1)
            
        elif (thisProduct["productFamily"] == "Storage Snapshot"):
            # Storage Snapshot
            thisSku = thisProduct['sku']
            thisRegion = thisProduct["attributes"]["regionCode"]
            thisPrice = terms[thisSku]
            thisPricingDictKey = "{}|{}".format('storage-snapshot',thisRegion)
            pd[thisPricingDictKey] = {'type':'storage-snapshot','region':thisRegion,'price':thisPrice}
             
        elif (thisProduct["productFamily"] == "Database Utilization"):
            # Database Utilization - EC vCPU pricing
            thisSku = thisProduct['sku']
            thisRegion = thisProduct["attributes"]["regionCode"]
            thisPrice = terms[thisSku]
            thisPricingDictKey = "{}|{}".format('ec-vcpu',thisRegion)
            pd[thisPricingDictKey] = {'type':'ec-vcpu','region':thisRegion,'price':thisPrice}
             
        elif (thisProduct["productFamily"] == "Serverless"):
            # Serverless
            thisSku = thisProduct['sku']
            thisRegion = thisProduct["attributes"]["regionCode"]
            thisPrice = terms[thisSku]
            if thisProduct["attributes"]["volume_optimization"] in ["IO-Optimized"]:
                volumeType = 'iopt1'
            elif thisProduct["attributes"]["volume_optimization"] in ["General Purpose"]:
                volumeType = 'standard'
            else:
                print("*** Unknown volumeType {}, exiting".format(thisProduct["attributes"]["volume_optimization"]))
                sys.exit(1)
            thisPricingDictKey = "{}|{}|{}".format('dcu',thisRegion,volumeType)
            pd[thisPricingDictKey] = {'type':'dcu','region':thisRegion,'price':thisPrice,'volumeType':volumeType}

        elif (thisProduct["productFamily"] == "CPU Credits"):
            # CPU Credits
            # using db.t4g.medium for all burstable [conserving cloudwatch calls - cluster only, minor price difference] but use t3g if that is all that is available
            thisRegion = thisProduct["attributes"]["regionCode"]
            if (thisProduct["attributes"]["instanceType"] == 'db.t3.medium' and 'cpu-credits|'+thisRegion not in pd):
                thisSku = thisProduct['sku']
                thisPrice = terms[thisSku]
                thisInstanceType = thisProduct["attributes"]["instanceType"]
                thisPricingDictKey = "{}|{}".format('cpu-credits',thisRegion)
                pd[thisPricingDictKey] = {'type':'cpu-credits','region':thisRegion,'price':thisPrice,'instanceType':thisInstanceType}
            elif (thisProduct["attributes"]["instanceType"] == 'db.t4g.medium'):
                thisSku = thisProduct['sku']
                thisPrice = terms[thisSku]
                thisInstanceType = thisProduct["attributes"]["instanceType"]
                thisPricingDictKey = "{}|{}".format('cpu-credits',thisRegion)
                pd[thisPricingDictKey] = {'type':'cpu-credits','region':thisRegion,'price':thisPrice,'instanceType':thisInstanceType}
             
        else:
            print("UNKNOWN - {}".format(thisProduct))
            sys.exit(1)
            
    return pd


def main():
    parser = argparse.ArgumentParser(description='DocumentDB Deployment Scanner')

    parser.add_argument('--region',required=True,type=str,help='AWS Region')
    parser.add_argument('--start-date',required=False,type=str,help='Start date for historical usage calculations, format=YYYYMMDD')
    parser.add_argument('--end-date',required=False,type=str,help='End date for historical usage calculations, format=YYYYMMDD')
    parser.add_argument('--log-file-name',required=True,type=str,help='Log file for CSV output')
                        
    args = parser.parse_args()
   
    if (args.start_date is not None and args.end_date is None):
        print("Must provide --end-date when providing --start-date, exiting.")
        sys.exit(1)

    elif (args.start_date is None and args.end_date is not None):
        print("Must provide --start-date when providing --end-date, exiting.")
        sys.exit(1)

    if (args.start_date is None) and (args.end_date is None):
        # use last 30 days
        startTime = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=30)).strftime("%Y-%m-%dT00:00:00")
        endTime = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=0)).strftime("%Y-%m-%dT00:00:00")
    else:
        # use provided start/end dates
        startTime = "{}-{}-{}T00:00:00".format(args.start_date[0:4],args.start_date[4:6],args.start_date[6:8])
        endTime = "{}-{}-{}T00:00:00".format(args.end_date[0:4],args.end_date[4:6],args.end_date[6:8])
    
    print("collecting CloudWatch data for {} to {}".format(startTime,endTime))

    appConfig = {}
    appConfig['region'] = args.region
    appConfig['logFileName'] = args.log_file_name+'.csv'
    appConfig['startTime'] = startTime
    appConfig['endTime'] = endTime

    deleteLog(appConfig)
    pricingDict = get_pricing(appConfig)
    clusterList = get_docdb_instance_based_clusters(appConfig,pricingDict)
    
    print("")
    print("Created {} with CSV data".format(appConfig['logFileName']))
    print("")


if __name__ == "__main__":
    main()
