# CLUSTER LEVEL METRICS
ClusterHeading = {
    "type": "text",
    "properties": {"markdown": "# Cluster Level Metrics"}
}

# ADDITIONAL HELP METRICS
metricHelp = {
    "type": "text",
    "properties": {"markdown": "### Metrics Overview\nLearn more about metric information by visiting the Amazon DocumentDB Metrics section [here](https://docs.aws.amazon.com/documentdb/latest/developerguide/cloud_watch.html#cloud_watch-metrics_list)\n"}}
bestPractices = {
    "type": "text",
    "properties": {"markdown": "### DocumentDB Specialist Optimization Tips\nLearn how to optimize your workload by visiting the DocDB Specialist recommended guidelines [here](https://docs.aws.amazon.com/documentdb/latest/developerguide/best_practices.html)"}}

# ----------------------------------------------
DBClusterReplicaLagMaximum = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DBClusterReplicaLagMaximum", "DBClusterIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "DBClusterReplicaLagMaximum"
    }
}
DatabaseCursorsTimedOut = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DatabaseCursorsTimedOut", "DBClusterIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},
        "title": "DatabaseCursorsTimedOut"
    }
}
VolumeWriteIOPS = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "VolumeWriteIOPs", "DBClusterIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "VolumeWriteIOPS"
    }
}
VolumeReadIOPS = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "VolumeReadIOPs", "DBClusterIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "VolumeReadIOPS"
    }
}

# ----------------------------------------------
OpscountersInsert = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "OpcountersInsert", "DBClusterIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},
        "title": "OpcountersInsert"
    }
}
OpscountersUpdate = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "OpcountersUpdate", "DBClusterIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},
        "title": "OpcountersUpdate"
    }
}
OpscountersDelete = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "OpcountersDelete", "DBClusterIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},
        "title": "OpcountersDelete"
    }
}
OpscountersQuery = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "OpcountersQuery", "DBClusterIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},
        "title": "OpcountersQuery"
    }
}

# INSTANCE LEVEL METRICS
InstanceHeading = {
    "type": "text",
    "properties": {"markdown": "# Instance Level Metrics"}
}

CPUUtilization = {
    "type": "metric",
    "properties": {
        "metrics": [["AWS/DocDB", "CPUUtilization", "DBInstanceIdentifier"]],
        "view": "timeSeries",
                "stacked": False,
                "title": "CPU Utilization",
                "period": 300,
                "stat": "Average",
                "yAxis": {"left": {"max": 100,"min": 0}}
            }
        }
DatabaseConnections = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "yAxis": {"left": {"min": 0}},
        "metrics": [["AWS/DocDB", "DatabaseConnections", "DBInstanceIdentifier"]],
    }
}
DatabaseCursors = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "yAxis": {"left": {"min": 0}},
        "metrics": [["AWS/DocDB", "DatabaseCursors", "DBInstanceIdentifier"]],
    }
}

# ----------------------------------------------
BufferCacheHitRatio = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "BufferCacheHitRatio", "DBInstanceIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"max": 100,"min": 0}},
        "title": "Buffer Cache Hit Ratio"
            }
        }
IndexBufferCacheHitRatio = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "IndexBufferCacheHitRatio", "DBInstanceIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"max": 100,"min": 0}},
        "title": "Index Buffer Cache Hit Ratio"
            }
        }
FreeableMemory = {
    "type": "metric",
    "properties": {
        "sparkline": True,
        "view": "timeSeries",
        "metrics": [["AWS/DocDB", "FreeableMemory", "DBInstanceIdentifier"]],
        "title": "Freeable Memory",
        "period": 300,
        "stacked": False,
        "yAxis": {"left": {"min": 0}}
            }
        }
FreeLocalStorage = {
    "type": "metric",
    "properties": {
        "sparkline": True,
        "view": "timeSeries",
        "metrics": [["AWS/DocDB", "FreeLocalStorage", "DBInstanceIdentifier"]],
        "title": "Free Local Storage",
        "period": 300,
        "stacked": False,
        "yAxis": {"left": {"min": 0}}
            }
        }

# ----------------------------------------------
NetworkTransmitThroughput = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "NetworkTransmitThroughput", "DBInstanceIdentifier"]
        ],
        "period": 300,
        "yAxis": {"left": {"min": 0}},
        "title": "Network Transmit Throughput"
            }
        }
NetworkReceiveThroughput = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "NetworkReceiveThroughput", "DBInstanceIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},
        "title": "Network Receive Throughput"
            }
        }

# ----------------------------------------------
StorageNetworkTransmitThroughput = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "StorageNetworkTransmitThroughput", "DBInstanceIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},
         "title": "Storage Network Transmit Throughput"
            }
        }
StorageNetworkReceiveThroughput = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "StorageNetworkReceiveThroughput", "DBInstanceIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},
        "title": "Storage Network Receive Throughput"
            }
        }

# ----------------------------------------------
DocsInserted = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DocumentsInserted", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Documents Inserted"
    }
}
DocsDeleted = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DocumentsDeleted", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Documents Deleted"
    }
}
DocsUpdated = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DocumentsUpdated", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Documents Updated"
    }
}
DocsReturned = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DocumentsReturned", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Documents Returned"
    }
}

# ----------------------------------------------
ReadLatency = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "ReadLatency", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Read Latency"
    }
}
WriteLatency = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "WriteLatency", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Write Latency"
    }
}
DiskQueueDepth = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DiskQueueDepth", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Disk Queue Depth"
    }
}
DBInstanceReplicaLag = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DBInstanceReplicaLag", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Replica Lag"
    }
}

# ----------------------------------------------
WriteIops = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "WriteIOPS", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Write IOPs"
    }
}
WriteThroughput = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "WriteThroughput", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Write Throughput"
    }
}
ReadIops = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "ReadIOPS", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Read IOPs"
    }
}
ReadThroughput = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "ReadThroughput", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Read Throughput"
    }
}

# BACKUP AND STORAGE METRICS
BackupStorageHeading = {
    "type": "text",
    "properties": {"markdown": "# Backup and Storage Metrics"}
}

# ----------------------------------------------
VolumeBytesUsed = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "VolumeBytesUsed", "DBClusterIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "VolumeBytesUsed"
    }
}
BackupRetentionPeriodStorageUsed = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "BackupRetentionPeriodStorageUsed", "DBClusterIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "BackupRetentionPeriodStorageUsed"
    }
}
TotalBackupStorageBilled = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "TotalBackupStorageBilled", "DBClusterIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "TotalBackupStorageBilled"
    }
}

# NVME 1 ----------------------------------------------
NVMeHeading = {
    "type": "text",
    "properties": {"markdown": "# NVMe-Backed Instances"}
}
# NVME 2 ----------------------------------------------
FreeNVMeStorage = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "FreeNVMeStorage", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Free NVMe Storage"
    }
}
NVMeStorageCacheHitRatio = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "NVMeStorageCacheHitRatio", "DBInstanceIdentifier"]],
        "title": "NVMe Storage Cache Hit Ratio",
        "yAxis": {"left": {"max": 100,"min": 0}}
    }
}
# NVME 3 ----------------------------------------------
ReadIopsNVMeStorage = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "ReadIOPSNVMeStorage", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Read IOPs NVMe Storage"
    }
}
ReadLatencyNVMeStorage = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "ReadLatencyNVMeStorage", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Read Latency NVMe Storage"
    }
}
ReadThroughputNVMeStorage = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "ReadThroughputNVMeStorage", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Read Throughput NVMe Storage"
    }
}
# NVME 4 ----------------------------------------------
WriteIopsNVMeStorage = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "WriteIOPSNVMeStorage", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Write IOPs NVMe Storage"
    }
}
WriteLatencyNVMeStorage = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "WriteLatencyNVMeStorage", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Write Latency NVMe Storage"
    }
}
WriteThroughputNVMeStorage = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "WriteThroughputNVMeStorage", "DBInstanceIdentifier"]],
        "yAxis": {"left": {"min": 0}},
        "title": "Write Throughput NVMe Storage"
    }
}

# ----------------------------------------------
# MongoDB to DocumentDB Migration Monitoring Widgets
# ----------------------------------------------

# Migration Monitoring Heading
MigrationMonitoringHeading = {
    "type": "text",
    "properties": {"markdown": "# MongoDB to DocumentDB Migration Monitoring"}
}

# Full Load Migration Metrics
# ----------------------------------------------
FullLoadMigrationHeading = {
    "type": "text",
    "properties": {"markdown": "## Full Load Migration Metrics"}
}

MigratorFLInsertsPerSecond = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["CustomDocDB", "MigratorFLInsertsPerSecond", "Cluster", "DBClusterIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},  # No max to allow auto-scaling
        "title": "Migration Operations Per Second",
        "annotations": {
            "horizontal": [
                {
                    "label": "High Throughput",
                    "value": 1000,
                    "color": "#2ca02c"
                },
                {
                    "label": "Medium Throughput",
                    "value": 500,
                    "color": "#ffbb78"
                }
            ]
        }
    }
}


MigratorFLRemainingSeconds = {
    "type": "metric",
    "properties": {
        "view": "singleValue",
        "metrics": [["CustomDocDB", "MigratorFLRemainingSeconds", "Cluster", "DBClusterIdentifier"]],
        "period": 300,
        "title": "Remaining Time (seconds)",
        "stat": "Average",
        "setPeriodToTimeRange": True,
        "trend": True
    }
}

# MigrationRemainingSecondsGraph = {
#     "type": "metric",
#     "properties": {
#         "view": "gauge",
#         "metrics": [
#             ["CustomDocDB", "MigratorFLRemainingSeconds", "Cluster", "DBClusterIdentifier"]
#         ],
#         "period": 60,
#         "yAxis": {
#             "left": {
#                 "min": 0,
#                 "max": 3600
#             }
#         },
#         "gauge": {
#             "percentageMode": False,
#             "minValue": 0,
#             "maxValue": 3600
#         },
#         "title": "Migration Time Remaining",
#         "stat": "Average",
#         "setPeriodToTimeRange": True,
#         "region": "us-east-1",
#         "thresholds": [
#             {
#                 "color": "#2ca02c",
#                 "label": "Almost Done",
#                 "value": 600
#             },
#             {
#                 "color": "#98df8a",
#                 "label": "Good Progress",
#                 "value": 1800
#             },
#             {
#                 "color": "#ffbb78",
#                 "label": "In Progress",
#                 "value": 3000
#             },
#             {
#                 "color": "#ff9896",
#                 "label": "Just Started",
#                 "value": 3600
#             }
#         ]
#     }
# }


# CDC Replication Metrics
# ----------------------------------------------
CDCReplicationHeading = {
    "type": "text",
    "properties": {"markdown": "## CDC Replication Metrics"}
}

MigratorCDCOperationsPerSecond = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["CustomDocDB", "MigratorCDCOperationsPerSecond", "Cluster", "DBClusterIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},  # Removed max to allow auto-scaling
        "title": "CDC Operations Per Second",
        "annotations": {
            "horizontal": [
                {
                    "label": "High Throughput",
                    "value": 500,
                    "color": "#2ca02c"
                },
                {
                    "label": "Medium Throughput",
                    "value": 100,
                    "color": "#ffbb78"
                }
            ]
        }
    }
}

MigratorCDCNumSecondsBehind = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["CustomDocDB", "MigratorCDCNumSecondsBehind", "Cluster", "DBClusterIdentifier"]],
        "period": 300,
        "yAxis": {"left": {"min": 0}},  # No max to allow auto-scaling
        "title": "CDC Replication Lag (seconds)",
        "annotations": {
            "horizontal": [
                {
                    "label": "Critical Lag",
                    "value": 3600,
                    "color": "#ff9896"
                },
                {
                    "label": "High Lag",
                    "value": 900,
                    "color": "#ffbb78"
                },
                {
                    "label": "Moderate Lag",
                    "value": 300,
                    "color": "#98df8a"
                },
                {
                    "label": "Low Lag",
                    "value": 60,
                    "color": "#2ca02c"
                }
            ]
        }
    }
}




# ----------------------------------------------
# DMS Task Metrics
# ----------------------------------------------

DMSHeading = {
    "type": "text",
    "properties": {"markdown": "# AWS DMS Task Metrics"}
}

DMSFullLoadThroughputRowsTarget = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DMS", "FullLoadThroughputRowsTarget", "ReplicationInstanceIdentifier", "instance_id", "ReplicationTaskIdentifier", "TASK_ID"]],
        "period": 60,
        "yAxis": {"left": {"min": 0}},
        "title": "Full Load Throughput Rows Target"
    }
}

DMSCDCLatencyTarget = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DMS", "CDCLatencyTarget","ReplicationInstanceIdentifier", "instance_id", "ReplicationTaskIdentifier", "TASK_ID"]],
        "period": 60,
        "yAxis": {"left": {"min": 0}},
        "title": "CDC Latency Target (seconds)"
    }
}

DMSCDCThroughputRowsTarget = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DMS", "CDCThroughputRowsTarget", "ReplicationInstanceIdentifier", "instance_id","ReplicationTaskIdentifier", "TASK_ID"]],
        "period": 60,
        "yAxis": {"left": {"min": 0}},
        "title": "CDC Throughput Rows Target"
    }
}

