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
        "title": "DatabaseCursorsTimedOut"
    }
}
VolumeWriteIOPS = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "VolumeWriteIOPs", "DBClusterIdentifier"]],
        "title": "VolumeWriteIOPS"
    }
}
VolumeReadIOPS = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "VolumeReadIOPs", "DBClusterIdentifier"]],
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
                "yAxis": {
                    "left": {
                        "max": 100,
                        "min": 0
                    }
                }
            }
        }
DatabaseConnections = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DatabaseConnections", "DBInstanceIdentifier"]],
    }
}
DatabaseCursors = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
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
        "yAxis": {
            "left": {
                "max": 100,
                "min": 0
                    }
                },
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
        "yAxis": {
            "left": {
                "max": 100,
                "min": 0
                    }
                },
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
        "yAxis": {
            "left": {
                "min": 0
                    }
                }
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
        "yAxis": {
            "left": {
                "min": 0
                    }
                },
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
        "yAxis": {
            "left": {
                "min": 0
                    }
                },
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
        "yAxis": {
            "left": {
                "min": 0
                    }
                },
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
        "yAxis": {
            "left": {
                "min": 0
                    }
                },
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
        "title": "Documents Inserted"
    }
}
DocsDeleted = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DocumentsDeleted", "DBInstanceIdentifier"]],
        "title": "Documents Deleted"
    }
}
DocsUpdated = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DocumentsUpdated", "DBInstanceIdentifier"]],
        "title": "Documents Updated"
    }
}
DocsReturned = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DocumentsReturned", "DBInstanceIdentifier"]],
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
        "title": "Read Latency"
    }
}
WriteLatency = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "WriteLatency", "DBInstanceIdentifier"]],
        "title": "Write Latency"
    }
}
DiskQueueDepth = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DiskQueueDepth", "DBInstanceIdentifier"]],
        "title": "Disk Queue Depth"
    }
}
DBInstanceReplicaLag = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "DBInstanceReplicaLag", "DBInstanceIdentifier"]],
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
        "title": "Write IOPs"
    }
}
WriteThroughput = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "WriteThroughput", "DBInstanceIdentifier"]],
        "title": "Write Throughput"
    }
}
ReadIops = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "ReadIOPS", "DBInstanceIdentifier"]],
        "title": "Read IOPs"
    }
}
ReadThroughput = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "ReadThroughput", "DBInstanceIdentifier"]],
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
        "title": "VolumeBytesUsed"
    }
}
BackupRetentionPeriodStorageUsed = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "BackupRetentionPeriodStorageUsed", "DBClusterIdentifier"]],
        "title": "BackupRetentionPeriodStorageUsed"
    }
}
TotalBackupStorageBilled = {
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [["AWS/DocDB", "TotalBackupStorageBilled", "DBClusterIdentifier"]],
        "title": "TotalBackupStorageBilled"
    }
}
