# CLUSTER LEVEL METRICS
ClusterHeading = {
    "type": "text",
    "properties": {
        "markdown": "# Cluster Level Metrics"
    }
}

# ADDITIONAL HELP METRICS
metricHelp = {
            "height": 2,
            "width": 12,
            "y": 1,
            "x": 0,
            "type": "text",
            "properties": {
                "markdown": "### Metrics Overview\nLearn more about metric information by visiting the Amazon DocumentDB Metrics section [here](https://docs.aws.amazon.com/documentdb/latest/developerguide/cloud_watch.html#cloud_watch-metrics_list)\n"
            }
        }
bestPractices = {
            "height": 2,
            "width": 12,
            "y": 1,
            "x": 12,
            "type": "text",
            "properties": {
                "markdown": "### DocumentDB Specialist Optimization Tips\nLearn how to optimize your workload by visiting the DocDB Specialist recommended guidelines [here](https://docs.aws.amazon.com/documentdb/latest/developerguide/best_practices.html)"
            }
        }

DBClusterReplicaLagMaximum = {
    "height": 7,
    "width": 6,
    "y": 3,
    "x": 0,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "DBClusterReplicaLagMaximum", "DBClusterIdentifier"]
        ],
        "title": "DBClusterReplicaLagMaximum"
    }
}
DatabaseCursorsTimedOut = {
    "height": 7,
    "width": 6,
    "y": 3,
    "x": 6,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "DatabaseCursorsTimedOut", "DBClusterIdentifier"]
        ],
        "period": 300,
        "title": "DatabaseCursorsTimedOut"
    }
}
VolumeWriteIOPS = {
    "height": 7,
    "width": 6,
    "y": 3,
    "x": 12,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "VolumeWriteIOPs", "DBClusterIdentifier"]
        ],
        "title": "VolumeWriteIOPS"
    }
}
VolumeReadIOPS = {
    "height": 7,
    "width": 6,
    "y": 3,
    "x": 18,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "VolumeReadIOPs", "DBClusterIdentifier"]
        ],
        "title": "VolumeReadIOPS"
    }
}


OpscountersInsert = {
    "height": 7,
    "width": 6,
    "y": 10,
    "x": 0,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "OpcountersInsert", "DBClusterIdentifier"]
        ],
        "period": 300,
        "title": "OpcountersInsert"
    }
}
OpscountersUpdate = {
    "height": 7,
    "width": 6,
    "y": 10,
    "x": 6,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "OpcountersUpdate", "DBClusterIdentifier"]
        ],
        "period": 300,
        "title": "OpcountersUpdate"
    }
}
OpscountersDelete = {
    "height": 7,
    "width": 6,
    "y": 10,
    "x": 12,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "OpcountersDelete", "DBClusterIdentifier"]
        ],
        "period": 300,
        "title": "OpcountersDelete"
    }
}
OpscountersQuery = {
    "height": 7,
    "width": 6,
    "y": 10,
    "x": 18,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "OpcountersQuery", "DBClusterIdentifier"]
        ],
        "period": 300,
        "title": "OpcountersQuery"
    }
}


# INSTANCE LEVEL METRICS
InstanceHeading = {
    "height": 1,
    "width": 24,
    "y": 20,
    "x": 0,
    "type": "text",
    "properties": {
        "markdown": "# Instance Level Metrics"
    }
}

CPUUtilization = {
    "height": 7,
    "width": 8,
    "y": 21,
    "x": 0,
    "type": "metric",
    "properties": {
        "metrics": [
            ["AWS/DocDB", "CPUUtilization", "DBInstanceIdentifier"]
        ],
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
    "height": 7,
    "width": 8,
    "y": 21,
    "x": 8,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "DatabaseConnections", "DBInstanceIdentifier"]
        ],
    }
}
DatabaseCursors = {
    "height": 7,
    "width": 8,
    "y": 21,
    "x": 16,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "DatabaseCursors", "DBInstanceIdentifier"]
        ],
    }
}

IndexBufferCacheHitRatio = {
    "height": 7,
    "width": 8,
    "y": 30,
    "x": 8,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "IndexBufferCacheHitRatio", "DBInstanceIdentifier"]
        ],
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
BufferCacheHitRatio = {
    "height": 7,
    "width": 8,
    "y": 30,
    "x": 0,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "BufferCacheHitRatio", "DBInstanceIdentifier"]
        ],
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
FreeableMemory = {
    "height": 7,
    "width": 8,
    "y": 30,
    "x": 16,
    "type": "metric",
    "properties": {
        "sparkline": True,
        "view": "timeSeries",
        "metrics": [
            ["AWS/DocDB", "FreeableMemory", "DBInstanceIdentifier"]
        ],
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

NetworkReceiveThroughput = {
    "height": 7,
    "width": 12,
    "y": 38,
    "x": 12,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "NetworkReceiveThroughput", "DBInstanceIdentifier"]
        ],
        "period": 300,
        "yAxis": {
            "left": {
                "min": 0
                    }
                },
        "title": "Network Receive Throughput"
            }
        }
NetworkTransmitThroughput = {
    "height": 7,
    "width": 12,
    "y": 38,
    "x": 0,
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

StorageNetworkReceiveThroughput = {
    "height": 7,
    "width": 12,
    "y": 45,
    "x": 12,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "StorageNetworkReceiveThroughput", "DBInstanceIdentifier"]
        ],
        "period": 300,
        "yAxis": {
            "left": {
                "min": 0
                    }
                },
        "title": "Storage Network Receive Throughput"
            }
        }
StorageNetworkTransmitThroughput = {
    "height": 7,
    "width": 12,
    "y": 45,
    "x": 0,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "StorageNetworkTransmitThroughput", "DBInstanceIdentifier"]
        ],
        "period": 300,
        "yAxis": {
            "left": {
                "min": 0
                    }
                },
                "title": "Storage Network Transmit Throughput"
            }
        }


DocsInserted = {
            "height": 6,
            "width": 6,
            "y": 52,
            "x": 0,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "DocumentsInserted", "DBInstanceIdentifier"]
                ],
                "title": "Documents Inserted"
            }
        }
DocsDeleted = {
            "height": 6,
            "width": 6,
            "y": 52,
            "x": 6,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "DocumentsDeleted", "DBInstanceIdentifier"]
                ],
                "title": "Documents Deleted"
            }
        }
DocsUpdated = {
            "height": 6,
            "width": 6,
            "y": 52,
            "x": 12,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "DocumentsUpdated", "DBInstanceIdentifier"]
                ],
                "title": "Documents Updated"
            }
        }
DocsReturned = {
            "height": 6,
            "width": 6,
            "y": 52,
            "x": 18,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "DocumentsReturned", "DBInstanceIdentifier"]
                ],
                "title": "Documents Returned"
            }
}

ReadLatency = {
            "height": 6,
            "width": 6,
            "y": 58,
            "x": 0,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "ReadLatency", "DBInstanceIdentifier"]
                ],
                "title": "Read Latency"
            }
        }
WriteLatency = {
            "height": 6,
            "width": 6,
            "y": 58,
            "x": 6,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "WriteLatency", "DBInstanceIdentifier"]
                ],
                "title": "Write Latency"
            }
        }
DiskQueueDepth = {
            "height": 6,
            "width": 6,
            "y": 58,
            "x": 12,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "DiskQueueDepth", "DBInstanceIdentifier"]
                ],
                "title": "Disk Queue Depth"
            }
        }
DBInstanceReplicaLag = {
            "height": 6,
            "width": 6,
            "y": 58,
            "x": 18,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "DBInstanceReplicaLag", "DBInstanceIdentifier"]
                ],
                "title": "Replica Lag"
            }
        }

WriteIops = {
            "height": 6,
            "width": 6,
            "y": 64,
            "x": 0,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "WriteIOPS", "DBInstanceIdentifier"]
                ],
                "title": "Write IOPs"
            }
        }
WriteThroughput = {
            "height": 6,
            "width": 6,
            "y": 64,
            "x": 6,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "WriteThroughput", "DBInstanceIdentifier"]
                ],
                "title": "Write Throughput"
            }
        }
ReadIops = {
            "height": 6,
            "width": 6,
            "y": 64,
            "x": 12,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "ReadIOPS", "DBInstanceIdentifier"]
                ],
                "title": "Read IOPs"
            }
        }
ReadThroughput = {
            "height": 6,
            "width": 6,
            "y": 64,
            "x": 18,
            "type": "metric",
            "properties": {
                "view": "timeSeries",
                "stacked": False,
                "metrics": [
                    ["AWS/DocDB", "ReadThroughput", "DBInstanceIdentifier"]
                ],
                "title": "Read Throughput"
            }
}


# BACKUP AND STORAGE METRICS
BackupStorageHeading = {
    "height": 1,
    "width": 24,
    "y": 70,
    "x": 0,
    "type": "text",
    "properties": {
        "markdown": "# Backup and Storage Metrics"
    }
}
BackupRetentionPeriodStorageUsed = {
    "height": 7,
    "width": 8,
    "y": 71,
    "x": 8,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "BackupRetentionPeriodStorageUsed", "DBClusterIdentifier"]
        ],
        "title": "BackupRetentionPeriodStorageUsed"
    }
}
TotalBackupStorageBilled = {
    "height": 7,
    "width": 8,
    "y": 71,
    "x": 16,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "TotalBackupStorageBilled", "DBClusterIdentifier"]
        ],
        "title": "TotalBackupStorageBilled"
    }
}
VolumeBytesUsed = {
    "height": 7,
    "width": 8,
    "y": 71,
    "x": 0,
    "type": "metric",
    "properties": {
        "view": "timeSeries",
        "stacked": False,
        "metrics": [
            ["AWS/DocDB", "VolumeBytesUsed", "DBClusterIdentifier"]
        ],
        "title": "VolumeBytesUsed"
    }
}

