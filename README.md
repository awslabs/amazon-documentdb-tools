# Amazon DocumentDB Tools

This repo contains the following tools.

## Amazon DocumentDB Index Tool 

The `DocumentDB Index Tool` makes it easier to migrate only indexes (not data) between a source MongoDB deployment and an Amazon DocumentDB cluster. The Index Tool can also help you find potential compatibility issues between your source databases and Amazon DocumentDB. You can use the Index Tool to dump indexes and database metadata, or you can use the tool against an existing dump created with the mongodump tool.

For more information about this tool, checkout the [Amazon DocumentDB Index Tool README](./index-tool/README.md) file.

## Amazon DocumentDB Compatibility Tool 

The `DocumentDB Compatibility Tool` examines log files from MongoDB or source code from MongoDB applications to determine if there are any queries which use operators that are not supported in Amazon DocumentDB. This tool produces a simple report of unsupported operators and file names with line numbers for further investigation.

For more information about this tool, checkout the [Amazon DocumentDB Compatibility Tool README](./compat-tool/README.md) file.

## Amazon DocumentDB Global Clusters Automation Tool

The `global-clusters-automation` is a tool created to automate the global cluster failover process for Disaster Recovery (DR) and Business Continuity Planning (BCP) use cases. It uses AWS lambda functions to trigger failover process and convert a standalone regional cluster to a global cluster.Amazon Route53 private hosted zone is used to manage cluster endpoints changes for applications. 

For more information about the Global Clusters Automation Tool, checkout the [Global Clusters Automation Tool](./global-clusters-automation/README.md) file.

## License

This library is licensed under the Apache 2.0 License. 
