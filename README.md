# Amazon DocumentDB Tools

This repository contains several tools to help users with Amazon DocumentDB including migration, monitoring, and performance. A few of the most popular tools are listed below but there are additional tools in the [migration](./migration), [monitoring](./monitoring), [operations](./operations), and [performance](./performance) folders.

## Amazon DocumentDB Index Tool 

The [DocumentDB Index Tool](./index-tool) makes it easy to migrate only indexes (not data) between a source MongoDB deployment and an Amazon DocumentDB cluster.

## Amazon DocumentDB Compatibility Tool 

The [DocumentDB Compatibility Tool](./compat-tool) examines log files from MongoDB or source code from MongoDB applications to determine if there are any queries which use operators that are not supported in Amazon DocumentDB.

## Amazon DocumentDB Global Clusters Automation Tool

The [global-clusters-automation](./global-clusters-automation) automates the global cluster failover process for Disaster Recovery (DR) and Business Continuity Planning (BCP) use cases.

## Support

The contents of this repository are maintained by Amazon DocumentDB Specialist SAs and are not officially supported by AWS. Please file a [Github Issue](https://github.com/awslabs/amazon-documentdb-tools/issues) if you experience any problems.

## License

This library is licensed under the Apache 2.0 License. 
