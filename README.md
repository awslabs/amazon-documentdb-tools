# Amazon DocumentDB Tools

This repository contains several tools to help users with Amazon DocumentDB including migration, monitoring, and performance. A few of the most popular tools are listed below but there are additional tools in the [migration](./migration), [monitoring](./monitoring), [operations](./operations), and [performance](./performance) folders.

## Amazon DocumentDB Compatibility Tool 

The [DocumentDB Compatibility Tool](./compat-tool) examines log files from MongoDB or source code from MongoDB applications to determine if there are any queries which use operators that are not supported in Amazon DocumentDB.

## Amazon DocumentDB Index Tool 

The [DocumentDB Index Tool](./index-tool) makes it easy to migrate only indexes (not data) between a source MongoDB deployment and an Amazon DocumentDB cluster.

## Prism for Amazon DocumentDB 

[Prism](./performance/prism) is an AI-powered automated Well-Architected analysis and advisory tool for Amazon DocumentDB. It provides a fleet-wide view of every cluster in a region, drills down to per-collection index and compression insights, runs a Well-Architected review across all 6 AWS pillars, and can review your application source against Amazon DocumentDB client best practices.

## Support

The contents of this repository are maintained by Amazon DocumentDB Specialist SAs and are not officially supported by AWS. Please file a [Github Issue](https://github.com/awslabs/amazon-documentdb-tools/issues) if you experience any problems.

## License

This library is licensed under the Apache 2.0 License. 
