# Amazon DocumentDB Tools

This repo contains tools to make migrating to Amazon DocumentDB (with MongoDB compatibility) easier.

## Amazon DocumentDB Index Tool 

The `DocumentDB Index Tool` makes it easier to migrate only indexes (not data) between a source MongoDB deployment and a Amazon DocumentDB  cluster. The Index Tool can also help you find potential compatibility issues between your source databases and Amazon DocumentDB. You can use the Index Tool to dump indexes and database metadata, or you can use the tool against an existing dump created with the mongodump tool.

For more information about this tool, checkout the [Amazon DocumentDB Index Tool README](./index-tool/README.md) file.

## Cosmos DB Migrator

The `Cosmos DB Migrator` is an application created to help live migrate the Azure Cosmos DB for MongoDB API databases to Amazon DocumentDB with very little downtime. It keeps the target Amazon DocumentDB cluster in sync with the source Microsoft Azure Cosmos DB until the client applications are cut over to the DocumentDB cluster. 

For more information about the Cosmos DB Migrator tool, checkout the [Cosmos DB Migrator README](./cosmos-db-migrator/README.md) file.

## License

This library is licensed under the Apache 2.0 License. 
