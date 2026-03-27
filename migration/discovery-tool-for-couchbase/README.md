# Discovery Tool for Couchbase
This tool gathers information from a Couchbase cluster to aid in discovery and planning for a migration to Amazon DocumentDB. This tool does not modify any data or settings and is read-only. The following information is gathered and written to .csv and .txt files:

* Details for all buckets in the cluster are written to ```bucket-stats.csv```:
    * bucket name
    * bucket type
    * item count
    * total data used (bytes)
    * total disk used (bytes)
    * total memory used (bytes)
    * RAM quota (bytes)
    * quota used percent
* If the cbstats tool is available and username/password authentication is used, details for all collections in the cluster are written to ```collection-stats.csv```:
    * bucket name
    * bucket type
    * scope name
    * collection name
    * total size (bytes)
    * total items
    * average item size (bytes)
* K/V operation statistics for all buckets in the cluster are written to ```kv-stats.csv```:
    * bucket name
    * gets/second
    * sets/second
    * deletes/second

For clusters with the query and index and services enabled:
* N1QL query statistics for the cluster are written to ```n1ql-stats.csv```:
    * selects/second
    * deletes/second
    * inserts/second
* Statistics for all indexes in the cluster are written to ```index-stats.csv```:
    * bucket name
    * scope name
    * collection name
    * index name
    * index size (bytes)
* Index definitions for all buckets in the cluster are written to ```indexes-<bucket name>.txt```. Primary index defintions are not included since all Amazon DocumentDB collections have a default primary index on ```_id```. If the bucket does not have any indexes defined, the ```indexes-<bucket name>``` file will be empty. 

## Prerequisites
The [cbstats tool](https://docs.couchbase.com/server/current/cli/cbstats-intro.html) is used to gather collection details. If cbstats is not available, collection metrics will not be gathered and you will see the following message in the output:
```
Error: cbstats not found at <tools_path>.
```

## Requirements
Python 3.9 or later

## Installation
Clone the repository and go to the Discovery Tool for Couchbase folder:
```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/migration/discovery-tool-for-couchbase/
```

## Usage/Examples
The script has the following arguments:
```
--data_node             Couchbase data node IP address or DNS name
--username              Couchbase cluster username (required if not using client certificates)
--password              Couchbase cluster password (required if not using client certificates)
--client_cert           Path to client certificate file (required if not using username/password)
--client_key            Path to client private key file (required if not using username/password)
--admin_port            administration REST port (default: 18091)
--kv_zoom               get bucket statistics for specified interval: <minute | hour | day | week | month | year> (default: month)
--tools_path            full path to Couchbase tools (default: /opt/couchbase/bin)
--index_metrics         gather index definitions and N1QL metrics: <true | false> (default: false)
--indexer_port          indexer service REST port (default: 19102)
--n1ql_start            number of milliseconds prior at which to start sampling (default: -60000)
--n1ql_step             sample interval over the sample period, in milliseconds (default: 100)
--skip_ssl_verify       Skip SSL certificate verification (not recommended for production)
```

### Example - username/password authentication
#### HTTPS (Couchbase Server 7.2+)
If your Couchbase cluster is using self-signed certificates add the ```skip_ssl_verify`` flag.
```
python3 discover.py --data_node "https://<hostname or IP>" --username <username> --password <password> --index_metrics true
```

#### HTTP (Couchbase Server 7.0+)
Change admin and indexer port values to the unencrypred port number values.
```
python3 discover.py --data_node "http://<hostname or IP>" --username <username> --password <password> --admin_port 8091 --index_metrics true --indexer_port 9102
```

### Example - certificate authentication (Couchbase Server 7.2+)
If your Couchbase cluster is using self-signed certificates add the ```skip_ssl_verify`` flag.

> [!IMPORTANT]
> Collection metrics will not be gathered when using certificate authentication since cbstats does not support certificate authentication.
```
python3 discover.py --data_node "https://<hostname or IP>" --client_cert client-cert.pem --client_key client-key.pem --index_metrics true
```

## Sample output

In this example, the ```beer-sample``` and ```travel-sample``` buckets have been loaded and there is a ```pillowfight``` bucket being used for [cbc-pillowfight](https://docs.couchbase.com/sdk-api/couchbase-c-client/md_doc_2cbc-pillowfight.html) and [n1qlback](https://docs.couchbase.com/sdk-api/couchbase-c-client/md_doc_2cbc-n1qlback.html) load testing.

The tool generates the following output while executing:
```
found data nodes ['10.0.129.165', '10.0.130.123', '10.0.133.73']
found buckets ['beer-sample', 'pillowfight', 'travel-sample']

getting bucket stats via REST API...
getting statistics for bucket beer-sample...
getting statistics for bucket pillowfight...
getting statistics for bucket travel-sample...

getting collection stats...
found collection beer-sample._default._default
found collection pillowfight._default._default
found collection travel-sample.inventory.airport
found collection travel-sample.inventory.airline
found collection travel-sample.inventory.route
found collection travel-sample.inventory.landmark
found collection travel-sample.inventory.hotel
found collection travel-sample.tenant_agent_00.users
found collection travel-sample.tenant_agent_00.bookings
found collection travel-sample.tenant_agent_01.users
found collection travel-sample.tenant_agent_01.bookings
found collection travel-sample.tenant_agent_02.bookings
found collection travel-sample.tenant_agent_02.users
found collection travel-sample.tenant_agent_03.users
found collection travel-sample.tenant_agent_03.bookings
found collection travel-sample.tenant_agent_04.bookings
found collection travel-sample.tenant_agent_04.users
found collection travel-sample._default._default

getting K/V stats...

getting KV stats for last month for bucket beer-sample...
cmd_get: 0/second
cmd_set: 0/second
delete_hits: 0/second

getting KV stats for last month for bucket pillowfight...
cmd_get: 397/second
cmd_set: 549/second
delete_hits: 217/second

getting KV stats for last month for bucket travel-sample...
cmd_get: 0/second
cmd_set: 0/second
delete_hits: 0/second

found index nodes ['10.0.132.125', '10.0.150.144']

getting index definitions...
found 0 indexes in bucket beer-sample
found 0 indexes in bucket pillowfight
found 17 indexes in bucket travel-sample

getting index stats for bucket beer-sample
getting index stats for bucket pillowfight
getting index stats for bucket travel-sample

getting N1QL stats every 1000 ms for -60000 ms...
n1ql_selects: 0/second
n1ql_deletes: 152/second
n1ql_inserts: 99/second
```

The output files contain the following information:
#### bucket-stats.csv
```
bucket,bucket_type,item_count,data_used,disk_used,mem_used,ram_quota,quota_used_percent
beer-sample,membase,7303,7620888,30863131,86389304,629145600,13.73
gamesim-sample,membase,586,1194770,19834565,75258056,629145600,11.96
pillowfight,membase,1000006,3874707316,5219114640,6332467768,216076910592,2.93
travel-sample,membase,63288,96731384,143680696,183310344,629145600,29.14
```

#### collection-stats.csv
```
bucket,bucket_type,scope_name,collection_name,total_size,total_items,document_size
beer-sample,membase,_default,_default,2796956,7303,383
pillowfight,membase,_default,_default,1901907730,1000005,1902
travel-sample,membase,inventory,airport,547914,1968,279
travel-sample,membase,inventory,airline,117261,187,628
travel-sample,membase,inventory,route,13402503,24024,558
travel-sample,membase,inventory,landmark,3072746,4495,684
travel-sample,membase,inventory,hotel,4086989,917,4457
travel-sample,membase,tenant_agent_00,users,88173,2,44087
travel-sample,membase,tenant_agent_00,bookings,87040,0,0
travel-sample,membase,tenant_agent_01,users,93163,11,8470
travel-sample,membase,tenant_agent_01,bookings,89088,0,0
travel-sample,membase,tenant_agent_02,bookings,89088,0,0
travel-sample,membase,tenant_agent_02,users,98134,20,4907
travel-sample,membase,tenant_agent_03,users,105583,33,3200
travel-sample,membase,tenant_agent_03,bookings,89088,0,0
travel-sample,membase,tenant_agent_04,bookings,87040,0,0
travel-sample,membase,tenant_agent_04,users,107629,40,2691
travel-sample,membase,_default,_default,20780949,31591,658
```

### kv-stats.csv
```
bucket,gets,sets,deletes
beer-sample,0,0,0
pillowfight,397,549,217
travel-sample,0,0,0
```

### n1ql-stats.csv
```
selects,deletes,inserts
0,152,99
```

### index-stats.csv
```
bucket,scope,collection,index-name,index-size
beer-sample,_default,_default,beer_primary,479061
travel-sample,_default,_default,def_airportname,389408
travel-sample,_default,_default,def_city,1029476
travel-sample,_default,_default,def_faa,367120
travel-sample,_default,_default,def_icao,387678
travel-sample,_default,_default,def_name_type,79948
travel-sample,_default,_default,def_primary,1140554
travel-sample,_default,_default,def_route_src_dst_day,16235078
travel-sample,_default,_default,def_schedule_utc,13864561
travel-sample,_default,_default,def_sourceairport,2429464
travel-sample,_default,_default,def_type,3628526
travel-sample,inventory,airline,def_inventory_airline_primary,198473
travel-sample,inventory,airport,def_inventory_airport_airportname,515968
travel-sample,inventory,airport,def_inventory_airport_city,489507
travel-sample,inventory,airport,def_inventory_airport_faa,529491
travel-sample,inventory,airport,def_inventory_airport_primary,288326
travel-sample,inventory,hotel,def_inventory_hotel_city,498513
travel-sample,inventory,hotel,def_inventory_hotel_primary,227093
travel-sample,inventory,landmark,def_inventory_landmark_city,957396
travel-sample,inventory,landmark,def_inventory_landmark_primary,365002
travel-sample,inventory,route,def_inventory_route_primary,832154
travel-sample,inventory,route,def_inventory_route_route_src_dst_day,13978936
travel-sample,inventory,route,def_inventory_route_schedule_utc,13461388
travel-sample,inventory,route,def_inventory_route_sourceairport,2405883
```

In this example, only the ```travel-sample``` bucket has indexes.
### indexes-travel-sample.txt
```
CREATE INDEX `def_airportname` ON `travel-sample`(`airportname`)
CREATE INDEX `def_city` ON `travel-sample`(`city`)
CREATE INDEX `def_faa` ON `travel-sample`(`faa`)
CREATE INDEX `def_icao` ON `travel-sample`(`icao`)
CREATE INDEX `def_inventory_airport_airportname` ON `travel-sample`.`inventory`.`airport`(`airportname`)
CREATE INDEX `def_inventory_airport_city` ON `travel-sample`.`inventory`.`airport`(`city`)
CREATE INDEX `def_inventory_airport_faa` ON `travel-sample`.`inventory`.`airport`(`faa`)
CREATE INDEX `def_inventory_hotel_city` ON `travel-sample`.`inventory`.`hotel`(`city`)
CREATE INDEX `def_inventory_landmark_city` ON `travel-sample`.`inventory`.`landmark`(`city`)
CREATE INDEX `def_inventory_route_route_src_dst_day` ON `travel-sample`.`inventory`.`route`(`sourceairport`,`destinationairport`,(distinct (array (`v`.`day`) for `v` in `schedule` end)))
CREATE INDEX `def_inventory_route_schedule_utc` ON `travel-sample`.`inventory`.`route`(array (`s`.`utc`) for `s` in `schedule` end)
CREATE INDEX `def_inventory_route_sourceairport` ON `travel-sample`.`inventory`.`route`(`sourceairport`)
CREATE INDEX `def_name_type` ON `travel-sample`(`name`) WHERE (`_type` = "User")
CREATE INDEX `def_route_src_dst_day` ON `travel-sample`(`sourceairport`,`destinationairport`,(distinct (array (`v`.`day`) for `v` in `schedule` end))) WHERE (`type` = "route")
CREATE INDEX `def_schedule_utc` ON `travel-sample`(array (`s`.`utc`) for `s` in `schedule` end)
CREATE INDEX `def_sourceairport` ON `travel-sample`(`sourceairport`)
CREATE INDEX `def_type` ON `travel-sample`(`type`)
```

## Contributing
Contributions are always welcome! See the [contributing page](https://github.com/awslabs/amazon-documentdb-tools/blob/master/CONTRIBUTING.md) for ways to get involved.

## License
Apache 2.0