# Python Updater tool 
This sample applications compresses pre-existing documents in an existing collection after compression is turned on that existing collection.

Single threaded application - issues **5000** (controlled by argument --batch-size) updates serially in a _round_, and sleeps for **60** (controlled by argument --wait-period) seconds before starting next _round_.

Status of the updates are maintained in database **tracker_db** - for each collection there is a tracker collection named **<< collection >>__tracker_col**.

The application can be restarted if it crashes and it will pick up from last successful _round_ based on data in **<< collection >>__tracker_col**.

The update statements use field **6nh63** (controlled by argument --update-field), for triggering compression on existing records.

The application uses **_id** field for tracking and updating existing documents. If you are using a custom value _id, the value should be sort-able.

## Requirements
Python 3.7 or later, pymongo

## Installation
Clone the repository and go to the application folder:
```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/operations/document-compression-updater
```

## Usage/Examples

```
 python3 update_apply_compression.py --uri "<<documentdb_uri>>"  --database <<database>>   --collection <<collection>> --update-field << field_name >> --wait-period << int >>> --batch-size << int >>
```

The application has the following arguments:

```
Required parameters
  --uri URI                                      URI (connection string)
  --database DATABASE                            Database
  --collection COLLECTION                        Collection

Optional parameters
 --file-name                                    Starting name of the created log files
 --update-field                                 Field used for updating an existing document. This should not conflict with any fieldname you are already using
 --wait-period                                  Number of seconds to wait between each batch
 --batch-size                                   Number of documents to update in a single batch
```
