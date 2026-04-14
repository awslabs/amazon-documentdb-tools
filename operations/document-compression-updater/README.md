# Python Updater tool 
This sample application compresses pre-existing documents in an existing collection after compression is turned on that existing collection.

Single threaded application - issues **5000** (controlled by argument --batch-size) updates serially in a _round_, and sleeps for **60** (controlled by argument --wait-period) seconds before starting next _round_.

After each batch, the temporary dummy field used to trigger compression is automatically removed from all updated documents. Use `--skip-cleanup` to disable this behaviour.

Status of the updates are maintained in database **tracker_db** - for each collection there is a tracker collection named **<< collection >>__tracker_col**. Each tracker entry includes a `cleanupComplete` flag indicating whether the dummy field was removed for that batch.

The application can be restarted if it crashes and it will pick up from last successful _round_ based on data in **<< collection >>__tracker_col**. On successful completion the tracker collection is automatically dropped, as it is no longer needed.

The update statements use field **6nh63** (controlled by argument --update-field), for triggering compression on existing records. This field is removed from each document after compression is applied unless `--skip-cleanup` is set.

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
 python3 update_apply_compression.py --uri "<<documentdb_uri>>"  --database <<database>>   --collection <<collection>> --update-field << field_name >> --wait-period << int >> --batch-size << int >>
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
 --append-log                                   Append to existing log file instead of overwriting it on startup
 --skip-cleanup                                 Skip removing the dummy field after each batch (leaves update field permanently on documents)
```
