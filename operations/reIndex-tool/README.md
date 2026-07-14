DocumentDB-Reindex
Amazon DocumentDB (with MongoDB compatibility) is a fully managed, scalable, and highly available document database service that uses Multi-Version Concurrency Control(MVCC).
A Multi-Version Concurrency Control (MVCC) database is a database system that manages concurrent access by creating and maintaining multiple versions of data records. This allows us to perform read and writes without blocking operations. Instead of locking data during reads or writes, MVCC generates a new version of a record when updates occur, so transactions can read a consistent snapshot.
However, with a very high-write workloads  it introduces trade-offs that may impact the database performance. Every update or delete operation results in a new version of collections and indexes which causes fragmentation within the pages. With Amazon DocumentDB, this bloat is captured in the index stats and is represented in the fields unusedSizeBytes and unusedSizePercent .
This tool helps to script out the reindex commands for the collections which are highly fragmented from the update/deletes to the Amazon DocuemntDB.
Requirements
Python 3.8 or greater, Pymongo.
Installation
git clone <GIT-REPOSITORY>
cd DocumentDB-Reindex
python3 -m pip install -r requirements.txt
Usage
--skip-python-version-check        Permit execution using Python 3.6 and prior 
--uri                              Amazon DocumentDB URI 
--unusedCollectionSizeMB           Unused collection size in MB. Defaults to 1024 MB
--unusedCollectionSizePercent      Unused collection size percentage. Defaults to 50%
Example commands
python reindex_script.py --uri mongodb://localhost:27017/ --unusedCollectionSizeMB 10240 --unusedCollectionSizePercent 30

Example Output
db.runCommand({ reIndex: "dummy.books", index: "_id_" })
db.runCommand({ reIndex: "dummy.books", index: "isbn_1_title_1_pageCount_1" })
db.runCommand({ reIndex: "dummy.books", index: "shortDescription_text" })
