# Export Users tool

This tool will export Amazon DocumentDB or MongoDB users to a file, which then can be used to import them to other instance. Note: Passwords are not exported.

# Requirements
 - Python 3.7+
 - PyMongo

## Using the Export Users Tool
`python3 docdbExportUsers.py --users-file <users-file> --uri <docdb-uri>`

## Example:
`python3 docdbExportUsers.py --users-file mydocdb-users.js --uri "mongodb://user:password@mydocdb.cluster-cdtjj00yfi95.eu-west-2.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&retryWrites=false"`

## Restore users
Edit the file and update passwords for each user. Run the .js script:

`mongo --ssl --host mydocdb.cluster-cdtjj00yfi95.eu-west-2.docdb.amazonaws.com:27017 --sslCAFile /root/rds-combined-ca-bundle.pem --username <user> --password <password> <mydocdb-users.js`

## License
This tool is licensed under the Apache 2.0 License.
