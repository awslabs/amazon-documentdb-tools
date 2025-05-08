# Export Users tool
This tool will export Amazon DocumentDB or MongoDB users and custom roles to files, which then can be used to create them in another cluster. Note: Passwords are not exported.

# Requirements
 - Python 3.7+
 - PyMongo

## Using the Export Users Tool
`python3 docdbExportUsers.py --users-file <users-file> --roles-file <roles-file> --uri <docdb-uri>`

## Example:
`python3 docdbExportUsers.py --users-file mydocdb-users.js --roles-file mydocdb-roles.js --uri "mongodb://user:password@mydocdb.cluster-cdtjj00yfi95.eu-west-2.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&retryWrites=false"`

## Restore custom roles
Run the custom roles .js script:
`mongo --ssl --host mydocdb.cluster-cdtjj00yfi95.eu-west-2.docdb.amazonaws.com:27017 --sslCAFile rds-combined-ca-bundle.pem --username <user> --password <password> mydocdb-roles.js`

## Restore users
Edit the users .js script and update passwords for each user. Run the users .js script:
`mongo --ssl --host mydocdb.cluster-cdtjj00yfi95.eu-west-2.docdb.amazonaws.com:27017 --sslCAFile rds-combined-ca-bundle.pem --username <user> --password <password> mydocdb-users.js`

## License
This tool is licensed under the Apache 2.0 License.