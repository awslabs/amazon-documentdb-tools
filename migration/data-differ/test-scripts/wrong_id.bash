#!/bin/bash

mongoimport $SOURCE_URI -d $SOURCE_DB -c $SOURCE_COLL wrong_id_source.json
mongoimport $TARGET_URI -d $TARGET_DB -c $TARGET_COLL wrong_id_target.json

python3 ../data-differ.py --source-uri $SOURCE_URI --target-uri $TARGET_URI --source-namespace "$SOURCE_DB.$SOURCE_COLL" --target-namespace "$TARGET_DB.$TARGET_COLL" --percent 100

mongosh $SOURCE_URI <<EOF
use $SOURCE_DB
db.$SOURCE_COLL.drop()
EOF

mongosh $TARGET_URI <<EOF
use $TARGET_DB
db.$TARGET_COLL.drop()
EOF
