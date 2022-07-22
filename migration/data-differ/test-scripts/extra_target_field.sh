#!/bin/bash
export myDB=$SOURCE_DB
export myColl=$SOURCE_COLL
mongo $SOURCE_URI extra_target_field_source.js

export myDB=$TARGET_DB
export myColl=$TARGET_COLL
mongo $TARGET_URI extra_target_field_target.js

cd ..
python3 data-differ.py --source-uri $SOURCE_URI --target-uri $TARGET_URI --source-namespace "$SOURCE_DB.$SOURCE_COLL" --target-namespace "$TARGET_DB.$TARGET_COLL" --percent 100

export myDB=$SOURCE_DB
export myColl=$SOURCE_COLL
mongo $SOURCE_URI drop_coll.js

export myDB=$TARGET_DB
export myColl=$TARGET_COLL
mongo $TARGET_URI drop_coll.js