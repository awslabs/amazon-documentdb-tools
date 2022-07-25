#!/bin/bash
export myDB=$SOURCE_DB
export myColl=$SOURCE_COLL
mongo $SOURCE_URI nested_dict_diff_source.js

export myDB=$TARGET_DB
export myColl=$TARGET_COLL
mongo $TARGET_URI nested_dict_diff_target.js

cd ..
python3 data-differ.py --source-uri $SOURCE_URI --target-uri $TARGET_URI --source-namespace "$SOURCE_DB.$SOURCE_COLL" --target-namespace "$TARGET_DB.$TARGET_COLL" --percent 100
cd test-scripts

export myDB=$SOURCE_DB
export myColl=$SOURCE_COLL
mongo $SOURCE_URI drop_coll.js

export myDB=$TARGET_DB
export myColl=$TARGET_COLL
mongo $TARGET_URI drop_coll.js