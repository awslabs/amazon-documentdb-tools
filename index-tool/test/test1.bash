#! /bin/bash

python3 ../migrationtools/documentdb_index_tool.py --restore-indexes --dry-run --dir test1 | sed -n '1d;p' | cut -c 26- | diff - test1.expects
