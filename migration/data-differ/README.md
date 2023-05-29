# Amazon DocumentDB DataDiffer Tool
The purpose of DataDiffer tool is to compare two collections in order to validate if the data matches and it's useful for migration scenarios.
The tools does 3 checks:
 - Check document count, if is not the same it will try to find the documents that are missing.
 - Check if indexes are the same and reports differences.
 - Checks every document in each collection and compares using the DeepDiff library. This check can be quite intensive, the time it takes to scan for all documents will depend on document complexity and the CPU resources of the machine you're running the script from.
   The script is using the Python multiprocessing library in order to parallelize the DeppDiff check.

## Prerequisites:

 - Python 3
 - Modules: pymongo, deepdiff, tqdm
```
  pip3 install pymongo deepdiff tqdm
```
Note: See the DeepDiff [documentation](https://zepworks.com/deepdiff/current/optimizations.html) for possible optimisations you may try out to see if you get better performance for your particular data set.

## How to use

1. Clone the repository and go to the tool folder:
```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/migration/data-differ/
```

2. Update the `source.vars` file and export the variables with `source source.vars`

3. Run the data-differ.py tool, which accepts two optional arguments:

```
python3 compare_mp_final_indexes.py --help
usage: compare_mp_final_indexes.py [-h] [--batch_size BATCH_SIZE]
                                   [--output_file OUTPUT_FILE]

Compare two collections and report differences.

optional arguments:
  -h, --help            show this help message and exit
  --batch_size BATCH_SIZE
                        Batch size for bulk reads (default: 1000)
  --output_file OUTPUT_FILE
```
