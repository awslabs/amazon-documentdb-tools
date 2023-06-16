# Amazon DocumentDB DataDiffer Tool

The purpose of the DataDiffer tool is to facilitate the validation of data consistency by comparing two collections, making it particularly useful in migration scenarios.
This tool performs the following checks:

- Document existence check: It reads documents in batches from the source collection and checks for their existence in the target collection. If there is a discrepancy, the tool attempts will identify and report the missing documents.
- Index Comparison: examines the indexes of the collections and reports any differences.
- Document Comparison: each document in the collections, with the same _id, is compared using the DeepDiff library. This process can be computationally intensive, as it involves scanning all document fields. The duration of this check depends on factors such as document complexity and the CPU resources of the machine executing the script.

## Prerequisites:

 - Python 3
 - Modules: pymongo, deepdiff, tqdm
```
  pip3 install pymongo deepdiff tqdm
```
Note: Refer to the DeepDiff [documentation](https://zepworks.com/deepdiff/current/optimizations.html) for potential optimizations you may try out specifically for your dataset.

## How to use

1. Clone the repository and go to the tool folder:
```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/migration/data-differ/
```

2. Update the `source.vars` file and export the variables with `source source.vars`

3. Run the data-differ.py tool, which accepts the following (optional) arguments:

```
python3 data-differ.py --help
usage: data-differ.py [-h] [--batch_size BATCH_SIZE] [--output_file OUTPUT_FILE] [--check_target CHECK_TARGET]

Compare two collections and report differences.

optional arguments:
  -h, --help            show this help message and exit
  --batch_size BATCH_SIZE
                        Batch size for bulk reads (default: 100)
  --output_file OUTPUT_FILE
                        Output file path (default: differences.txt)
  --check_target CHECK_TARGET
                        Check if extra documents exist in target database
```
