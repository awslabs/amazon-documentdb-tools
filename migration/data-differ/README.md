# DataDiffer Overview
DataDiffer will compare a user-defined percentage of random documents between two MongoDB/DocumentDB servers for a single collection in each. 

## Prerequisites that Must be Met for Running DataDiffer
1. Install pymongo, deepdiff, and tqdm software packages by running the following commands in the command line: 
```
    pip install pymongo
    pip install deepdiff
    pip install tqdm
```

2. Clone the repo to your machine where you want to run DataDiffer.
3. CD into DataDiffer

## To Run DataDiffer On Your Own Source/Target Collections
1. Assuming you have the prerequisites above met, run the main.py file in the command line with the proper arguments, such as the following: 
```
     python main.py --source-uri $SOURCE_URI --target-uri $TARGET_URI --source-namespace "$SOURCE_DB.$SOURCE_COLL" --target-namespace       
     "$TARGET_DB.$TARGET_COLL" --percent 100

```
Note: You can adjust the percent value as needed in the command above. 

2. See the ouput in the command line! 
