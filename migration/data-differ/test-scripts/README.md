## To Run DataDiffer Test Scripts
1. Ensure you have met the prerequsities from the general README.
2. Create an envrionment file(s) as needed based on your migration needs. The environment file should set environment variables and look something like this with each variable filled out for your use case:
```
    export SOURCE_URI=""
    export SOURCE_DB=""
    export SOURCE_COLL=""
    export TARGET_URI=""
    export TARGET_DB=""
    export TARGET_COLL=""
```
3. Source the environment file you built in the command line with a command such as the following: 
```
    source <environment-file-name-here>.sh
```
4. Run the appropriate bash test script in the command line with a command such as the following: 
```
    bash <test-script-name-here>.bash
```
5. See the output in the command line!
