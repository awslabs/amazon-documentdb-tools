#!/usr/bin/env bash
   echo 'Packaging and uploading global cluster automation code to S3'
   # Upload Lambda Code
   cd app 
   #mkdir app2 && cd app2
   #TODO Uncomment above line after downloading source code from git hub (after open sourcing)
   
   python3 -m venv convertToGlobalLambda
   source convertToGlobalLambda/bin/activate
   mv convert_to_global_lambda_function.py convertToGlobalLambda/lib/python*/site-packages/
   mv add_secondarycluster.py convertToGlobalLambda/lib/python*/site-packages/
   cp requirements.txt convertToGlobalLambda/lib/python*/site-packages/
   cd convertToGlobalLambda/lib/python*/site-packages/
   pip install -r requirements.txt 
   deactivate
   mv ../dist-packages/* .
   zip -r9 convertToGlobalLambdaFunction.zip .
   aws s3 cp convertToGlobalLambdaFunction.zip s3://$1
   cd ..
