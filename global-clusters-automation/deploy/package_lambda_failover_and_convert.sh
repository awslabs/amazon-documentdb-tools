#!/usr/bin/env bash
   echo 'Packaging and uploading global cluster automation code to S3'
   # Upload Lambda Code
   cd app 
   #mkdir app3 && cd app3
   #TODO Uncomment above line after downloading source code from git hub (after open sourcing)
   
   
   python3 -m venv failoverAndConvertToGlobalLambda
   source failoverAndConvertToGlobalLambda/bin/activate
   mv failover_and_convert_lambda_function.py failoverAndConvertToGlobalLambda/lib/python*/site-packages/
   mv failover_and_convert_to_global.py failoverAndConvertToGlobalLambda/lib/python*/site-packages/
   cp requirements.txt failoverAndConvertToGlobalLambda/lib/python*/site-packages/
   cd failoverAndConvertToGlobalLambda/lib/python*/site-packages/
   pip install -r requirements.txt 
   deactivate
   mv ../dist-packages/* .
   zip -r9 failoverAndConvertToGlobalLambda.zip .
   aws s3 cp failoverAndConvertToGlobalLambda.zip s3://$1
   cd ..