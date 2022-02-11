#!/usr/bin/env bash
   echo 'Packaging and uploading global cluster automation code to S3'
   # Upload Lambda Code
   cd app

   python3 -m venv failoverLambda
   source failoverLambda/bin/activate
   mv failover_and_delete_lambda_function.py failoverLambda/lib/python*/site-packages/
   mv failover_and_delete_global_cluster.py failoverLambda/lib/python*/site-packages/
   mv route53_endpoint_management.py failoverLambda/lib/python*/site-packages/
   cp requirements.txt failoverLambda/lib/python*/site-packages/
   cd failoverLambda/lib/python*/site-packages/
   pip install -r requirements.txt 
   deactivate
   mv ../dist-packages/* .
   zip -r9 failoverLambdaFunction.zip .
   aws s3 cp failoverLambdaFunction.zip s3://$1

   cd ..
