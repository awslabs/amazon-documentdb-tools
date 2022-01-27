#!/usr/bin/env bash
   echo 'Packaging and uploading global cluster automation code to S3'
   # Upload Lambda Code
   cd..
   mkdir app && cd app
   
   python3 -m venv failoverLambda
   source failoverLambda/bin/activate
   mv /GlobalClustersAutomation/failover_and_delete_lambda_function.py failoverLambda/lib/python*/site-packages/
   mv /GlobalClustersAutomation/failover_and_delete_global_cluster.py failoverLambda/lib/python*/site-packages/
   mv /GlobalClustersAutomation/route53_endpoint_management.py failoverLambda/lib/python*/site-packages/
   cp /GlobalClustersAutomation/requirements.txt failoverLambda/lib/python*/site-packages/
   cd failoverLambda/lib/python*/site-packages/
   pip install -r requirements.txt 
   deactivate
   mv ../dist-packages/* .
   zip -r9 failoverLambdaFunction.zip .
   aws s3 cp failoverLambdaFunction.zip s3://$1

   python3 -m venv convertToGlobalLambda
   source convertToGlobalLambda/bin/activate
   mv /GlobalClustersAutomation/convert_to_global_lambda_function.py convertToGlobalLambda/lib/python*/site-packages/
   mv /GlobalClustersAutomation/add_secondarycluster.py convertToGlobalLambda/lib/python*/site-packages/
   cp /GlobalClustersAutomation/requirements.txt convertToGlobalLambda/lib/python*/site-packages/
   cd convertToGlobalLambda/lib/python*/site-packages/
   pip install -r requirements.txt 
   deactivate
   mv ../dist-packages/* .
   zip -r9 convertToGlobalLambdaFunction.zip .
   aws s3 cp convertToGlobalLambdaFunction.zip s3://$1

   python3 -m venv failoverAndConvertToGlobalLambda
   source failoverAndConvertToGlobalLambda/bin/activate
   mv /GlobalClustersAutomation/failover_and_convert_lambda_function.py failoverAndConvertToGlobalLambda/lib/python*/site-packages/
   mv /GlobalClustersAutomation/failover_and_convert_to_global.py failoverAndConvertToGlobalLambda/lib/python*/site-packages/
   cp /GlobalClustersAutomation/requirements.txt failoverAndConvertToGlobalLambda/lib/python*/site-packages/
   cd failoverAndConvertToGlobalLambda/lib/python*/site-packages/
   pip install -r requirements.txt 
   deactivate
   mv ../dist-packages/* .
   zip -r9 failoverAndConvertToGlobalLambda.zip .
   aws s3 cp failoverAndConvertToGlobalLambda.zip s3://$1
