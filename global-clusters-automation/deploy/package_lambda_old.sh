#!/usr/bin/env bash
   echo 'Packaging and uploading global cluster automation code to S3'
   # Upload Lambda Code
   cd..
   mkdir app && cd app
   wget https://raw.githubusercontent.com/aws-samples/amazon-documentdb-samples/master/samples/change-streams/app/lambda_function.py
   wget https://raw.githubusercontent.com/aws-samples/amazon-documentdb-samples/master/samples/change-streams/app/requirements.txt
   wget https://raw.githubusercontent.com/aws-samples/amazon-documentdb-samples/master/samples/change-streams/app/lambda_function.py
   wget https://raw.githubusercontent.com/aws-samples/amazon-documentdb-samples/master/samples/change-streams/app/requirements.txt
   wget https://raw.githubusercontent.com/aws-samples/amazon-documentdb-samples/master/samples/change-streams/app/lambda_function.py
   wget https://raw.githubusercontent.com/aws-samples/amazon-documentdb-samples/master/samples/change-streams/app/requirements.txt
   wget https://raw.githubusercontent.com/aws-samples/amazon-documentdb-samples/master/samples/change-streams/app/lambda_function.py
   wget https://raw.githubusercontent.com/aws-samples/amazon-documentdb-samples/master/samples/change-streams/app/requirements.txt

   python -m venv failoverLambda
   source failoverLambda/bin/activate
   mv failover_and_delete_lambda_function.py failoverLambda/lib/python*/site-packages/
   mv failover_and_delete_global_cluster.py failoverLambda/lib/python*/site-packages/
   mv route53_endpoint_management.py failoverLambda/lib/python*/site-packages/
   mv requirements.txt failoverLambda/lib/python*/site-packages/
   cd failoverLambda/lib/python*/site-packages/
   pip install -r requirements.txt 
   deactivate
   mv ../dist-packages/* .
   zip -r9 failoverLambdaFunction.zip .
   aws s3 cp failoverLambdaFunction.zip s3://$1

   python -m venv convertToGlobalLambda
   source convertToGlobalLambda/bin/activate
   mv convert_to_global_lambda_function.py convertToGlobalLambda/lib/python*/site-packages/
   mv add_secondarycluster.py convertToGlobalLambda/lib/python*/site-packages/
   mv requirements.txt convertToGlobalLambda/lib/python*/site-packages/
   cd convertToGlobalLambda/lib/python*/site-packages/
   pip install -r requirements.txt 
   deactivate
   mv ../dist-packages/* .
   zip -r9 convertToGlobalLambdaFunction.zip .
   aws s3 cp convertToGlobalLambdaFunction.zip s3://$1
