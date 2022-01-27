#!/usr/bin/env bash
   # Upload Lambda Code
   mkdir app

   cp failover_and_delete_lambda_function.py app/
   cp failover_and_delete_global_cluster.py app/
   cp route53_endpoint_management.py app/
   cp convert_to_global_lambda_function.py app/
   cp add_secondarycluster.py app/
   cp failover_and_convert_lambda_function.py app/
   cp failover_and_convert_to_global.py app/
   cp requirements.txt app/

   sh ./deploy/package_lambda_failover.sh $1
   sh ./deploy/package_lambda_convert.sh $1
   sh ./deploy/package_lambda_failover_and_convert.sh $1
