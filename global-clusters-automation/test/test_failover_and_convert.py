from failover_and_convert_lambda_function import lambda_handler

event = {
    "global_cluster_id": "global-7",
    "secondary_cluster_arn": "arn:aws:rds:us-east-2:378282045186:cluster:cluster-13",
    "primary_cluster_cname": "primary.sample.com",
    "hosted_zone_id": "Z00565841LXHQLXKDOHSB"
}
import time

start = time.time()
lambda_handler(event, '')
done = time.time()
elapsed = done - start
print("Total Time to execute the lambda function is", elapsed, "secs")
