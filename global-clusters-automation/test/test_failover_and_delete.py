from failover_and_delete_lambda_function import lambda_handler

event = {
    "global_cluster_id": "global-demos",
    "secondary_cluster_arn": "arn:aws:rds:us-east-2:378282045186:cluster:cluster-5-165836",
    "primary_cluster_cname": "primary.sample.com",
    "hosted_zone_id": "Z00565841LXHQLXKDOHSB",
    "is_delete_global_cluster": True
}

lambda_handler(event,'')