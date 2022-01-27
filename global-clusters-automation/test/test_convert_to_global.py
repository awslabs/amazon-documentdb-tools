from convert_to_global_lambda_function import lambda_handler

event ={
    "global_cluster_id":"global-2",
    "primary_cluster_arn":"arn:aws:rds:us-east-1:378282045186:cluster:demo",
    "secondary_clusters":[
        {
            "region":"us-west-2",
            "secondary_cluster_id":"cluster-1812222021-125454",
            "number_of_instances":3,
            "subnet_group":"default",
            "security_group_id":[
                "sg-0817d8725e9edffda"
            ],
            "kms_key_id":"arn:aws:kms:us-west-2:378282045186:key/1ffd4692-238f-459e-9ced-5620bb8b426b",
            "backup_retention_period":1,
            "cluster_parameter_group":"default.docdb4.0",
            "preferred_back_up_window":"10:25-10:55",
            "preferred_maintenance_window":"wed:06:40-wed:07:10",
            "storage_encryption":True,
            "deletion_protection":False
        }
    ]
}

lambda_handler(event,'')