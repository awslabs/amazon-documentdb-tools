import boto3
from botocore.exceptions import ClientError

session = boto3.Session()
client = session.client('route53')


def update_endpoint(zone_id, name, value):
    try:
        client.change_resource_record_sets(
            HostedZoneId=zone_id,
            ChangeBatch={
                "Comment": "Switching endpoint",
                "Changes": [
                    {
                        "Action": "UPSERT",
                        "ResourceRecordSet": {
                            "Name": name,
                            "Type": "CNAME",
                            "TTL": 1,
                            "ResourceRecords": [{"Value": value}]
                        }
                    }
                ]
            }
        )
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError


def manage_application_endpoint(hosted_zone_id, endpoint, cname):
    try:
        response = client.list_resource_record_sets(
            HostedZoneId=hosted_zone_id
        )
        for record in response['ResourceRecordSets']:
            record_name = ''
            if record['Type'] == 'CNAME':
                record_name = record['Name']
                record_value = record['ResourceRecords'][0]
                if cname in record_name:
                    # get record value by calling describe cluster

                    update_endpoint(hosted_zone_id, record_name, endpoint)
                    print('Updated CNAME ', record_name, 'with record value', endpoint)
                    break

    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise ClientError
