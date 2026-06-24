import boto3
client = boto3.client('s3', endpoint_url='http://minio:9000',
                      aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin123',
                      region_name='us-east-1')
buckets = client.list_buckets()
print("Buckets:")
for b in buckets['Buckets']:
    print(f"  {b['Name']}")
    objs = client.list_objects_v2(Bucket=b['Name'], MaxKeys=5)
    for o in objs.get('Contents', []):
        print(f"    - {o['Key']} ({o['Size']} bytes)")
