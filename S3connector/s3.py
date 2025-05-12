from dotenv import load_dotenv

import boto3, os

load_env = load_dotenv()

key1 = os.getenv("AWS_ACCESS_KEY")
key2 = os.getenv("AWS_SECRET_KEY")
key3 = os.getenv("REGION")
key4 = os.getenv("BUCKET_NAME")


class S3object:
    def __init__(self, accessKey, secretKey, region):
        self.s3 = boto3.client(
                's3',
                aws_access_key_id     = accessKey,
                aws_secret_access_key = secretKey,
                region_name           = region
            )

    def list_buckets(self):
        response = self.s3.list_buckets()
        return [b["Name"] for b in response.get("Buckets", [])]

    def list_objects(self, bucket, prefix: str = "~/"):
        """prefix는 /로 끝나야 함.
           ex : abc/
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket = bucket, Prefix = prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                yield obj
                    
bucket = S3object(key1, key2, key3)
rawData = bucket.list_objects('lake.evt', 'raw/')

print(f"bucket name : {bucket.list_buckets()}")

for i in rawData:
    print(f"data list : {i['Key'], i['Size']}")