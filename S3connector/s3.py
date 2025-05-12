from dotenv import load_dotenv

import boto3, os

load_env = load_dotenv()

key1 = os.getenv("AWS_ACCESS_KEY")
key2 = os.getenv("AWS_SECRET_KEY")
key3 = os.getenv("REGION")
key4 = os.getenv("BUCKET_NAME")

# Minio, GCS 등을 사용한다면 의존성 교체가 용이하도록 ABC 고려

class S3object:
    def __init__(self, accessKey, secretKey, region):
        self.s3 = boto3.client(
                's3',
                aws_access_key_id     = accessKey,
                aws_secret_access_key = secretKey,
                region_name           = region
            )

    def list_buckets(self) -> str:
        """bucket 리스트를 출력"""
        response = self.s3.list_buckets()
        return [b["Name"] for b in response.get("Buckets", [])]

    def list_objects(self, bucket, prefix: str = "~/") -> list[dict]:
        """
           bucket의 특정 폴더를 조회
           prefix는 /로만 끝나야 함.
           ex : abc/ -> 버킷 내 폴더
        """
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket = bucket, Prefix = prefix)
        result = []
        for page in pages:
            for obj in page.get("Contents", []):
                result.append(obj)
                
        return result


### 사용 예시        
# bucket = S3object(key1, key2, key3)
# rawData = bucket.list_objects('lake.evt', "raw/")

# print(f"bucket name : {bucket.list_buckets()}")
# print(rawData)
# # 자세히 보기
# for i in rawData:
#     print(f"data list : {i['Key'], i['Size']}")
