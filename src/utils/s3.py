import os
from typing import List
import boto3
from utils.log import Logging


class S3resources(Logging):
    def __init__(self, bucket=os.getenv("AWS_S3_BUCKET")):
        self.s3 = boto3.resource("s3")
        self.bucket = bucket

    def read_json_file(self, key):
        obj = self.s3.Object(self.bucket, key)
        return obj.get()["Body"].read().decode()

    def put_object_to_s3(self, file_key_name, data):
        file = self.s3.Object(self.bucket, file_key_name)
        file.put(Body=data)

    def get_obj_for_bucket(self) -> List[str]:
        return [obj for obj in self.s3.Bucket(self.bucket).objects.all()]

    def ensure_non_zero_byte_file(self, key):
        data = self.read_json_file(key)
        return True if len(data) > 10 else False

    def list_all_bucket_objects(self):
        bucket = self.s3.Bucket(self.bucket)
        return [s3_file for s3_file in bucket.objects.all()]
