import boto3
import os
import pandas as pd
from io import BytesIO
import zipfile


class Q3:
    """
    q3 is a class that provides a wrapper around the s3 resource in boto3.
    It provides methods to list buckets, upload files, and list files in a bucket.
    """

    def __init__(self):
        aws_access_key_id = os.getenv("AWS_S3_ACCESS_KEY_ID")
        aws_secret_access_key = os.getenv("AWS_S3_SECRET_ACCESS_KEY")
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )

    # ----------------

    def list_buckets(self):
        try:
            resp = self.client.list_buckets()
            return [x["Name"] for x in resp["Buckets"]]

        except Exception as e:
            print(e)
            return None

    def list(self, bucket_name, prefix=""):
        try:
            if prefix and prefix > "":
                response = self.client.list_objects_v2(
                    Bucket=bucket_name, Prefix=prefix
                )
            else:
                response = self.client.list_objects_v2(Bucket=bucket_name)
            return [
                "s3://" + bucket_name + "/" + x["Key"] for x in response["Contents"]
            ]

        except Exception as e:
            print(e)
            return None

    def download_df(self, bucket_name, filename):
        buffer = BytesIO()
        try:
            self.client.download_fileobj(bucket_name, filename, buffer)
            buffer.seek(0)
            if filename.endswith(".zip"):
                return self.zip_to_df(buffer)
            elif filename.endswith(".csv"):
                return pd.read_csv(buffer)
            return pd.DataFrame()
        except Exception as e:
            print(e)
            return None

    def download_file(self, bucket_name, filename):
        buffer = BytesIO()
        try:
            self.client.download_fileobj(bucket_name, filename, buffer)
            buffer.seek(0)
            return buffer
        except Exception as e:
            print(e)
            return None

    def upload_file(self, bucket_name, filename, filepath):
        if os.path.exists(filepath) is False:
            print(f"File not found: {filepath}")
            return None

        try:
            self.client.upload_file(filepath, bucket_name, filename)
            return True

        except Exception as e:
            print(e)
            return None

    # upload file like object - should be in binary format
    # open(filepath, "rb") will give you a file like object
    def upload_fileobj(self, bucket_name, filename, data):
        try:
            self.client.upload_fileobj(data, bucket_name, filename)
            return True

        except Exception as e:
            print(e)
            return None

    def upload_df(self, bucket_name, filename, df):
        if df.shape[0] == 0:
            print("DataFrame is empty")
            return None

        buffer = BytesIO()
        df.to_csv(buffer)
        return self.upload_fileobj(bucket_name, filename, buffer)

    def put_tags(self, bucket_name, filename, tags_dict, overwrite=True):
        try:
            if not overwrite:
                existing_tags = self.get_tags(bucket_name, filename)
                if existing_tags is not None:
                    tags_dict = {**existing_tags, **tags_dict}
            tags = [
                {"Key": k, "Value": self.clean_tag_value(v)}
                for k, v in tags_dict.items()
            ]
            self.client.put_object_tagging(
                Bucket=bucket_name, Key=filename, Tagging={"TagSet": tags}
            )
            return True
        except Exception as e:
            print(e)
            return None

    def clean_tag_value(self, tag_value):
        tag = "".join(filter(self.handle_special_chars, tag_value))
        return tag

    def handle_special_chars(self, char):
        allowed_special_chars = ["_", ".", "/", "=", "+", "-", " "]
        if char.isalnum() or char in allowed_special_chars:
            return char
        return ""

    # Driver code
    # special_string = "ERCOT & System Operating Limit (SOL) Methodology"
    # tag = "".join(filter(handle_special_chars, special_string))
    # print(tag)

    def get_tags(self, bucket_name, filename):
        try:
            resp = self.client.get_object_tagging(Bucket=bucket_name, Key=filename)
            return {x["Key"]: x["Value"] for x in resp["TagSet"]}
        except Exception as e:
            print(e)
            return None

    def get_list_page_iterator(self, bucket_name, prefix="", pagesize=1000):
        params = {
            "Bucket": bucket_name,
            "Prefix": prefix,
            "PaginationConfig": {"PageSize": pagesize},
        }
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(**params)
        return page_iterator

    def move(self, from_bucket, from_key, to_bucket, to_key):
        try:
            copy_source = {
                "Bucket": from_bucket,
                "Key": from_key,
            }
            self.client.copy_object(
                Bucket=to_bucket, Key=to_key, CopySource=copy_source
            )
            self.client.delete_object(Bucket=from_bucket, Key=from_key)
            return True
        except Exception as e:
            print(e)
            return None

    def zip_to_df(self, buffer):
        result = []
        zip_files = zipfile.ZipFile(buffer)
        zip_list = zip_files.infolist()
        for file in zip_list:
            result.append(zip_files.open(file))

        return pd.concat([pd.read_csv(f) for f in result])
