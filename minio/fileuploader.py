#!/bin/python3

from minio import Minio
from minio.error import S3Error


def main():
    # Create a client with the MinIO server playground, its access key
    # and secret key.
    client = Minio(
        "namubuntu:9010",
        access_key="1ijxXR6p1JtvDSGa8a8X",
        secret_key="qUGuvrEY0V1y58dx7bEgtoBdXmszPZKdWJu4fLHg",
        session_token=None,
        secure=False,
        region=None,
        http_client=None,
        credentials=None,
    )

    # Make 'asiatrip' bucket if not exist.
    found = client.bucket_exists("asiatrip")
    if not found:
        client.make_bucket("asiatrip")
    else:
        print("Bucket 'asiatrip' already exists")

    # Upload '/home/user/Photos/asiaphotos.zip' as object name
    # 'asiaphotos-2015.zip' to bucket 'asiatrip'.
    client.fput_object(
        "asiatrip", "wordSample.txt", "/home/yknam/wordSample.txt",
    )
    print(
        "'/home/yknam/wordSample.txt' is successfully uploaded as "
        "object 'wordSample.txt' to bucket 'asiatrip'."
    )


if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)
