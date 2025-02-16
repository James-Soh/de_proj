from minio import Minio
from minio.error import S3Error

import os
from datetime import date

def main():
    # Create a client with the MinIO server playground, its access key and secret key.
    client = Minio("172.22.143.31:9000",
        access_key="IOYsPkyt2Rj6BIDmhTCp",
        secret_key="Qbd7sEWR26XwoFf08QClMD3oBzZjBrOOjvEcFXHM",
        secure=False, # https://stackoverflow.com/questions/71123560/getting-ssl-wrong-version-number-wrong-version-number-when-working-with-mini
    )

    # The destination bucket and filename on the MinIO server
    bucket_name = "de-proj"

    # Make the bucket if it doesn't exist.
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        print("Created bucket", bucket_name)
    else:
        print("Bucket", bucket_name, "already exists")

    files = [ f for f in os.listdir( os.curdir ) if ('.csv' in f or '.json' in f)] 
    print(files)
    # for i in files:
    #     # The file to upload, change this path if needed
    #     source_file = i
    #     file_extension = i.split('.')[1]
    #     destination_file = f'/data/{file_extension}/' + source_file
    #     # Upload the file, renaming it in the process
    #     client.fput_object(
    #         bucket_name, destination_file, source_file,
    #     )
    #     print(
    #         source_file, "successfully uploaded as object",
    #         destination_file, "to bucket", bucket_name,
    #     )
    #     # Remove file from PC
    #     os.remove(i)

if __name__ == "__main__":
    try:
        main()
    except S3Error as exc:
        print("error occurred.", exc)
        with open('error.txt', 'w') as f:
            f.write(str(exc))