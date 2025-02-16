import de_project_fx.module as data_sync

from minio import Minio
import io
import psycopg
import pandas as pd
from datetime import date, timedelta

from dotenv import load_dotenv
import os

from minio import Minio
import io
import psycopg
import pandas as pd
from datetime import date, timedelta
from io import StringIO

load_dotenv('secret.env')
minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
pg_user = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASSWORD")
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT")
pg_db_name = os.getenv("PG_DB_NAME")

bucket_name = "de-proj"
table_name = 'flights'
start_date = date.today()
file_name = f'/data/csv/{table_name}_{start_date}.csv'

type_mapping = {
    "int64":"integer",
    "float64":"double precision",
    "object":"text",
}

# Create a client with the MinIO server playground, its access key and secret key.
client = Minio(
    endpoint = minio_endpoint,
    access_key = minio_access_key,
    secret_key = minio_secret_key,
    secure = False, # https://stackoverflow.com/questions/71123560/getting-ssl-wrong-version-number-wrong-version-number-when-working-with-mini
)

# get file for ingestion
url = client.get_presigned_url(
    'GET',
    bucket_name,
    file_name,
    expires=timedelta(seconds=120),
    response_headers={"response-content-type": "application/json"},
)

header = data_sync.generate_header(minio_file_name = file_name, minio_bucket_name=bucket_name, minio_client=client) 
table_definition = data_sync.generate_table_definition(minio_file_name = file_name, minio_bucket_name=bucket_name, minio_client=client)

# schema check before doing anything
to_add = data_sync.schema_check(minio_table_header=header, pg_table_name=table_name)

if to_add['added']['length'] > 0:
    for new_column in to_add['added']['items']:
        # updates tables with new coumn first before ingesting data
        result = data_sync.schema_append(new_column_to_add=new_column, pg_table_definition=table_definition, pg_table_name=table_name)

print('Connecting to Postgres \n -----')
# base table data ingestion
with psycopg.connect(user=pg_user, password=pg_password, host=pg_host, port=pg_port, dbname=pg_db_name) as pg_connection:
    print('Connecting to Postgres Bronze: Successful \n -----')

    print('Creating a cursor \n -----')
    cursor = pg_connection.cursor()

    print(f'attempting to ingest data to base table: {table_name} \n -----')
    query = f'''
        COPY AVIATIONSTACK.{table_name} ({header}) 
        FROM PROGRAM 'curl "{url}"' 
        WITH (FORMAT CSV, HEADER, QUOTE '"', DELIMITER ',')
        ;
    '''
    pg_connection.execute(query)
    print('attempt success \n -----')