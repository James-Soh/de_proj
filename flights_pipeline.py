from minio import Minio
import io
import psycopg
import pandas as pd
from datetime import date, timedelta
from io import StringIO

bucket_name = "de-proj"
table_name = 'flights'
start_date = date.today()
# start_date = '2024-12-28'
file_name = f'/data/csv/{table_name}_{start_date}.csv'

type_mapping = {
    "int64":"integer",
    "float64":"double precision",
    "object":"text",
}

# Create a client with the MinIO server playground, its access key and secret key.
client = Minio(
    endpoint = "172.22.143.31:9000",
    access_key = "IOYsPkyt2Rj6BIDmhTCp",
    secret_key = "Qbd7sEWR26XwoFf08QClMD3oBzZjBrOOjvEcFXHM",
    secure = False, # https://stackoverflow.com/questions/71123560/getting-ssl-wrong-version-number-wrong-version-number-when-working-with-mini
)

# getting table column metadata
# needed to create header if table exist | create table if table does not exist
response = client.get_object(bucket_name, file_name)
df = pd.read_csv(StringIO(response.data.decode('utf-8'))) 	# https://www.geeksforgeeks.org/convert-bytes-to-a-pandas-dataframe/
buf = io.StringIO() 										# https://stackoverflow.com/questions/64067424/how-to-convert-df-info-into-data-frame-df-info
df.info(buf=buf)
s = buf.getvalue()
lines = [line.split() for line in s.splitlines()[3:-2]]
df_metadata = pd.DataFrame(lines).rename(columns={1:'name',4:'dtype'}).iloc[2:,:]
df_metadata['db_type'] = df_metadata.dtype.map(type_mapping)

# get file for ingestion
url = client.get_presigned_url(
    'GET',
    bucket_name,
    file_name,
    expires=timedelta(seconds=120),
    response_headers={"response-content-type": "application/json"},
)

try:
    header = ','.join(list(df_metadata.name)).replace('.','_')
    print('Connecting to Postgres \n -----')
    # base table data ingestion
    with psycopg.connect(user='admin', password='pgadmin', host='127.0.0.1', port='5436', dbname ='bronze') as connection:
        print('Connecting to Postgres Bronze: Successful \n -----')

        print('Creating a cursor \n -----')
        cursor = connection.cursor()
        print('Cursor created \n -----')

        print(f'attempting to ingest data to base table: {table_name} \n -----')
        query = f'''
            COPY AVIATIONSTACK.{table_name} ({header}) 
            FROM PROGRAM 'curl "{url}"' 
            WITH (FORMAT CSV, HEADER, QUOTE '"', DELIMITER ',')
            ;
        '''
        connection.execute(query)
        print('attempt success \n -----')

except (Exception, psycopg.Error) as error:
    print(error)
    if 'does not exist' in str(error):
        with psycopg.connect(user='admin', password='pgadmin', host='127.0.0.1', port='5436', dbname ='bronze') as connection:
            # creating DDL statement to create new table
            df_metadata['column'] = df_metadata.name.str.replace('.', '_') + ' ' + df_metadata.db_type + ','
            thing = '\n'.join(list(df_metadata.column))[:-1]
            thing = '\n' + thing
            query = f'''
                create table AVIATIONSTACK.{table_name.upper()} ( 
                    {thing} \n
                );
            '''
            try:
                connection.execute(query)
                print(f' ----- \n new table {table_name} created \n -----')

                # base table data ingestion re-attempt
                print(f'attempting to re-ingest data to base table: {table_name} \n -----')
                query = f'''
                    COPY AVIATIONSTACK.{table_name} ({header}) 
                    FROM PROGRAM 'curl "{url}"' 
                    WITH (FORMAT CSV, HEADER, QUOTE '"', DELIMITER ',')
                    ;
                '''
                connection.execute(query)
                print('attempt success \n -----')

            except:
                pass

    else:
        pass