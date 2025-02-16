# from minio import Minio
import pandas as pd
import io
from io import StringIO
import psycopg
from datetime import date, timedelta
from dotenv import load_dotenv
import os
import requests
import json

load_dotenv('secret.env')
pg_user = os.getenv("PG_USER")
pg_password = os.getenv("PG_PASSWORD")
pg_host = os.getenv("PG_HOST")
pg_port = os.getenv("PG_PORT")
pg_db_name = os.getenv("PG_DB_NAME")

avs_key_1 = os.getenv("AVIATIONSTACK_ACESS_KEY")
avs_key_2 = os.getenv("AVIATIONSTACK_ACESS_KEY_2")

def aviationstack_data_retrieval(avs_endpoint: str, params: dict, api_offset: int, api_limit: int):
	today = date.today()

	if avs_endpoint != 'flights':
		url = f'https://api.aviationstack.com/v1/{avs_endpoint}?offset={api_offset}&limit={api_limit}'
	elif avs_endpoint == 'flights':
		url = f'https://api.aviationstack.com/v1/{avs_endpoint}?offset={api_offset}&limit={api_limit}&airline_iata=SQ'
	
	api_result = requests.get(url, params)
	api_response = api_result.json()

	try:
		df = pd.json_normalize(api_response['data'])
		df['snapshot_date'] = today	
		df.to_csv(f'{avs_endpoint}_{today}.csv', encoding='utf-8', index=False)

		with open(f'{avs_endpoint}_{today}.json', 'w') as f:
			json.dump(api_response, f)	

		result = {
			'success': True,
			'message': f'Data for {avs_endpoint}: Retrieved for {date.today()}'
		}
			
	except Exception as e: # trying to find key 'data' in JSON, but not such key --> means no data retrieved
		error_message = api_response['error']['code']
		result = {
			'success': False,
			'message': f'Data for {avs_endpoint}: {error_message}'
		}		

	return result

def generate_header(minio_file_name: str, minio_bucket_name: str, minio_client) -> str:
	response = minio_client.get_object(minio_bucket_name, minio_file_name)

	df = pd.read_csv(StringIO(response.data.decode('utf-8'))) 
	buf = io.StringIO() 
	df.info(buf=buf)

	s = buf.getvalue()
	lines = [line.split() for line in s.splitlines()[3:-2]]
	df_metadata = pd.DataFrame(lines).rename(columns={1:'name',4:'dtype'}).iloc[2:,:]

	header = ','.join(list(df_metadata.name))

	return header

def generate_table_definition(minio_file_name: str, minio_bucket_name: str, minio_client) -> str:
	type_mapping = {
	"int64":"integer",
	"float64":"double precision",
	"object":"text"
	}

	response = minio_client.get_object(minio_bucket_name, minio_file_name)

	df = pd.read_csv(StringIO(response.data.decode('utf-8'))) 
	buf = io.StringIO() 
	df.info(buf=buf)

	s = buf.getvalue()
	lines = [line.split() for line in s.splitlines()[3:-2]]

	df_metadata = pd.DataFrame(lines).rename(columns={1:'name',4:'dtype'}).iloc[2:,:]
	df_metadata['db_type'] = df_metadata.dtype.map(type_mapping)
	df_metadata.loc[df_metadata['name'] == 'snapshot_date', 'db_type'] = 'date'
	df_metadata.loc[df_metadata['name'] == 'snapshot_date', 'name'] = 'ingest_date'
	df_metadata['column'] = df_metadata.name + ' ' + df_metadata.db_type + ','

	table_definition = '\n'.join(list(df_metadata.column))
	table_definition = '\n' + table_definition 

	return table_definition[:-1]

def create_table(pg_table_definition: str, pg_table_schema:str, pg_table_name:str):
	with psycopg.connect(user=pg_user, password=pg_password, host=pg_host, port=pg_port, dbname =pg_db_name) as pg_connection:
		query = f'''
			create table {pg_table_schema.upper()}.{pg_table_name.upper()} ( 
				{pg_table_definition} \n
			);
		'''
		pg_connection.execute(query)

	return (True, f'Table created: {pg_table_name}')

def base_table_sync(minio_table_header: str, pg_table_name: str, minio_url: str):
	minio_table_header = minio_table_header.replace('snapshot_date','ingest_date')
	with  psycopg.connect(user=pg_user, password=pg_password, host=pg_host, port=pg_port, dbname =pg_db_name) as pg_connection:
		cursor = pg_connection.cursor()

		query = f'''
			TRUNCATE TABLE AVIATIONSTACK.{pg_table_name}
			;
		'''
		a = pg_connection.execute(query)
		

		query = f'''
			COPY AVIATIONSTACK.{pg_table_name} ({minio_table_header}) 
			FROM PROGRAM 'curl "{minio_url}"' 
			WITH (FORMAT CSV, HEADER, QUOTE '"', DELIMITER ',')
			;
		'''
		a = pg_connection.execute(query)

	return (True, f'Data for Table: {pg_table_name} synced for {date.today()}')

def hist_table_append(minio_table_header: str, pg_table_name: str, minio_url: str):
	with  psycopg.connect(user=pg_user, password=pg_password, host=pg_host, port=pg_port, dbname =pg_db_name) as pg_connection:
		cursor = pg_connection.cursor()

		query = f'''
			COPY AVIATIONSTACK_HIST.{pg_table_name} ({minio_table_header}) 
			FROM PROGRAM 'curl "{minio_url}"' 
			WITH (FORMAT CSV, HEADER, QUOTE '"', DELIMITER ',')
			;
		'''
		a = pg_connection.execute(query)

	return (True, f'Data for Table: {pg_table_name} synced for {date.today()}')

def compare_list(old, new):
	new_set = set(new)
	old_set = set(old)
	inter = new_set & old_set

	added = new_set - inter if len(new_set - inter) > 0 else {}
	deleted = old_set - inter if len(old_set - inter) > 0 else {}
	unchanged = inter if len(inter) > 0 else {}

	return added, deleted, unchanged

def schema_check(minio_table_header: str, pg_table_name: str):
	with  psycopg.connect(user=pg_user, password=pg_password, host=pg_host, port=pg_port, dbname=pg_db_name) as pg_connection:
		cursor = pg_connection.cursor()
		query = f'''
			select 
				case when column_name = 'ingest_date' then 'snapshot_date' else column_name end as column_name
			from information_schema.columns
			where 
				table_schema = 'aviationstack'
				and table_name = '{pg_table_name}'
			order by ordinal_position
		'''
		cursor.execute(query)
		mobile_records = cursor.fetchall()
		new_header = [row[0] for row in mobile_records]
	current_header = minio_table_header.split(',')

	added, deleted, unchanged = compare_list(current_header, new_header)

	# print("added: ", len(added), ":", added) # new, in new file but not in current db table
	# print("deleted: ", len(deleted), ",", deleted) # deleted, not in new file but in current db table
	# print("unchanged: ", len(unchanged), ",", unchanged)

	output = {
		"added":{"length": len(added), "items": added},
		"deleted":{"length": len(deleted), "items": deleted},
		"unchanged":{"length": len(unchanged), "items": unchanged}
	}
	return output

def schema_append(new_column_to_add: str, pg_table_definition: str, pg_table_name: str):
	to_add = [column_data_type for column_data_type in pg_table_definition if new_column_to_add in column_data_type]
	column_to_add = to_add[0]
	with psycopg.connect(user=pg_user, password=pg_password, host=pg_host, port=pg_port, dbname=pg_db_name) as pg_connection:
		cursor = pg_connection.cursor()
		query = f'''
			ALTER TABLE AVIATIONSTACK_HIST.{pg_table_name}
			ADD COLUMN {column_to_add}
		'''
		# cursor.execute(query)
		print(query)
		query = f'''
			ALTER TABLE AVIATIONSTACK.{pg_table_name}
			ADD COLUMN {column_to_add}
		'''
		# cursor.execute(query)
		print(query)

	# for column in deleted: -- to work on if external tables are set up from Minio > Postgres as views
	return (True, f'Data for Table: {pg_table_name} updated with column : {new_column_to_add}')	