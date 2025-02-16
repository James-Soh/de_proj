#!/usr/bin/env python3
# import requests
# import json
# import os
from datetime import date
# import pandas as pd
from dotenv import load_dotenv
import os
import de_project_fx.module as data_sync

list = ['flights', 'airports', 'airlines', 'cities', 'countries']
today = date.today()
offset = 0
limit = 30000

load_dotenv('secret.env')
avs_key_1 = os.getenv("AVIATIONSTACK_ACESS_KEY")
avs_key_2 = os.getenv("AVIATIONSTACK_ACESS_KEY_2")

params = {'access_key': avs_key_1}
params2 = {'access_key': avs_key_2}

for endpoint in list:
    
	data_retrieveal_result = data_sync.aviationstack_data_retrieval(avs_endpoint=endpoint, params=params, api_limit=limit, api_offset=offset)
	print(data_retrieveal_result)
    
	# initial data retrieval failed, reztry with backup access_key
	if data_retrieveal_result['success'] == False:
		data_retrieveal_result = data_sync.aviationstack_data_retrieval(avs_endpoint=endpoint, params=params2, api_limit=offset, api_offset=limit)
		print(data_retrieveal_result)