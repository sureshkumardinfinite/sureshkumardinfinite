# -*- coding: utf-8 -*-
"""
Created on Tue Mar  2 12:08:32 2021

@author: Hp
"""
import os,glob
import datetime
from datetime import timedelta
import getopt
import psycopg2
import csv
import math
import sys
import codecs
import base64
import urllib.request
import shutil
import json
import requests
import csv
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from time import strptime
from flatten_json import flatten
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/config")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/utils")

import global_config

from global_utils import DA
CONFIG = global_config.CONFIG
logging = DA._init_log(os.path.basename(__file__))

LOCAL_CONFIG = {
"csv_loc" : os.path.dirname(os.path.abspath(__file__))+"/wdir/ingest_mpx_data_sets_linear_VOD/csv/",
"download_loc" : os.path.dirname(os.path.abspath(__file__))+"/wdir/ingest_mpx_data_sets_linear_VOD/download/",
"s3_folder":"ingest_mpx_data_sets_linear_VOD",
"s3_configs":"s3_configs"
}

url =  ''
response = requests.get(url)
content = response.content.decode('utf-8') # list of ugly strings
j = json.loads(content) # json list having nested dictionary
data=j['entries']
df_entries =pd.json_normalize(data)
del df_entries['scopes']
dict_flattened = (flatten(record, '.') for record in data)
df_flat_1 = pd.DataFrame(dict_flattened)

df_flat_1.to_csv(os.path.join(LOCAL_CONFIG["csv_loc"],"Mpx_TVOD_DataRecords_1.csv"),index=False)

    
data = pd.read_csv(os.path.join(LOCAL_CONFIG["csv_loc"],"Mpx_TVOD_DataRecords_1.csv")) 

data.columns = [d.strip().lower().replace('.', '_') for d in data.columns]
data.columns = [d.strip().lower().replace('-', '_') for d in data.columns]
data.columns = [d.strip().lower().replace('0', '') for d in data.columns]
data.columns = [d.strip().lower().replace('__', '_') for d in data.columns]
data.columns = [d.strip().lower().replace('images_', '') for d in data.columns]
data.columns = [d.strip().lower().replace('pricingplan_pricingtiers_', 'pricingtiers_') for d in data.columns]
data.columns = [d.strip().lower().replace('pricingtiers_rightsids_', 'pricingtiers_rightsids') for d in data.columns]

    
data.to_csv(os.path.join(LOCAL_CONFIG["csv_loc"],"Mpx_TVOD_DataRecords_1.csv"),index=False)

url_2 =  ''
response_2 = requests.get(url_2)
content_2 = response_2.content.decode('utf-8') # list of ugly strings
j2 = json.loads(content_2) # json list having nested dictionary
data2=j2['entries']
df_entries2 =pd.json_normalize(data2)
del df_entries2['scopes']
dict_flattened2 = (flatten(record, '.') for record in data2)
df_flat_2 = pd.DataFrame(dict_flattened2)

df_flat_2.to_csv(os.path.join(LOCAL_CONFIG["csv_loc"],"Mpx_TVOD_DataRecords_2.csv"),index=False)

data1 = pd.read_csv(os.path.join(LOCAL_CONFIG["csv_loc"],"Mpx_TVOD_DataRecords_2.csv")) 
data1.columns = [d.strip().lower().replace('.', '_') for d in data1.columns]
data1.columns = [d.strip().lower().replace('-', '_') for d in data1.columns]
data1.columns = [d.strip().lower().replace('0', '') for d in data1.columns]
data1.columns = [d.strip().lower().replace('__', '_') for d in data1.columns]
data1.columns = [d.strip().lower().replace('images_', '') for d in data1.columns]
data1.columns = [d.strip().lower().replace('pricingplan_pricingtiers_', 'pricingtiers_') for d in data1.columns]
data1.columns = [d.strip().lower().replace('pricingtiers_rightsids_', 'pricingtiers_rightsids') for d in data1.columns]


data1.to_csv(os.path.join(LOCAL_CONFIG["csv_loc"],"Mpx_TVOD_DataRecords_2.csv"),index=False)

url_3 =  ''
response_3 = requests.get(url_3)
content_3 = response_3.content.decode('utf-8') # list of ugly strings
j3 = json.loads(content_3) # json list having nested dictionary
data3=j3['entries']
df_entries3 =pd.json_normalize(data3)
del df_entries3['scopes']
dict_flattened3 = (flatten(record, '.') for record in data3)
df_flat_3 = pd.DataFrame(dict_flattened3)

df_flat_3.to_csv(os.path.join(LOCAL_CONFIG["csv_loc"],"Mpx_TVOD_DataRecords_3.csv"),index=False)

data2 = pd.read_csv(os.path.join(LOCAL_CONFIG["csv_loc"],"Mpx_TVOD_DataRecords_3.csv")) 
data2.columns = [d.strip().lower().replace('.', '_') for d in data2.columns]
data2.columns = [d.strip().lower().replace('-', '_') for d in data2.columns]
data2.columns = [d.strip().lower().replace('0', '') for d in data2.columns]
data2.columns = [d.strip().lower().replace('__', '_') for d in data2.columns]
data2.columns = [d.strip().lower().replace('images_', '') for d in data2.columns]
data2.columns = [d.strip().lower().replace('pricingplan_pricingtiers_', 'pricingtiers_') for d in data2.columns]
data2.columns = [d.strip().lower().replace('pricingtiers_rightsids_', 'pricingtiers_rightsids') for d in data2.columns]

    
data2.to_csv(os.path.join(LOCAL_CONFIG["csv_loc"],"Mpx_TVOD_DataRecords_3.csv"),index=False)