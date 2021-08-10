# -*- coding: utf-8 -*-
"""
Created on Mon Nov 16 20:16:59 2020

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
import ast
from pandas.io.json import json_normalize
from time import strptime


csv_path = "F:\intigral-bi\wdir\pushwoosh\csv\pushwoosh.csv"
csv_path_new="F:\intigral-bi\wdir\pushwoosh\csv\pushwoosh_converted.csv"
df_pushwoosh = pd.read_csv(csv_path) 
# print(df_pushwoosh.sample(5))

# df_pushwoosh = df_pushwoosh.join(pd.json_normalize(df_pushwoosh["tags"].tolist()).add_prefix("tags.")).drop(["tags"], axis=1)

df_pushwoosh = df_pushwoosh.join(pd.json_normalize(df_pushwoosh['tags'].map(json.loads).tolist()).add_prefix('tags.'))\
    .drop(['tags'], axis=1)
    
# df_pushwoosh.drop(['tzOffset','badges','androidPackages','latitude','longitude','publicKey','authToken'],axis=1) 

# print(df_pushwoosh)

df_pushwoosh.to_csv("F:\intigral-bi\wdir\pushwoosh\csv\pushwoosh_converted.csv",index=False)

df_new = pd.read_csv(csv_path_new)
del df_new['tzOffset']
del df_new['badges']
del df_new['androidPackages']
del df_new['latitude']
del df_new['longitude']
del df_new['publicKey']
del df_new['authToken']
del df_new['fcmToken']
del df_new['fcmPushSet']

# df_new.drop(['tzOffset','badges','androidPackages','latitude','longitude','publicKey','authToken'],axis=1) 

df_new.columns = [d.strip().replace('tags.', '') for d in df_new.columns]
df_new.columns = [d.strip().replace(' ', '') for d in df_new.columns]
del df_new['Tag_User_ID']
df_new_prcs=df_new.to_csv('F:\intigral-bi\wdir\pushwoosh\csv\pushwoosh_converted_final.csv', encoding='utf-8',quoting=csv.QUOTE_MINIMAL,index = False)