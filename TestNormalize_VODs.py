# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 00:25:59 2020

@author: Sureshkumar D
"""


import numpy as np
import pandas as pd
import json
from pandas.io.json import json_normalize
import requests

url =  
response = requests.get(url)
content = response.content.decode('utf-8') # list of ugly strings
j = json.loads(content) # json list having nested dictionary

df_entries = pd.json_normalize(data=j['entries'])
del df_entries['scopes']

df_scopes = pd.json_normalize(data=j['entries'], record_path=["scopes"],meta = ["id"],record_prefix="scopes.")
df_images_landscape = pd.json_normalize(data=j['entries'], record_path=["images","landscape-highestResolution"],meta = ["id"],record_prefix="landscape-highestResolution.")
df_images_portrait = pd.json_normalize(data=j['entries'], record_path=["images","portrait-highestResolution"],meta = ["id"],record_prefix="portrait-highestResolution.")
df_images_pricingTiers = pd.json_normalize(data=j['entries'], record_path=["pricingPlan","pricingTiers"],meta = ["id"],record_prefix="pricingTiers.")

df_1    = df_entries.merge(df_scopes,how='left',on=['id'])
df_2    = df_1.merge(df_images_landscape,how='left',on=['id'])
df_3    = df_2.merge(df_images_portrait,how='left',on=['id'])
df_4    = df_3.merge(df_images_pricingTiers,how='left',on=['id'])
df_4.to_csv("VOD_data_Final_Aug31_2020.csv",index=False)

data = pd.read_csv("VOD_data_Final_Aug31_2020.csv") 
data = data.drop('images.landscape-highestResolution',axis=1)
data = data.drop('images.portrait-highestResolution',axis=1)
data = data.drop('pricingPlan.pricingTiers',axis=1)
data.columns = [d.strip().lower().replace('.', '_') for d in data.columns]
data.columns = [d.strip().lower().replace('-', '_') for d in data.columns]
data['pricingtiers_producttagids'] = data['pricingtiers_producttagids'].str.strip('[]').astype(str)
data['pricingtiers_producttags'] = data['pricingtiers_producttags'].str.strip('[]').astype(str)
data['pricingplan_masterproducttagids'] = data['pricingplan_masterproducttagids'].str.strip('[]').astype(str)
data['pricingtiers_rightsids'] = data['pricingtiers_rightsids'].str.strip('[]').astype(str)
data['pricingtiers_rightsids'] = data['pricingtiers_rightsids'].str.strip('\'').astype(str)



data.to_csv("VOD_data_Final_Aug31_2020.csv",index=False)


