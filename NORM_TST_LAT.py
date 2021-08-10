# -*- coding: utf-8 -*-
"""
Created on Sun Aug 16 17:16:27 2020

@author: Raji
"""
"""
Created on Wed Aug 12 00:25:59 2020

@author: Sureshkumar D
"""


import numpy as np
import json
import requests
from pandas.io.json import json_normalize
import pandas as pd
import csv  # For CSV dict writer

url =  ''
response = requests.get(url)
content = response.content.decode('utf-8') # list of ugly strings
j = json.loads(content) # json list having nested dictionary
#j.replace("pl1$AppCountries","pl1$AppCountries")
# obj_str = json.dumps(j).replace("pl1$AppCountries","pl1_AppCountries")
df_a = pd.json_normalize(data=j['entries'])
del df_a['ratings']
del df_a['tagIds']
del df_a['tags']
del df_a['media']
# del df_a['disabledDevices']
# del df_a['subscriptions']
df_ratings          = pd.json_normalize(data=j['entries'], record_path=["ratings"],          meta = ["id"],record_prefix="ratings.")
df_tagIds           = pd.json_normalize(data=j['entries'], record_path=["tagIds"],           meta = ["id"],record_prefix="tagIds.")
df_tags             = pd.json_normalize(data=j['entries'], record_path=["tags"],             meta = ["id"],record_prefix="tags.")
df_media            = pd.json_normalize(data=j['entries'], record_path=["media"],            meta = ["id"],record_prefix="media.")
# df_pl1_AppCountries = pd.json_normalize(data=j['entries'], record_path=["pl1$AppCountries"], meta = ["updated"],record_prefix="pl1$AppCountries.")

# df_media_tags = pd.json_normalize(data=df_media['media'], record_path=["ratings"], meta = ["id"],record_prefix=".ratings.")
# df_subscriptions = pd.json_normalize(data=j['channels'], record_path=["subscriptions"],meta = ["channelID"], record_prefix="subscriptions.")
# df_disabledDevices = pd.json_normalize(data=j['channels'], record_path=["disabledDevices"], meta = ["channelID"], record_prefix="disabledDevices.")
df    = df_a.merge(df_ratings,how='left',on=['id'])
df_1  = df.merge(df_tagIds,how='left',on=['id'])
df_2  = df_1.merge(df_tags,how='left',on=['id'])
df_3  = df_2.merge(df_media,how='left',on=['id'])
# df_4  = df_3.merge(df_pl1_AppCountries,how='left',on=['id'])
# df_dd  = df.merge(df_disabledDevices,how='left',on=['channelID'])
# df_subs  = df_dd.merge(df_subscriptions,how='left',on=['channelID'])

df_3.to_csv("VOD_TEST_ratings_tagid_tags_media_plapp_18Aug2020.csv",index=False)


v_stc_home_df=pd.read_csv("VOD_TEST_ratings_tagid_tags_media_plapp_18Aug2020.csv")
#v_stc_home_df.columns = [d.strip().lower().replace('pl1$.', 'PL1_',regex=True) for d in v_stc_home_df.columns]
# v_stc_home_df.replace({'pl1$AppCountries': {'$': '_'}},regex=True)
v_stc_home_df.columns = [d.strip().lower().replace('.', '_') for d in v_stc_home_df.columns]
v_stc_home_df.columns = [d.strip().lower().replace('-', '_') for d in v_stc_home_df.columns]

# v_stc_home_df.insert(loc=0, column='etl_business_ts', value=datetime.datetime.now()-datetime.timedelta(days=1))
# v_stc_home_df.insert(loc=1, column='etl_create_process', value=str(process_id))
# v_stc_home_df.insert(loc=2, column='etl_datasource_id', value=str(datasource_id))
# v_stc_home_df.insert(loc=3, column='etl_create_ts', value=datetime.datetime.now())
v_stc_home_df.insert(loc=4, column='channel_type', value='VOD_Mpx_Home')
v_stc_home_df['credits'] = v_stc_home_df['credits'].str.strip('[]').astype(str)
v_stc_home_df['imagemediaids'] = v_stc_home_df['imagemediaids'].str.strip('[]').astype(str)
v_stc_home_df['imagemediaids'] = v_stc_home_df['imagemediaids'].str.strip('[]').astype(str)
v_stc_home_df['languages'] = v_stc_home_df['languages'].str.strip('[]').astype(str)
v_stc_home_df['languages'] = v_stc_home_df['languages'].str.strip('\'').astype(str)
v_stc_home_df['pl1$subscriptionguids'] = v_stc_home_df['pl1$subscriptionguids'].str.strip('[]').astype(str)
v_stc_home_df['pl1$subscriptionguids'] = v_stc_home_df['pl1$subscriptionguids'].str.strip('\'').astype(str)
v_stc_home_df['thumbnails_landscape_highestresolution_assettypes'] = v_stc_home_df['thumbnails_landscape_highestresolution_assettypes'].str.strip('[]').astype(str)
v_stc_home_df['thumbnails_portrait_highestresolution_assettypes'] = v_stc_home_df['thumbnails_portrait_highestresolution_assettypes'].str.strip('[]').astype(str)
v_stc_home_df['ratings_subratings'] = v_stc_home_df['ratings_subratings'].str.strip('[]').astype(str)

v_stc_home_df=v_stc_home_df.to_csv("VOD_TEST_basesubscr_18Aug2020.csv", encoding='utf-8',quoting=csv.QUOTE_MINIMAL,index = False)
