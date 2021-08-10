# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 11:08:33 2020

@author: Hp
"""
import requests
import sys
import os
import urllib.request
import shutil
import json,csv
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/config")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/utils")

import global_config

from global_utils import DA
CONFIG = global_config.CONFIG

LOCAL_CONFIG = {
    "gadm_mp":"gadm_mp",
    "jh_mp":"jh_mp"
    }

account = 'http://access.auth.theplatform.com/data/Account/'+CONFIG['mp'][LOCAL_CONFIG['gadm_mp']]['accountId']
token = CONFIG['mp'][LOCAL_CONFIG['gadm_mp']]['Token']
myUrl = ''
head = '&account='+str(account)+'&token='+token
field_filter = '&fields=id,guid,updated,title,description,added,ownerId,addedByUserId,updatedByUserId,version,locked,approved,credits,descriptionLocalized,displayGenre,editorialRating,firstRunCompanyId,imageMediaIds,isAdult,languages,lastPubDate,longDescription,longTitle,partNumber,partTotal,programType,pubDate,ratings,runtime,secondaryTitle,seriesEpisodeNumber,seriesId,shortDescription,shortTitle,sortTitle,tagIds,tags,thumbnails,tvSeasonEpisodeNumber,tvSeasonId,tvSeasonNumber,year,distributionRightIds,mediacount,media'

response = requests.get(myUrl+head+field_filter)
print(myUrl+head)
start_resultsCursor=response.headers['X-thePlatform-ResultsCursor']

print('statuscode= '+str(response.status_code))
content_Mpx = response.content.decode('utf-8') # list of ugly strings
j_Mpx = json.loads(content_Mpx) 

df_j_Mpx = pd.json_normalize(data=j_Mpx['entries'])
# df_j_Mpx = df_j_Mpx.drop(df_j_Mpx.columns[[44,45,46,49, 63]], axis=1)

df_j_Mpx.to_csv("mpx_cursor_Test.csv",index=False)

tile_cnt_incr=df_j_Mpx['title'].size

print("count of unique Titles : "+str(tile_cnt_incr))

response_count = requests.get(myUrl+head+'&count=true')

print(myUrl+head+'&count=true')
print('statuscode= '+str(response_count.status_code))

content_Mpx_count = response_count.content.decode('utf-8') # list of ugly strings
j_Mpx_count = json.loads(content_Mpx_count)
total_svod = j_Mpx_count['totalResults']

print('total_svod='+str(total_svod))

while(tile_cnt_incr<total_svod):
    myUrl_mod = '='+start_resultsCursor
    
    response_mod = requests.get(myUrl_mod+head+field_filter)
    print(myUrl+head)
    
    
    print('statuscode= '+str(response_mod.status_code))
    content_Mpx_mod = response_mod.content.decode('utf-8') # list of ugly strings
    j_Mpx_mod = json.loads(content_Mpx_mod) 
    #RE intiating resultcursor
    start_resultsCursor=response_mod.headers['X-thePlatform-ResultsCursor']
    
    df_j_Mpx_mod = pd.json_normalize(data=j_Mpx_mod['entries'])
    # df_j_Mpx_mod = df_j_Mpx_mod.drop(df_j_Mpx_mod.columns[[44,45,46,49, 63]], axis=1)
    
    tile_cnt_delta = 0
    tile_cnt_delta=df_j_Mpx_mod['title'].size+tile_cnt_delta
    
    print("value of tile_cnt_delta  <current data frame title count>== : "+str(tile_cnt_delta))
    
    tile_cnt_incr=tile_cnt_delta+tile_cnt_incr
    
    print("latest value of tile_cnt_incr : "+str(tile_cnt_incr))
    
    df_j_Mpx_mod.to_csv("mpx_cursor_Test.csv",mode='a',header=False,index=False)
    
print("done")



v_stc_home_df=pd.read_csv("mpx_cursor_Test.csv", encoding="utf-8",dtype='unicode',delimiter=',')

    
v_stc_home_df.columns = [d.strip().lower().replace('.', '_') for d in v_stc_home_df.columns]
v_stc_home_df.columns = [d.strip().lower().replace('-', '_') for d in v_stc_home_df.columns]

v_stc_home_df['credits'] = v_stc_home_df['credits'].str.strip('[]').astype(str)
v_stc_home_df['imagemediaids'] = v_stc_home_df['imagemediaids'].str.strip('[]').astype(str)
v_stc_home_df['imagemediaids'] = v_stc_home_df['imagemediaids'].str.strip('[]').astype(str)
v_stc_home_df['languages'] = v_stc_home_df['languages'].str.strip('[]').astype(str)
v_stc_home_df['languages'] = v_stc_home_df['languages'].str.strip('\'').astype(str)
v_stc_home_df['tagids'] = v_stc_home_df['tagids'].str.strip('[]').astype(str)
v_stc_home_df['ratings'] = v_stc_home_df['ratings'].str.strip('[]').astype(str)

v_stc_home_df['media'] = v_stc_home_df['media'].str.strip('[]').astype(str)  
v_stc_home_df['tags'] = v_stc_home_df['tags'].str.strip('[]').astype(str)

v_stc_home_df['distributionrightids'] = v_stc_home_df['distributionrightids'].str.strip('[]').astype(str)
v_stc_home_df['distributionrightids'] = v_stc_home_df['distributionrightids'].str.strip('\'').astype(str)
v_stc_home_df['thumbnails_landscape_highestresolution_assettypes'] = v_stc_home_df['thumbnails_landscape_highestresolution_assettypes'].str.strip('[]').astype(str)
v_stc_home_df['thumbnails_portrait_highestresolution_assettypes'] = v_stc_home_df['thumbnails_portrait_highestresolution_assettypes'].str.strip('[]').astype(str)

v_stc_home_df_prcs=v_stc_home_df.to_csv("mpx_cursor_Test.csv", encoding='utf-8',quoting=csv.QUOTE_MINIMAL,index = False)
