# -*- coding: utf-8 -*-
"""
Created on Tue Dec  1 14:09:40 2020

@author: Hp
"""

import pandas as pd
import numpy as np
import sys,os
import datetime
from datetime import timedelta

chunk_size = 100000
batch_no = 1
for chunk in pd.read_csv('event_attribution_e_2021-04-21.csv',chunksize=chunk_size,dtype=str,encoding="utf-8"):
    # chunk['lookback_seconds'] = chunk['lookback_seconds'].astype(str)
    # # chunk['lookback_seconds'] = chunk['lookback_seconds'].str.replace('.0', '')
    # chunk['lookback_seconds'] = chunk['lookback_seconds'].str.replace('nan', '')
    # chunk.insert(loc=7, column='type', value='POSTPAID_CTIVE')
    # chunk.insert(loc=8, column='dump_file_name',value='reports-20201206.tar.bz2-001')
    # chunk.insert(loc=9, column='etl_business_ts', value=datetime.datetime.now())
    # chunk.insert(loc=10, column='etl_create_process', value=str(60))
    # chunk.insert(loc=11, column='etl_datasource_id', value=str(13))    
    chunk.to_csv('part-'+str(batch_no)+'_etl_s_event_attribution_e_2021-04-21.csv',index=False)
    batch_no+=1