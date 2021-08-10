# -*- coding: utf-8 -*-
"""
Created on Tue Oct 27 16:47:38 2020

@author: Hp
"""

import imaplib
import sys
import email
import os
import datetime
from datetime import timedelta
from copy import copy
import openpyxl
from openpyxl.utils import get_column_letter
import pandas as pd
import csv
import shutil
import calendar
from time import strptime
import numpy as np
from pandas.tseries.offsets import MonthEnd,MonthBegin

wkly_df=pd.read_csv(os.path.join('F:\intigral-bi\wdir\ingest_weekly_monthly_revenue\csv','week_B2C_Revenues_2020.csv'))

# v_wk_start_dt = wkly_df['Week Start Date'][0]
# v_wk_end_dt =wkly_df['Week End Date'][0]
# v_counter =1
# print(len(wkly_df))
# print(len(wkly_df.columns))
# for i in range(len(wkly_df)):
#     i=i+4
#     # print(wkly_df['Revenue'])
#     if(wkly_df['Revenue'].equals('Week '+str(v_counter))):
#         curr_wk_start_dt = wkly_df['Week Start Date'][v_counter]
#         curr_wk_end_dt   = wkly_df['Week End Date'][v_counter]
#         print(curr_wk_start_dt)
#         print(curr_wk_end_dt)
#     # else :
#     #     curr_wk_start_dt = v_wk_start_dt
#     #     curr_wk_end_dt   = v_wk_end_dt
#         # print(curr_wk_start_dt)
    
#     v_counter=v_counter+1
# print(wkly_df.iloc[0:158, 0:4] )
newdf_1 = wkly_df[wkly_df.columns[0:4]]
newdf_1 = newdf_1.rename(columns={'STB-KSA': 'values'})
newdf_1 = newdf_1.rename(columns={'Revenue': 'Header_Name'})
newdf_1['Revenue_name']='STB-KSA'

newdf_1[newdf_1['Week Start Date']==""] = np.NaN
newdf_1['Week Start Date']=newdf_1.fillna(method='ffill')
newdf_1[newdf_1['Week End Date']==""] = np.NaN
newdf_1['Week End Date'].fillna(method='ffill', inplace=True)
            
print(newdf_1)
 
 
 
 