# -*- coding: utf-8 -*-
"""
Created on Thu Mar  4 21:19:54 2021

@author: Hp
"""


#-------------------------------------------------------------------------------
# Name:        module2
# Purpose:
#
# Author:      Sureshkumar
#
# Created:     24/07/2020
# Copyright:   (c) dsk 2020
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import time,hashlib,webbrowser,os,sys,datetime,shutil,glob
from urllib import request
import csv
import json
import flatten_json
import hashlib
import requests
import getopt
import zipfile, urllib.request, shutil
import urllib
import traceback
import psycopg2,pandas as pd
from datetime import datetime,timedelta
import numpy as np
import smtplib
import email, smtplib, ssl
import xlsxwriter
import openpyxl
from string import ascii_uppercase
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/config")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/utils")

import global_config

from global_utils import DA
CONFIG = global_config.CONFIG
logging = DA._init_log(os.path.basename(__file__))

LOCAL_CONFIG = {
"csv_loc" : os.path.dirname(os.path.abspath(__file__))+"/wdir/logging_automation_tbl_count/csv/"
}


def main():
    logging.info("start")
    _st_=datetime.now()
    script_name = os.path.basename(__file__)
    logging.info("empty csv folder:"+LOCAL_CONFIG['csv_loc'])
    try:

        if not os.path.exists(LOCAL_CONFIG['csv_loc']):
            os.makedirs(LOCAL_CONFIG['csv_loc'])
        else:
            DA._empty_dir(LOCAL_CONFIG['csv_loc'],'*')

    except (Exception) as error :
        DA._log_etl(script_name, 'logging_automation_tbl_count', None, None, "error empty segment folder: "+str(error))
        logging.error("error empty segment folder: "+str(error))
        return
    
    conn = psycopg2.connect(dbname=CONFIG['rs']['dbname'], host=CONFIG['rs']['host'], port=CONFIG['rs']['port'], user=CONFIG['rs']['user'], password=CONFIG['rs']['password'])

    logging.info('Kochava Daily Load...')
    cmd_kochava_ld ='select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_click_properties\' as table_name  from public.tbl_raw_kochava_click_properties  union all select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_clicks_primary\' as table_name  from public.tbl_raw_kochava_clicks_primary  union all select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_event_attribution\' as table_name  from public.tbl_raw_kochava_event_attribution  union all select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_event_dimensions\' as table_name  from public.tbl_raw_kochava_event_dimensions  union all select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_events_primary\' as table_name  from public.tbl_raw_kochava_events_primary  union all select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_identity_link\' as table_name  from public.tbl_raw_kochava_identity_link  union all select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_install_attribution\' as table_name  from public.tbl_raw_kochava_install_attribution  union all select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_install_identifiers\' as table_name  from public.tbl_raw_kochava_install_identifiers  union all select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_install_influencers\' as table_name  from public.tbl_raw_kochava_install_influencers  union all select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_installs_primary\' as table_name  from public.tbl_raw_kochava_installs_primary  union all select COUNT(*) as COUNT, max(etl_create_ts) as etl_create_ts,max(etl_business_ts) as etl_business_ts,\'tbl_raw_kochava_postback_logs\'  from public.tbl_raw_kochava_postback_logs'
    SQL_Query5 = pd.read_sql_query(cmd_kochava_ld,conn)
    
    logging.info('Checking the daily load for Kochava Daily Load Tables ')
    logging.info('Query'+cmd_kochava_ld)
    flag=DA._execute_rs(cmd_kochava_ld)
    df_kochava_ld = pd.DataFrame(SQL_Query5, columns=['COUNT','etl_create_ts','etl_business_ts','table_name'])
    
    df_kochava_ld['etl_business_ts'] = pd.to_datetime(df_kochava_ld['etl_business_ts']).dt.strftime('%Y-%m-%d')
    df_kochava_ld['etl_create_ts'] = pd.to_datetime(df_kochava_ld['etl_create_ts']).dt.strftime('%Y-%m-%d')
    df_kochava_ld['etl_compare_ts']=datetime.now() - timedelta(1)
    df_kochava_ld['etl_compare_ts']=pd.to_datetime(df_kochava_ld['etl_compare_ts']).dt.strftime('%Y-%m-%d')

    df_kochava_ld['Error_message'] = np.where(df_kochava_ld['etl_create_ts'] == df_kochava_ld['etl_compare_ts'], '', 'Not Loaded as per Schedule')
    print(df_kochava_ld)
if __name__ == '__main__':
    main() 