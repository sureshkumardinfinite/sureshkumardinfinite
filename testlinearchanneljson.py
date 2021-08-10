# -*- coding: utf-8 -*-
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

import time,json,csv
from stat import S_ISDIR, S_ISREG,S_ISDIR
import datetime
from datetime import timedelta
from time import strptime
import os
import sys
import pysftp
import pandas as pd
from pandas.io.json import json_normalize #package for flattening json in pandas df

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/config")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/utils")

import global_config

from global_utils import DA
CONFIG = global_config.CONFIG
logging = DA._init_log(os.path.basename(__file__))

LOCAL_CONFIG = {
"csv_loc" : os.path.dirname(os.path.abspath(__file__))+"/wdir/LinearChannel_ICE_SrcExtract/csv/",
"download_gadm_loc" : os.path.dirname(os.path.abspath(__file__))+"/wdir/LinearChannel_ICE_SrcExtract/download/gadm/",
"download_jawwyh_loc" : os.path.dirname(os.path.abspath(__file__))+"/wdir/LinearChannel_ICE_SrcExtract/download/jawwy_home/",
"ice_output":"ice_output",
"source_report_gadm":"ChannelList_Jawwy App",
"source_report_jawwyh":"ChannelList_Jawwy Home",
"remote_path_gadm":"/home/cc_intigraldemo/outgoing/gadmtv_prod/content/channel/processed",
"remote_path_jawwyh":"/home/cc_intigraldemo/outgoing/jawwytv_home_prod/content/channel/processed",
"s3_folder":"LinearChannel_ICE_SrcExtract",
"s3_configs":"s3_configs"
}


def get_r_portable(sftp, remotedir, localdir, preserve_mtime=False):
    # latest = 0
    # latestfile = None
    for entry in sftp.listdir_attr(remotedir):
        remotepath = remotedir + "/" + entry.filename
        localpath = os.path.join(localdir, entry.filename)
        logging.info("Time:"+str(entry.st_mtime) )
        # logging.info("localpath : "+localpath)
        # logging.info("entry.filename:"+entry.filename)
        # fileName_gadm=entry.filename
        mode = entry.st_mode
        if S_ISDIR(mode):
            try:
                os.mkdir(localpath)
            except OSError:     
                pass
            get_r_portable(sftp, remotepath, localpath, preserve_mtime)
        elif S_ISREG(mode):
            if (time.time() - entry.st_mtime) // (24 * 3600) <= 21:
                sftp.get(remotepath, localpath, preserve_mtime=preserve_mtime)
      
remote_path_gadm=LOCAL_CONFIG['remote_path_gadm']
remote_path_jawwyh=LOCAL_CONFIG['remote_path_jawwyh']
local_path_gadm=LOCAL_CONFIG['download_gadm_loc']
local_path_jawwyh=LOCAL_CONFIG['download_jawwyh_loc']

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None    
logging.info("start")
_st_=datetime.datetime.now()
sftp=pysftp.Connection('54.72.82.177', username='suresh',password= 'zVCheLs6y2feA7tG',cnopts=cnopts)

logging.info("fetch file for Gadm Linear Channel")
get_r_portable(sftp, remote_path_gadm, local_path_gadm, preserve_mtime=False)

files_list_gadm=DA._findin_dir(local_path_gadm,LOCAL_CONFIG['source_report_gadm'])
file_gadm=str(files_list_gadm)
input_File_gadm = local_path_gadm+file_gadm.replace('[','').replace(']','').replace('\'','')

logging.info("===============Processing FileName : "+input_File_gadm+"===============")   


with open(input_File_gadm,'r',encoding='UTF-8') as f:
        d = json.load(f)
df_Final = pd.json_normalize(d['channels'])
df_Final.to_csv(os.path.join(LOCAL_CONFIG['csv_loc'],'LinearChannel_ICE_SrcExtract_gadm.csv'),index=False)
 

logging.info("Csv File Converted initial")
l_gadm=pd.read_csv(os.path.join(LOCAL_CONFIG['csv_loc'],'LinearChannel_ICE_SrcExtract_gadm.csv'))

l_gadm.columns = [d.strip().lower().replace('.', '_') for d in l_gadm.columns]
l_gadm['subscriptionguids'] = l_gadm['subscriptionguids'].str.strip('[]').astype(str)
l_gadm['subscriptionguids'] = l_gadm['subscriptionguids'].str.strip('\'').astype(str)
l_gadm['rights_apppl_jawapponly']=l_gadm['rights_apppl_jawapponly'].str.strip('[]').astype(str)
l_gadm['rights_device_cotv']=l_gadm['rights_device_cotv'].str.strip('[]').astype(str)
l_gadm['rights_device_gameco']=l_gadm['rights_device_gameco'].str.strip('[]').astype(str)
l_gadm['rights_device_pc']=l_gadm['rights_device_pc'].str.strip('[]').astype(str)
l_gadm['rights_device_ipstb']=l_gadm['rights_device_ipstb'].str.strip('[]').astype(str)
l_gadm['rights_device_hybridstb']=l_gadm['rights_device_hybridstb'].str.strip('[]').astype(str)
l_gadm['rights_device_moph']=l_gadm['rights_device_moph'].str.strip('[]').astype(str)
l_gadm['rights_device_ta']=l_gadm['rights_device_ta'].str.strip('[]').astype(str)


l_gadm.to_csv(os.path.join(LOCAL_CONFIG['csv_loc'],'LinearChannel_ICE_SrcExtract_gadm.csv'),index=False)

logging.info("Csv File Generated for Gadm")

logging.info("fetch file for jawwhome Linear Channel")
get_r_portable(sftp, remote_path_jawwyh, local_path_jawwyh, preserve_mtime=False)

files_list_jawwyh=DA._findin_dir(local_path_jawwyh,LOCAL_CONFIG['source_report_jawwyh'])
file_jawwyh=str(files_list_jawwyh)
input_File_jawwyh = local_path_jawwyh+file_jawwyh.replace('[','').replace(']','').replace('\'','')

logging.info("===============Processing FileName : "+input_File_jawwyh+"===============")   


with open(input_File_jawwyh,'r',encoding='UTF-8') as f:
        d1 = json.load(f)
df_Final_jh = pd.json_normalize(d1['channels'])
df_Final_jh.to_csv(os.path.join(LOCAL_CONFIG['csv_loc'],'LinearChannel_ICE_SrcExtract_jawwyh.csv'),index=False)
 

logging.info("Csv File Converted initial")
l_jawwyh=pd.read_csv(os.path.join(LOCAL_CONFIG['csv_loc'],'LinearChannel_ICE_SrcExtract_jawwyh.csv'))



_et_ = datetime.datetime.now()
_time_ = (_et_-_st_).total_seconds()
logging.info("time in seconds = {0}".format(str(_time_)))

logging.info("end")