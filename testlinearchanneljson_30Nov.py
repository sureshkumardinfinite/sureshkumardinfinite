# -*- coding: utf-8 -*-
"""
Created on Mon Nov 30 20:16:55 2020

@author: Hp
"""

import functools
import paramiko 
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
"s3_configs":"s3_configs",

}



class AllowAnythingPolicy(paramiko.MissingHostKeyPolicy):
    def missing_host_key(self, client, hostname, key):
        return
def get_r_portable(adress,username,password):
   
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(AllowAnythingPolicy())
    client.connect(adress, username= username, password=password)
    
    
    sftp = client.open_sftp()
    sftp.chdir('/home/cc_intigraldemo/outgoing/jawwytv_home_prod/content/channel/processed')
    
    latest = 0
    latestfile = None
    
    for fileattr in sftp.listdir_attr():
        if fileattr.filename.endswith('ChannelList_Jawwy Home.json') and fileattr.st_mtime > latest:
            latest = fileattr.st_mtime
            latestfile = fileattr.filename
    
    if latestfile is not None:
        sftp.get(latestfile, latestfile)
    return 1

def main():
    print("Calling funton get_r_portable ")
    get_r_portable(LOCAL_CONFIG['adress'],LOCAL_CONFIG['username'],LOCAL_CONFIG['password'])
    
if __name__ == "__main__":
    main()