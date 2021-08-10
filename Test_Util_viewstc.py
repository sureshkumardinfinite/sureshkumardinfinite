# -*- coding: utf-8 -*-
"""
Created on Thu May 13 11:49:48 2021

@author: Hp
"""
# -*- coding: utf-8 -*-
"""
Created on Wed May 12 18:51:33 2021

@author: Hp
"""
import psycopg2 as pg
import pandas as pd
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os,glob
import datetime
from datetime import timedelta
import psycopg2
import sys
import shutil
from time import strptime
from pretty_html_table import build_table
import pandas
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/config")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/utils")

import global_config

from global_utils import DA
CONFIG = global_config.CONFIG
logging = DA._init_log(os.path.basename(__file__)) 
LOCAL_CONFIG=    {

            "imap_server":"imap.gmail.com",
            "imap_port":"993",
            'username':"intigralmailfetcher@gmail.com",
            'password':"intigralmailfetcher_1",
            'subject':"",
            'sender':"sureshkumar.durairaj@intigral.net",
            "rs":"rs",
            'file_loc':os.path.dirname(os.path.abspath(__file__))+"/wdir/vw_stc_daily_report_emailer/download/",
            "prefix_with_time":False
            }

query = 'select * from vw_stc_daily_report'

logging.info("user" +CONFIG['rs']['user'])
logging.info("password" +CONFIG['rs']['password'])
logging.info("dbname" +CONFIG['rs']['dbname'])
logging.info("host" +CONFIG['rs']['host'])
engine = create_engine("redshift:///?User="+CONFIG['rs']['user']+"&;Password="+CONFIG['rs']['password']+"&Database="+CONFIG['rs']['dbname']+"&Server="+CONFIG['rs']['host']+"&Port=5439")
#logging.info("engine : "+engine) 


# connection = pg.connect(dbname= CONFIG['rs']['dbname'], host = CONFIG['rs']['host'],
# port= CONFIG['rs']['port'], user= CONFIG['rs']['user'], password= CONFIG['rs']['password'])
# logging.info("Connecting DB ")  
# cursor = connection.cursor()
# logging.info("Connected to DB ") 
# cursor.execute(query)
# logging.info("Query Executed ")  
df = pandas.read_sql("select * from vw_stc_daily_report", engine)
#data = pd.DataFrame(cursor.fetchall(),columns=['parameters', 'date', 'counts'])
df.to_csv(os.path.join(LOCAL_CONFIG["file_loc"],"vw_stc_daily_report.csv"),index=False)  
logging.info("df :"+df)