# -*- coding: utf-8 -*-
"""
Created on Wed May 19 14:26:57 2021

@author: Hp
"""

# Importing the required libraries
import xml.etree.ElementTree as Xet
import pandas as pd
import xmltodict
import csv
import sys,os,glob
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/config")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/utils")

import global_config

from global_utils import DA
CONFIG = global_config.CONFIG
logging = DA._init_log(os.path.basename(__file__))

LOCAL_CONFIG = {
"csv_loc" : os.path.dirname(os.path.abspath(__file__))+"/wdir/marketing_spend_fusion/csv/",
"xml_loc" : os.path.dirname(os.path.abspath(__file__))+"/wdir/xml_to_csv/xml/",
"download_loc" : os.path.dirname(os.path.abspath(__file__))+"/wdir/marketing_spend_fusion/download/",
"s3_folder":"marketing_spend_fusion",
"s3_configs":"s3_configs",
"temp_table_tracker_performance":"tmp.tbl_raw_mspend_fusion_tracker_performance",
"prod_table_tracker_performance":"public.tbl_raw_mspend_fusion_tracker_performance",
"prefix_jtv_tracker_performance":"Tracker - Performance",
"s3columns_tracker_performance":"etl_business_ts,etl_create_process,etl_datasource_id,etl_create_ts,Date,Traffic_source,Ad_Name,Impressions,Clicks,Cost_USD"
}  
# cols = ["genre_id", "genre_lang_en", "genre_lang_ar"]
# rows = []
  
# Parsing the XML file
xmlparse = Xet.parse(os.path.join(LOCAL_CONFIG["xml_loc"],'genre.xml'))
root = xmlparse.getroot()
xmlstr = Xet.tostring(root, encoding='utf8', method='xml')

data_dict = dict(xmltodict.parse(xmlstr))

print(data_dict)
# for i in root:
#     genre_id = i.find("genre_id").text
#     #genre_lang = i.find("genre lang").text
  
#     rows.append({"genre_id": genre_id,
#                  # "genre lang": genre_lang
#                  }
#                 )
  
# df = pd.DataFrame(rows, columns=cols)
  
# # Writing dataframe to csv
# df.to_csv(os.path.join(LOCAL_CONFIG["csv_loc"],'genre.csv'),index=False)