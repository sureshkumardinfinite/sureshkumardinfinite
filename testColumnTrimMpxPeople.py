# -*- coding: utf-8 -*-
"""
Created on Sun Feb  7 10:39:35 2021

@author: Hp
"""
import os
from datetime import datetime, timedelta
import getopt
import psycopg2
import csv
import math
import sys
import codecs
import base64
import urllib.request
import shutil
import json
import traceback
import pandas as pd
import glob,string


sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/config")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/utils")

import global_config

from global_utils import DA
CONFIG = global_config.CONFIG
logging = DA._init_log(os.path.basename(__file__))




LOCAL_CONFIG = {
"s3_folder":"mixpanel_people_jtv_app",
"jw_mp":"jw_mp",
"csv_loc" : os.path.dirname(os.path.abspath(__file__))+"/wdir/mixpanel_people_jtv_app/csv/",
"prefix":"part",
"batch_len":25000000,
"s3_configs":"s3_configs",

"temp_table":"tmp.tbl_raw_mixpanel_people_jawwytv_app",
"prod_table":"public.tbl_raw_mixpanel_people_jawwytv_app",
"days_ago":5,
"interval_hr": 18 , # last number is hours
"rs_option" : " csv timeformat 'auto' delimiter ','  ACCEPTANYDATE BLANKSASNULL ",
"COL_LIST" : ["\"# of App Open\"",
"\"int::# of App Open\"",
"\"# of Guest Popup Views\"",
"\"int::# of Guest Popup Views\"",
"\"# of Items Shared\"",
"\"int::# of Items Shared\"",
"\"# Of Password Resends\"",
"\"int::# Of Password Resends\"",
"\"# of Screen opened\"",
"\"int::# of Screen opened\"",
"\"# of Searches\"",
"\"int::# of Searches\"",
"\"# Of Sign-in Attempts\"",
"\"int::# Of Sign-in Attempts\"",
"\"# of User Login\"",
"\"int::# of User Login\"",
"\"# Of Verification Attempts\"",
"\"int::# Of Verification Attempts\"",
"\"# of Verification attempts_\"",
"\"int::# of Verification attempts_\"",
"\"# of Verification Emails skipped\"",
"\"int::# of Verification Emails skipped\"",
"\"# Of Verification Failures\"",
"\"int::# Of Verification Failures\"",
"\"# Of Verification Resends\"",
"\"int::# Of Verification Resends\"",
"\"$android_app_version_code\"",
"\"$android_app_version\"",
"\"$android_brand\"",
"\"$android_devices\"",
"\"$android_invalid_push_tokens\"",
"\"$android_lib_version\"",
"\"$android_manufacturer\"",
"\"$android_model\"",
"\"$android_os_version\"",
"\"$android_os\"",
"\"$android_push_error\"",
"\"App Language\"",
"\"$browser_version\"",
"\"$browser\"",
"\"$city\"",
"\"Country\"",
"\"$country_code\"",
"\"Email Verified\"",
"\"Email\"",
"\"$email\"",
"\"First App Login\"",
"\"date::First App Login\"",
"\"First Login Date\"",
"\"date::First Login Date\"",
"\"$initial_referrer\"",
"\"$initial_referring_domain\"",
"\"$ios_app_release\"",
"\"$ios_app_version\"",
"\"$ios_device_model\"",
"\"$ios_devices\"",
"\"$ios_ifa\"",
"\"$ios_lib_version\"",
"\"$ios_version\"",
"\"Language\"",
"\"Last Home View\"",
"\"date::Last Home View\"",
"\"Last Login\"",
"\"date::Last Login\"",
"\"Last Search Keyword\"",
"\"date::Last Search Keyword\"",
"\"Last Search\"",
"\"date::Last Search\"",
"\"$last_seen\"",
"\"date::$last_seen\"",
"\"Last Share\"",
"\"date::Last Share\"",
"\"Last Shared Item\"",
"\"Last Sign out\"",
"\"date::Last Sign out\"",
"\"Last Verification Attempt\"",
"\"date::Last Verification Attempt\"",
"\"Last Verification Failure Date\"",
"\"date::Last Verification Failure Date\"",
"\"Last View home\"",
"\"date::Last View home\"",
"\"Last Watch\"",
"\"date::Last Watch\"",
"\"Live TV - View\"",
"\"float::Live TV - View\"",
"\"Mobile Number\"",
"\"$name\"",
"\"$os\"",
"\"Operator Name\"",
"\"Page Last Visit\"",
"\"date::Page Last Visit\"",
"\"Payment Method\"",
"\"Payment Plan\"",
"\"$phone\"",
"\"$predict_grade - Test\"",
"\"$predict_grade - test_\"",
"\"$predict_grade - Test2\"",
"\"$predict_grade\"",
"\"$predict_score\"",
"\"float::$predict_score\"",
"\"$region\"",
"\"Search Keywords\"",
"\"Subscription Date\"",
"\"date::Subscription Date\"",
"\"Subscription Last Visit\"",
"\"date::Subscription Last Visit\"",
"\"Subscription Price\"",
"\"Subscription Type\"",
"\"$timezone\"",
"\"$ae_total_app_session_length\"",
"ceil(\"int::$ae_total_app_session_length\")",
"\"$ae_total_app_sessions\"",
"\"int::$ae_total_app_sessions\"",
"\"Total Items Watched\"",
"\"int::Total Items Watched\"",
"\"Total Watch Hours\"",
"\"float::Total Watch Hours\"",
"\"User ID\"",
"\"User Id_\"",
"\"distinct_id\"",
"\"mp_last_seen\"",
"\"etl_business_ts\"",
"\"etl_create_process\"",
"\"etl_datasource_id\"" ]

}

COLUMNS_MAPPING={
"# of App Open":"# of App Open",
"int::# of App Open":"# of App Open",

"# of Guest Popup Views":"# of Guest Popup Views",
"int::# of Guest Popup Views":"# of Guest Popup Views",

"# of Items Shared":"# of Items Shared",
"int::# of Items Shared":"# of Items Shared",

"# Of Password Resends":"# Of Password Resends",
"int::# Of Password Resends":"# Of Password Resends",

"# of Screen opened":"# of Screen opened",
"int::# of Screen opened":"# of Screen opened",

"# of Searches":"# of Searches",
"int::# of Searches":"# of Searches",

"# Of Sign-in Attempts":"# Of Sign-in Attempts",
"int::# Of Sign-in Attempts":"# Of Sign-in Attempts",

"# of User Login":"# of User Login",
"int::# of User Login":"# of User Login",

"# Of Verification Attempts":"# Of Verification Attempts",
"int::# Of Verification Attempts":"# Of Verification Attempts",

"# of Verification attempts_":"# of Verification attempts",
"int::# of Verification attempts_":"# of Verification attempts",

"# of Verification Emails skipped":"# of Verification Emails skipped",
"int::# of Verification Emails skipped":"# of Verification Emails skipped",

"# Of Verification Failures":"# Of Verification Failures",
"int::# Of Verification Failures":"# Of Verification Failures",

"# Of Verification Resends":"# Of Verification Resends",
"int::# Of Verification Resends":"# Of Verification Resends",

"$android_app_version_code":"$android_app_version_code",
"$android_app_version":"$android_app_version",
"$android_brand":"$android_brand",
"$android_devices":"$android_devices",
"$android_invalid_push_tokens":"$android_invalid_push_tokens",
"$android_lib_version":"$android_lib_version",
"$android_manufacturer":"$android_manufacturer",
"$android_model":"$android_model",
"$android_os_version":"$android_os_version",
"$android_os":"$android_os",
"$android_push_error":"$android_push_error",
"App Language":"App Language",
"$browser_version":"$browser_version",
"$browser":"$browser",
"$city":"$city",
"Country":"Country",
"$country_code":"$country_code",
"Email Verified":"Email Verified",
"Email":"Email",
"$email":"$email",
"First App Login":"First App Login",
"date::First App Login":"First App Login",
"First Login Date":"First Login Date",
"date::First Login Date":"First Login Date",
"$initial_referrer":"$initial_referrer",
"$initial_referring_domain":"$initial_referring_domain",
"$ios_app_release":"$ios_app_release",
"$ios_app_version":"$ios_app_version",
"$ios_device_model":"$ios_device_model",
"$ios_devices":"$ios_devices",
"$ios_ifa":"$ios_ifa",
"$ios_lib_version":"$ios_lib_version",
"$ios_version":"$ios_version",
"Language":"Language",
"Last Home View":"Last Home View",
"date::Last Home View":"Last Home View",
"Last Login":"Last Login",
"date::Last Login":"Last Login",
"Last Search Keyword":"Last Search Keyword",
"date::Last Search Keyword":"Last Search Keyword",
"Last Search":"Last Search",
"date::Last Search":"Last Search",
"$last_seen":"$last_seen",
"date::$last_seen":"$last_seen",
"Last Share":"Last Share",
"date::Last Share":"Last Share",
"Last Shared Item":"Last Shared Item",
"Last Sign out":"Last Sign out",
"date::Last Sign out":"Last Sign out",
"Last Verification Attempt":"Last Verification Attempt",
"date::Last Verification Attempt":"Last Verification Attempt",
"Last Verification Failure Date":"Last Verification Failure Date",
"date::Last Verification Failure Date":"Last Verification Failure Date",
"Last View home":"Last View home",
"date::Last View home":"Last View home",
"Last Watch":"Last Watch",
"date::Last Watch":"Last Watch",

"Live TV - View":"Live TV - View",
"float::Live TV - View":"Live TV - View",

"Mobile Number":"Mobile Number",
"$name":"$name",
"$os":"$os",
"Operator Name":"Operator Name",
"Page Last Visit":"Page Last Visit",
"date::Page Last Visit":"Page Last Visit",
"Payment Method":"Payment Method",
"Payment Plan":"Payment Plan",
"$phone":"$phone",
"$predict_grade - Test":"$predict_grade - Test",
"$predict_grade - test_":"$predict_grade - test",
"$predict_grade - Test2":"$predict_grade - Test2",
"$predict_grade":"$predict_grade",

"$predict_score":"$predict_score",
"float::$predict_score":"$predict_score",

"$region":"$region",
"Search Keywords":"Search Keywords",
"Subscription Date":"Subscription Date",
"date::Subscription Date":"Subscription Date",
"Subscription Last Visit":"Subscription Last Visit",
"date::Subscription Last Visit":"Subscription Last Visit",
"Subscription Price":"Subscription Price",
"Subscription Type":"Subscription Type",
"$timezone":"$timezone",

"$ae_total_app_session_length":"$ae_total_app_session_length",
"int::$ae_total_app_session_length":"$ae_total_app_session_length",

"$ae_total_app_sessions":"$ae_total_app_sessions",
"int::$ae_total_app_sessions":"$ae_total_app_sessions",

"Total Items Watched":"Total Items Watched",
"int::Total Items Watched":"Total Items Watched",

"Total Watch Hours":"Total Watch Hours",
"float::Total Watch Hours":"Total Watch Hours",

"User ID":"User ID",
"User Id_":"User Id",
"distinct_id":"<distinct_id>",
"mp_last_seen":"<people_last_seen>",
"etl_business_ts":"<etl_business_ts>",
"etl_create_process":"<etl_create_process>",
"etl_datasource_id":"<etl_datasource_id>"

}

all_files = glob.glob(os.path.join(LOCAL_CONFIG['csv_loc'], "*.csv"))
for filename in all_files:
    colnames = ["# of App Open","int::# of App Open","# of Guest Popup Views","int::# of Guest Popup Views","# of Items Shared","int::# of Items Shared","# Of Password Resends","int::# Of Password Resends","# of Screen opened","int::# of Screen opened","# of Searches","int::# of Searches","# Of Sign-in Attempts","int::# Of Sign-in Attempts",
"# of User Login","int::# of User Login","# Of Verification Attempts","int::# Of Verification Attempts",
"# of Verification attempts_","int::# of Verification attempts_","# of Verification Emails skipped",
"int::# of Verification Emails skipped","# Of Verification Failures","int::# Of Verification Failures","# Of Verification Resends",
"int::# Of Verification Resends","$android_app_version_code","$android_app_version","$android_brand",
"$android_devices","$android_invalid_push_tokens","$android_lib_version","$android_manufacturer","$android_model","$android_os_version","$android_os","$android_push_error","App Language","$browser_version","$browser","$city","Country","$country_code",
"Email Verified","Email","$email","First App Login","date::First App Login","First Login Date","date::First Login Date","$initial_referrer","$initial_referring_domain",
"$ios_app_release","$ios_app_version","$ios_device_model","$ios_devices","$ios_ifa","$ios_lib_version","$ios_version","Language","Last Home View","date::Last Home View","Last Login","date::Last Login","Last Search Keyword","date::Last Search Keyword","Last Search","date::Last Search","$last_seen","date::$last_seen","Last Share",
"date::Last Share","Last Shared Item","Last Sign out","date::Last Sign out","Last Verification Attempt","date::Last Verification Attempt","Last Verification Failure Date","date::Last Verification Failure Date","Last View home","date::Last View home","Last Watch","date::Last Watch","Live TV - View","float::Live TV - View","Mobile Number","$name","$os","Operator Name","Page Last Visit","date::Page Last Visit","Payment Method","Payment Plan","$phone","$predict_grade - Test","$predict_grade - test_","$predict_grade - Test2","$predict_grade","$predict_score","float::$predict_score","$region","Search Keywords","Subscription Date","date::Subscription Date","Subscription Last Visit","date::Subscription Last Visit","Subscription Price","Subscription Type","$timezone","$ae_total_app_session_length","ceil(int::$ae_total_app_session_length)","$ae_total_app_sessions","int::$ae_total_app_sessions","Total Items Watched","int::Total Items Watched","Total Watch Hours","float::Total Watch Hours","User ID","User Id_","distinct_id","mp_last_seen","etl_business_ts","etl_create_process","etl_datasource_id" ]
    df = pd.read_csv(filename, index_col=None, names=colnames,low_memory=False)
    print(df.head())
    # df.apply(lambda x: x.str.slice(0, 5000))
    # print(df)
    # # print(df.iloc[:,[29]])
    df["$android_devices"]=df["$android_devices"].astype(str).str[:5000]
    # print(df["$android_devices"].map(len))
    # df.to_csv(os.path.join(LOCAL_CONFIG["csv_loc"],filename),index=False)
