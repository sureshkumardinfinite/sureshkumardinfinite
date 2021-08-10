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
import numpy as np
import csv
import shutil
import calendar
from time import strptime
from pandas.tseries.offsets import MonthEnd,MonthBegin

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/config")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/utils")

import global_config

from global_utils import DA
CONFIG = global_config.CONFIG
logging = DA._init_log(os.path.basename(__file__))


LOCAL_CONFIG=   {
                'fetch_config':
                    {
                        'RVN': 
                            { 
            "imap_server":"outlook.office365.com",
            "imap_port":"993",
            'username':"intigral_a@intigral.net",
            'password':"jNBgo65UM8",
            'subject':"SPN cohorts configuration file",
            'sender':"",
            'file_loc':os.path.dirname(os.path.abspath(__file__))+"/wdir/customermarketing_mpx_campaign/download/",
            'csv_loc':os.path.dirname(os.path.abspath(__file__))+"/wdir/customermarketing_mpx_campaign/csv/",
            'archive_loc':os.path.dirname(os.path.abspath(__file__))+"/wdir/customermarketing_mpx_campaign/archive/",
            "prefix_with_time":False,
            's3_configs' : "s3_configs",
            'source_report':"Segmented_PN_cohorts_configuration",
            's3_folder':"customermarketing_mpx_campaign",
            'local_csv_loc' :os.path.dirname(os.path.abspath(__file__))+"/wdir/customermarketing_mpx_campaign/csv/",
            'local_archive_loc' :os.path.dirname(os.path.abspath(__file__))+"/wdir/customermarketing_mpx_campaign/archive/"            
                            }
                    }
                }


def readAttachment(imap_server,imap_port,username,password,subject,sender,file_loc,seen ,extra_filter,prefix_with_time):

    try:

        fltr=""
        extra_filter=extra_filter.strip()

        if sender.strip()!="":
            fltr+='HEADER FROM "'+sender+'"'

        if subject.strip()!="":
            if fltr!="":
                fltr+=' SUBJECT "'+subject+'"'
            else:
                fltr+='SUBJECT "'+subject+'"'
        if seen ==False:
            if fltr!="":
                fltr+=' UNSEEN'
            else:
                fltr+='UNSEEN'


        if extra_filter.strip()!="":
            if fltr!="":
                fltr+=' '+extra_filter
            else:
                fltr+=extra_filter

        fltr="("+fltr+")"
        
        if not os.path.exists(file_loc):
                os.makedirs(file_loc)

        logging.info("connect to mail server:"+imap_server+':'+str(imap_port))
        mail=imaplib.IMAP4_SSL(imap_server,imap_port)
        mail.login(username,password)
        logging.info("connected")

        logging.info("set folder to INBOX")

        mail.select('"INBOX"')


        logging.info("set filter to:"+fltr)
        typ, msgs = mail.search(None, fltr )
        msgs = msgs[0].split()


        logging.info("reading mail list")
        if(len(msgs)==0):
            logging.info("no new mails found")
        for emailid in msgs:
            logging.info("fetch email id:"+str(emailid))


            resp, data = mail.fetch(emailid, "(RFC822)")
            email_body = data[0][1]
            email_body=email_body.decode('utf-8')
            m = email.message_from_string(email_body)
            #global email_date = m['date']
        

            if m.get_content_maintype() != 'multipart':
                continue
                logging.info("ignored, not attachments")

            for part in m.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                    logging.info("ignored, not attachments")



                filename=part.get_filename()
                if filename is not None:
                    sv_path = os.path.join(file_loc+ filename.replace('?','_'))
                    if not os.path.isfile(sv_path) or True:

                        fp = open(sv_path, 'wb')
                        fp.write(part.get_payload(decode=True))
                        logging.info ("file saved:"+sv_path)
                        fp.close()

    except Exception as err:
        logging.error("error: "+str(err))
        if('emailid' in locals() and mail != None):
            mail.store(emailid, '-FLAGS', '(\Seen)')
            logging.info("setting email as unseen again")
                 
def save_workbook_excel_file(workbook,file_loc: str , filename: str):
    """Tries to save created data to excel file"""
    try:
        workbook.save(os.path.join(file_loc,filename))
    except PermissionError:
        print("Error: No permission to save file.")
   
                        
def main():
   
    logging.info("start")
    _st_=datetime.datetime.now()
    script_name = os.path.basename(__file__)
    process_id=0
    datasource_id=0

    get_etl_detail = DA._get_process_id(script_name)
    if len(get_etl_detail) != 0:
        process_id = get_etl_detail[0][0]
        datasource_id = get_etl_detail[0][1]
    else:
        logging.error("Could not get script process ID. Please check config table public.etl_script")
        return()
    
    
    try:
        extra_filter = datetime.datetime.strftime(datetime.datetime.now() - timedelta(6), 'SINCE "%d-%b-%Y"')
        logging.info("fetch configs")
        for c in LOCAL_CONFIG['fetch_config']:
            logging.info("z="+str(LOCAL_CONFIG['fetch_config'][c]))
            imap_server= LOCAL_CONFIG['fetch_config'][c]['imap_server']
            imap_port= LOCAL_CONFIG['fetch_config'][c]['imap_port']
            username= LOCAL_CONFIG['fetch_config'][c]['username']
            password=LOCAL_CONFIG['fetch_config'][c]['password']
            subject=LOCAL_CONFIG['fetch_config'][c]['subject']
            sender=LOCAL_CONFIG['fetch_config'][c]['sender']
            file_loc=LOCAL_CONFIG['fetch_config'][c]['file_loc']
            csv_loc=LOCAL_CONFIG['fetch_config'][c]['local_csv_loc']
            s3_folder=LOCAL_CONFIG['fetch_config'][c]['s3_folder']
            s3_configs=LOCAL_CONFIG['fetch_config'][c]['s3_configs']            
            archive_loc=LOCAL_CONFIG['fetch_config'][c]['archive_loc']
            prefix_with_time=LOCAL_CONFIG['fetch_config'][c]['prefix_with_time']
            seen=False
            
            if not os.path.exists(csv_loc):
                logging.info("Create Dir If not exisiting already")
                os.makedirs(csv_loc)
            else:
                logging.info('clean csv folder *' + csv_loc)
                r=DA._empty_dir(csv_loc,"*")
                if(r==0):
                    DA._log_etl(script_name, "", 0, 0, "Error cleaning local csv folder")
                    logging.error('Error cleaning local csv folder')
                    return            
            if not os.path.exists(file_loc):
                logging.info("Create Dir If not exisiting already")
                os.makedirs(csv_loc)
            else:
                logging.info('clean download folder *' + file_loc)
                r=DA._empty_dir(file_loc,"*")
                if(r==0):
                    DA._log_etl(script_name, "", 0, 0, "Error cleaning local csv folder")
                    logging.error('Error cleaning local download folder')
                    return
            
            logging.info('download xls files from mail')
            logging.info("fetch emails")
            readAttachment(imap_server,imap_port,username,password,subject,sender,file_loc,seen,extra_filter,prefix_with_time)
            selRange = os.listdir(file_loc)
            for item in selRange:
                if item.endswith(".png") or item.endswith(".jpg"):
                    os.remove(os.path.join(file_loc, item))
            logging.info("done fetch emails for the current Week")
            
            logging.info("fetch Customer Marketing Mixpanel campaign configuration EXCEL file")
            files_list=DA._findin_dir(LOCAL_CONFIG['fetch_config'][c]['file_loc'],LOCAL_CONFIG['fetch_config'][c]['source_report'])
            file=str(files_list)
            _fileName = file_loc+file.replace('[','').replace(']','').replace('\'','')
            logging.info("===============processing file Name : "+_fileName+"===============")
            os.rename(_fileName,os.path.join(file_loc,'Intigral_Analytics_Segmented_PN_cohorts_configuration.xlsx')) 
    
            logging.info("file  "+file)
            logging.info("empty s3 folder")
            flag=DA._empty_s3(s3_folder,s3_configs)
            if (flag != 1):
                DA._log_etl(script_name, "", 0, 0, "Error while emptying S3")
                logging.info("done empty s3 folder")
                return
                    
            logging.info("upload to s3")
            flag = DA._upload_s3_xlsx(file_loc, s3_folder,s3_configs);
            if (flag != 1):
                DA._log_etl(script_name, "", 0, 0, "Error while uploading to S3")
                logging.error('Error upload to s3')
                return
            logging.info("done upload to s3")              
            
    except Exception as err:
        logging.error("error: "+str(err))


    _et_ = datetime.datetime.now()
    _time_ = (_et_-_st_).total_seconds()
    logging.info("time in seconds = {0}".format(str(_time_)))

    logging.info("end")

if __name__ == '__main__':
    main()
