import json
import os
from datetime import datetime, timedelta
import csv
import sys




sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/config")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__))+"/utils")

import global_config

from global_utils import DA
CONFIG = global_config.CONFIG
logging = DA._init_log(os.path.basename(__file__))


def main():

    _st_ = datetime.now()
    logging.info("start")


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


    res_list = {} # depend on data source



    logging.info("generate csv")
    flag= DA._write_to_csv(res_list,csv_folder,tt['batch_len'],LOCAL_CONFIG['prefix'])
    if (flag!=1):
        err_msg="Error while generating CSV files, table:"+tt['prod_table']
        logging.error(err_msg)
        DA._log_etl(script_name, tt['prod_table'], res_list_len, None, err_msg)
        return
    logging.info("done generate csv")

    logging.info("empty s3 folder")
    flag=DA._empty_s3(s3_folder,LOCAL_CONFIG['s3_configs'])
    if (flag!=1):
            err_msg="Error while empty s3 folder, table:"+tt['prod_table']
            logging.error(err_msg)
            DA._log_etl(script_name, tt['prod_table'], res_list_len, None, err_msg)
            break
    logging.info("done empty s3 folder")



    logging.info("upload to s3")
    flag=DA._upload_s3(csv_folder,s3_folder,LOCAL_CONFIG['s3_configs'])

    if (flag!=1):
        err_msg="Error while uploading S3, table:"+tt['prod_table']
        logging.error(err_msg)
        DA._log_etl(script_name, tt['prod_table'], res_list_len, None, err_msg)
        break
    logging.info("done upload to s3")



    logging.info("truncate temp table")
    cmd="truncate table "+tt['temp_table']
    logging.info(cmd)
    flag=DA._execute_rs(cmd)
    if (flag!=1):
        err_msg="Error while truncating temp table:"+tt['temp_table']
        logging.error(err_msg)
        DA._log_etl(script_name, tt['prod_table'], res_list_len, None, err_msg)
        return
    logging.info("done truncate temp table")


    logging.info("load data from s3 into temp table")
    rows_inserted=DA._execute_copy_rs(tt['temp_table'],"("+tt['rs_col']+")",s3_folder,LOCAL_CONFIG['prefix'])
    if (rows_inserted==0):
        err_msg="imported rows count =0 temp table:"+tt['temp_table']
        logging.error(err_msg)
        DA._log_etl(script_name, tt['prod_table'], res_list_len, None, err_msg)
        return
    logging.info("done load data from s3 into temp table")


    if res_list_len != rows_inserted:
        err_msg="rollback; source != dest rows for table:"+tt['temp_table']+" ["+str(res_list_len)+","+str(rows_inserted)+"]"
        logging.error(err_msg)
        DA._log_etl(script_name, tt['prod_table'], res_list_len, rows_inserted, err_msg)
        return


    logging.info("truncate prod table")
    cmd="truncate table "+tt['prod_table']
    logging.info(cmd)
    flag=DA._execute_rs(cmd)
    if (flag!=1):
        err_msg="Error while truncating prod table:"+tt['prod_table']
        logging.error(err_msg)
        DA._log_etl(script_name, tt['prod_table'], res_list_len, None, err_msg)
        continue
    logging.info("done truncate prod table")



    logging.info("copy data from temp to original table")
    cmd="insert into  "+tt['prod_table'] +" ("+tt['rs_col']+") select "+tt['rs_col']+" from "+tt['temp_table']
    logging.info(cmd)
    flag=DA._execute_rs_cnt(cmd)
    if (flag[0]!=1):
        err_msg="Error while copying data from temp to original table:"+tt['prod_table']
        logging.error(err_msg)
        DA._log_etl(script_name, tt['prod_table'], res_list_len, None, err_msg)
        continue

    DA._log_etl(script_name, tt['prod_table'], tt['source_count'], flag[1], None)
    logging.info("done copy data from temp to original table")










    ##Body of script
    ##Body of script
    ##Body of script



    _et_ = datetime.now()
    _time_ = (_et_-_st_).total_seconds()
    logging.info("time in seconds = {0}".format(str(_time_)))

    logging.info("end")

if __name__ == '__main__':
    main()


