import urllib
import global_config
import logging
import requests
import json
import psycopg2
import csv
import boto3
import os
import gzip
import shutil
import math
from xml.dom import minidom
import mysql.connector
from datetime import datetime
from requests.utils import quote
import time
import pymssql
import sys
import sshtunnel
import pymysql

CONFIG=global_config.CONFIG

class DA:
    @staticmethod
    def _init_log(task_id):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s "%(asctime)s" "'+task_id+'" "%(message)s"')


        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)

        handler.setFormatter(formatter)
        logger.addHandler(handler)

        handler2 = logging.FileHandler(CONFIG['general']['log_loc']+datetime.now().strftime('%Y-%m')+'-task-'+task_id+'-log.out')
        handler2.setLevel(logging.INFO)
        handler2.setFormatter(formatter)
        logger.addHandler(handler2)
        return logging


    @staticmethod
    def _mp_jql_call(script, configs):


        max_attempt=3


        for attempt in range(max_attempt):
            res_list=[]
            logging.info("mp call attempt # "+str(attempt+1)+" of "+str(max_attempt))

            url = "https://mixpanel.com/api/2.0/jql?script="+quote(script, safe='')

            logging.info(url)

            try:
                response=requests.get(url,auth=(CONFIG['mp'][configs]['Secret'], ''))
            except (Exception) as error :
                if (attempt+1 == max_attempt):
                    logging.error("call https://mixpanel.com/api/2.0/jql:  "+str(error))
                    return {}
                continue

            if not response.status_code == 200:
                if (attempt+1 == max_attempt):
                    logging.error("call https://mixpanel.com/api/2.0/jql "+ str(response.status_code)+" "+response.text)
                    return {}
                continue

            try:
                logging.info("parsing response")
                res_list=response.json()
                #res_list=json.loads(response.text)

            except (Exception) as error :
                if (attempt+1 == max_attempt):
                    logging.error("parsing response:  "+str(error))
                    return {}
                continue

            return(res_list)
        # for in range

        return []



    @staticmethod
    def _find_in_array(array,search_value,search_key,return_value):

        for x in array:

            if x[search_key] == search_value:
               return (x[return_value])
               break

        return -1

    @staticmethod
    def _execute_rs(query):
        try:
            logging.info("open RedShift connection")

            c=psycopg2.connect(dbname=CONFIG['rs']['dbname'], host=CONFIG['rs']['host'], port=CONFIG['rs']['port'], user=CONFIG['rs']['user'], password=CONFIG['rs']['password'])

            cursor = c.cursor()

            logging.info("execute command")
            cursor.execute(query)
            c.commit()
            c.close()
            # logging.info("commit")
            logging.info("affected rows = {0}".format(str(cursor.rowcount)))

        except (Exception, psycopg2.DatabaseError) as error :
            logging.error( "RedShift {0}".format(str(error)))

            try:
                c.rollback()
                c.close()
            except:
                pass

            return 0

        return 1

    @staticmethod
    def _read_mysql(query,conn_configs_key):
        res_list=[]
        try:
            logging.info("query={0}".format(query))
            logging.info("connect to MySQL")
            db=mysql.connector.connect(host=CONFIG[conn_configs_key]['host'], user=CONFIG[conn_configs_key]['user'],password=CONFIG[conn_configs_key]['password'],db=CONFIG[conn_configs_key]['dbname'], port=CONFIG[conn_configs_key]['port'])
            cursor= db.cursor()
            mysql_sql=query

            cursor.execute(mysql_sql)
            logging.info("fetch data from MySQL")
            res_list=cursor.fetchall()
            cursor.close()
            db.close()
            # logging.info("close MySQL connection")
        except mysql.connector.Error as e:
            logging.error("MySQL Error [%d]: %s" % (e.errno, e.msg))
            try:
                db.close()
            except:
                    return res_list

            return res_list

        return res_list

    @staticmethod
    def _write_to_csv(res_list, loc,seg_len,target_name):
    # sample call _write_to_csv(res_list,'c:/mpx_home_users/csv/',5000,'part')
        try:
            if not os.path.exists(loc):
                os.makedirs(loc)

            logging.info("clean dir=%s"%loc)

            for fl in os.listdir(loc):
                  if fl.endswith(".csv"):
                    logging.info("remove file:%s"%fl)
                    os.remove(loc+fl)
                    # logging.info("done remove file:%s"%fl)


            # logging.info("calculate number for segments")
            num_rows=len(res_list)
            segments=int(math.ceil(1.0*num_rows/seg_len))
            logging.info("number of segment files =%s , batch size=%s"%(segments,seg_len))
            #csv.register_dialect('myDialect',lineterminator = '\n')

            from_row=0
            to_row=seg_len
            for i in range(segments):
                logging.info("generate seg No {0} - from: {1}, to: {2}".format(i,from_row,to_row) )
                file_name=loc+target_name+"-"+str(i)+'.csv'
                part_list=res_list[from_row:to_row]
                with open(file_name, 'w', encoding='utf-8', newline='') as writeFile:
                    #writer = csv.writer(writeFile, dialect='myDialect')
                    writer = csv.writer(writeFile)
                    writer.writerows(part_list)
                    logging.info("create file "+file_name)
                    writeFile.close()



                from_row=to_row
                to_row=to_row+seg_len
                if to_row>=num_rows:
                    to_row=num_rows

            return 1
        except (Exception) as error:
            logging.error(format(str(error)))
            return 0


    @staticmethod
    def _upload_s3(loc_local, s3_folder,s3_configs):

        try:
            logging.info("connect to S3")
            s3 = boto3.client(  's3',aws_access_key_id=CONFIG[s3_configs]['aws_access_key_id'],aws_secret_access_key=CONFIG[s3_configs]['aws_secret_access_key'])
            # logging.info("connected to S3")

            for fl in os.listdir(loc_local):
                      if os.path.isfile(loc_local+fl):
                        logging.info("upload file:%s"%fl)
                        fnts=s3_folder+'/'+fl+"-"+str(int(time.time()))+".csv"
                        s3.upload_file(loc_local+fl, CONFIG[s3_configs]['bucket_name'],fnts )
                        # logging.info("done upload file:{0} to s3 {1}".format(str(fnts),str(s3_folder)))

            time.sleep(2)
            # logging.info("wait 2 seconds")
            return 1
        except (Exception) as error:
            logging.error(format(str(error)))
            return 0


    @staticmethod
    def _empty_s3(s3_folder,s3_configs):

        try:
            logging.info("connect to S3")
            s3res= boto3.resource('s3',aws_access_key_id=CONFIG[s3_configs]['aws_access_key_id'],aws_secret_access_key=CONFIG[s3_configs]['aws_secret_access_key'])
            bucket=s3res.Bucket(CONFIG[s3_configs]['bucket_name'])
            # logging.info("done connect to S3")

            if (s3_folder!=''):
                s3_folder=s3_folder+'/'

            for obj in bucket.objects.filter(Prefix=''+s3_folder):
                if (obj.key != s3_folder and  ( not  str(obj.key).endswith('/'))):
                        logging.info('delete file: '+obj.key)
                        obj.delete()
                        # logging.info('done delete file: '+obj.key)



            return 1
        except (Exception) as error:
            logging.error(format(str(error)))
            return 0

    @staticmethod
    def _googleAds_xml_file_to_list(xml):
        mydoc = minidom.parse(xml)
        xml_data=[]

        cols = len(mydoc.getElementsByTagName('ColumnHeader'))
        rows = mydoc.getElementsByTagName('Row')

        row_count=len(rows)
        logging.info("Processed number of rows: %s"%row_count)

        values = mydoc.getElementsByTagName('Val')

        from_row=0
        to_row=cols

        try:
            for row in range(row_count):
                row_data=[]
                for _value in values[from_row:to_row]:
                    row_data.append(_value.firstChild.nodeValue)
                from_row=to_row
                to_row=to_row+cols
                if (row_data):
                    xml_data.append(row_data)
            return xml_data
        except:
            pass
            logging.error( "Unable to parse XML {0}".format(str(error)))
        return xml_data


    @staticmethod
    def _execute_many_rs(query,par):
        try:
            logging.info("open RedShift connection")

            c=psycopg2.connect(dbname=CONFIG['rs']['dbname'], host=CONFIG['rs']['host'], port=CONFIG['rs']['port'], user=CONFIG['rs']['user'], password=CONFIG['rs']['password'])

            cursor = c.cursor()

            logging.info("execute command")
            cursor.execute(query,par)
            c.commit()
            c.close()
            logging.info("commit")
            logging.info("affected rows = {0}".format(str(cursor.rowcount)))
            return 1
        except (Exception, psycopg2.DatabaseError) as error :
            logging.error( "RedShift {0}".format(str(error)))

        try:
            c.rollback()
            c.close()
        except:
                pass
        return 0

    @staticmethod
    def _execute_copy_rs(table,columns,s3_folder,prefix,option ="csv delimiter ',' null as '\\0' "):
        rows_inserted=0;

        try:


            logging.info("open RedShift connection")
            if(s3_folder!=''):
                s3_folder=s3_folder+'/'

            c=psycopg2.connect(dbname=CONFIG['rs']['dbname'], host=CONFIG['rs']['host'], port=CONFIG['rs']['port'], user=CONFIG['rs']['user'], password=CONFIG['rs']['password'])

            cursor = c.cursor()
            cursor.execute("select count(*) rs from "+table)
            rows_before=cursor.fetchone()
            rows_before=int(rows_before[0])
            copy_cmd="copy "+table+columns+"from 's3://"+CONFIG['s3_configs']['bucket_name']+"/"+s3_folder+prefix+"' credentials  'aws_access_key_id="+CONFIG['s3_configs']['aws_access_key_id']+";aws_secret_access_key="+CONFIG['s3_configs']['aws_secret_access_key']+"' "+option
            logging.info(copy_cmd)

            logging.info("execute command")
            cursor.execute(copy_cmd)
            c.commit()
            cursor.execute("select count(*) rs from "+table)
            rows_after=cursor.fetchone()
            rows_after=int(rows_after[0])
            rows_inserted=rows_after-rows_before
            c.close()
            logging.info("commit")
            logging.info("rows added="+str(rows_inserted))


        except (Exception, psycopg2.DatabaseError) as error :
            logging.error( "RedShift {0}".format(str(error)))

            try:
                c.rollback()
                c.close()
            except:
                pass

            return 0

        return rows_inserted

    @staticmethod
    def _read_rs(query):
        res_list=[]

        try:
            logging.info("open RedShift connection")

            c=psycopg2.connect(dbname=CONFIG['rs']['dbname'], host=CONFIG['rs']['host'], port=CONFIG['rs']['port'], user=CONFIG['rs']['user'], password=CONFIG['rs']['password'])

            cursor = c.cursor()
            logging.info("execute command")
            cursor.execute(query)
            res_list=cursor.fetchall()



        except (Exception, psycopg2.DatabaseError) as error :
            logging.error( "RedShift {0}".format(str(error)))

            try:
                c.rollback()
                c.close()
            except:
                pass

            return res_list

        return res_list


    @staticmethod
    def _write_json_to_csv(res_list, loc,seg_len,target_name, fname):
        try:

                logging.info("clean dir=%s"%loc)

                for fl in os.listdir(loc):
                      if fl.endswith(".csv"):
                        logging.info("remove file:%s"%fl)
                        os.remove(loc+fl)
                        logging.info("done remove file:%s"%fl)


                logging.info("calculate number for segments")
                num_rows=len(res_list)
                segments=int(math.ceil(1.0*num_rows/seg_len))
                logging.info("number of segment files =%s , batch size=%s"%(segments,seg_len))
                csv.register_dialect('myDialect',lineterminator = '\n')

                from_row=0
                to_row=seg_len
                for i in range(segments):
                    logging.info("generate seg No {0} - from: {1}, to: {2}".format(i,from_row,to_row) )
                    file_name=loc+target_name+"-"+str(i)+'.csv'
                    part_list=res_list[from_row:to_row]

                    with open(file_name, 'w', encoding='utf-8-sig', newline='') as writeFile:
                        writer = csv.DictWriter(writeFile, delimiter=",", quoting=csv.QUOTE_ALL,
                                  fieldnames=fname)
                        writer.writeheader()

                        for kv in part_list:
                            x = {k: v for k, v in kv.items() }

                            writer.writerow(x)
                        logging.info("create file "+file_name)
                        writeFile.close()

                    from_row=to_row
                    to_row=to_row+seg_len
                    if to_row>=num_rows:
                        to_row=num_rows

                return 1
        except (Exception) as error:
            logging.error(format(str(error)))
            return 0

    @staticmethod
    def _read_mssql(query,conn_configs_key):
        res_list=[]
        try:
            logging.info("query={0}".format(query))
            logging.info("connect to MSSQL")
            conn=pymssql.connect(host=CONFIG[conn_configs_key]['host'], user=CONFIG[conn_configs_key]['user'],password=CONFIG[conn_configs_key]['password'],database=CONFIG[conn_configs_key]['dbname'], port=CONFIG[conn_configs_key]['port'])
            cursor= conn.cursor()
            mssql_sql=query

            cursor.execute(mssql_sql)
            logging.info("fetch data from MSSQL")
            res_list=cursor.fetchall()
            cursor.close()
            conn.close()
            # logging.info("close MSSQL connection")
        except pymssql.Error as e:
            logging.error("MSSQL Error: %s" % str(e))
            try:
                conn.close()
            except:
                    return res_list

            return res_list

        return res_list
    @staticmethod
    def _log_etl(script, tbl_name, s_cnt=None, d_cnt=None, err_msg=None):

        result=1
        if (err_msg):
            result=0
        if s_cnt != d_cnt:
            result=0

        cmd = "insert into etl_logging(script, tbl_name, ts, s_cnt, d_cnt, result, err_msg)"\
                     "values (%s,%s,%s,%s,%s,%s,%s)"
        args = (script, tbl_name, 'now', s_cnt, d_cnt, result, err_msg)

        logging.info("Logging to etl_logging table")

        flag = DA._execute_many_rs(cmd,args)
        if (flag != 1):
            return 0
        return 1

    @staticmethod
    def _execute_rs_cnt(query):
        try:
            logging.info("open RedShift connection")

            c=psycopg2.connect(dbname=CONFIG['rs']['dbname'], host=CONFIG['rs']['host'], port=CONFIG['rs']['port'], user=CONFIG['rs']['user'], password=CONFIG['rs']['password'])

            cursor = c.cursor()

            logging.info("execute command")
            cursor.execute(query)
            c.commit()
            c.close()
            logging.info("commit")
            logging.info("affected rows = {0}".format(str(cursor.rowcount)))

        except (Exception, psycopg2.DatabaseError) as error :
            logging.error( "RedShift {0}".format(str(error)))

            try:
                c.rollback()
                c.close()
            except:
                pass

            return 0,0

        return 1,cursor.rowcount

    @staticmethod
    def _execute_many_rs_cnt(query,par=None):
        try:
            logging.info("open RedShift connection")

            c=psycopg2.connect(dbname=CONFIG['rs']['dbname'], host=CONFIG['rs']['host'], port=CONFIG['rs']['port'], user=CONFIG['rs']['user'], password=CONFIG['rs']['password'])

            cursor = c.cursor()

            logging.info("execute command")
            cursor.execute(query,par)
            c.commit()
            c.close()
            logging.info("commit")
            logging.info("affected rows = {0}".format(str(cursor.rowcount)))
            return 1,cursor.rowcount
        except (Exception, psycopg2.DatabaseError) as error :
            logging.error( "RedShift {0}".format(str(error)))

        try:
            c.rollback()
            c.close()
        except:
                pass
        return 0,0

    @staticmethod
    def _get_process_id(script):
        try:
            cmd="select script_id, datasource_id from etl_script where script_name='"+script+"';"
            etl_detail=DA._read_rs(cmd)

            if (etl_detail):
                logging.info("For script %s, the process and datasource IDs are %s '"%(script.upper(),str(etl_detail)))
                return etl_detail
            else:
                return []
            '''else:
                try:
                    ins_cmd="insert into  etl_script(script_name) values ('"+script+"')"
                    logging.info("The process does not exist. Adding it to table etl_script")
                    DA._execute_rs_cnt(ins_cmd)
                except (Exception) as err:
                    logging.error("Could not insert: " + str(err))
                    return -1
            return DA._get_process_id(script)'''
        except (Exception) as error:
            logging.error("Error getting proces ID:"+str(error))
            return []

    @staticmethod
    def _download_files_s3(folder,s3_configs,s3_path):

        try:
            if not os.path.exists(folder):
                os.makedirs(folder)

            logging.info("connect to S3")
            s3res= boto3.resource('s3',aws_access_key_id=CONFIG[s3_configs]['aws_access_key_id'],aws_secret_access_key=CONFIG[s3_configs]['aws_secret_access_key'])
            bucket=s3res.Bucket(CONFIG[s3_configs]['bucket_name'])


            for obj in bucket.objects.filter(Prefix=''+s3_path):
                if (( not  str(obj.key).endswith('/'))):
                        logging.info('download file: '+obj.key)
                        bucket.download_file(obj.key, folder+'/'+obj.key)





            return 1
        except (Exception) as error:
            logging.error(format(str(error)))
            return 0



    @staticmethod
    def _del_file_s3(s3_file,s3_configs):

        try:
            logging.info("connect to S3")
            s3res= boto3.resource('s3',aws_access_key_id=CONFIG[s3_configs]['aws_access_key_id'],aws_secret_access_key=CONFIG[s3_configs]['aws_secret_access_key'])
            bucket=s3res.Bucket(CONFIG[s3_configs]['bucket_name'])



            for obj in bucket.objects.filter(Prefix=''+s3_file):
                if (str(obj.key) == s3_file):
                        logging.info('delete file: '+obj.key)
                        obj.delete()




            return 1
        except (Exception) as error:
            logging.error(format(str(error)))
            return 0


    @staticmethod
    def _list_file_s3(s3_file,s3_configs):

        res=[]
        try:
            logging.info("connect to S3")
            s3res= boto3.resource('s3',aws_access_key_id=CONFIG[s3_configs]['aws_access_key_id'],aws_secret_access_key=CONFIG[s3_configs]['aws_secret_access_key'])
            bucket=s3res.Bucket(CONFIG[s3_configs]['bucket_name'])



            for obj in bucket.objects.filter(Prefix=''+s3_file):
                    res.append(obj.key)

        except (Exception) as error:
            logging.error(format(str(error)))
            return 0

        return res


    @staticmethod
    def _empty_dir(folder,ext='*'):
        try:

            for fl in os.listdir(folder):
                if(fl.endswith(ext) or ext=="*"):
                    logging.info("remove file:%s"%fl)
                    os.remove(folder+fl)

        except (Exception) as error:
            logging.error(format(str(error)))
            return 0
        return 1

    @staticmethod
    def _archive_dir(src,dest, compress =0):
        try:
            if not os.path.exists(dest):
                os.makedirs(dest)

            for fl in os.listdir(src):

                    logging.info("archive file:%s"%fl)
                    fl_2=dest+str(int(time.time()))+"-"+fl
                    os.rename(src+fl,fl_2)

                    if(compress==1):

                        logging.info("compress file="+fl_2)
                        with open(fl_2, 'rb') as f_in, gzip.open(fl_2+'.gz', 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                        os.remove(fl_2);



        except (Exception) as error:
            logging.error(format(str(error)))
            return 0
        return 1

    @staticmethod
    def _findin_dir(folder,key):
        try:
            res=[]

            for fl in os.listdir(folder):
                  if (fl.find(key) != -1):
                    res.append(fl)

        except (Exception) as error:
            logging.error(format(str(error)))
        return res




    @staticmethod
    def _write_to_csv_no_del(res_list, loc,seg_len,target_name):
    # sample call _write_to_csv(res_list,'c:/mpx_home_users/csv/',5000,'part')
        try:
            if not os.path.exists(loc):
                os.makedirs(loc)

            num_rows=len(res_list)
            segments=int(math.ceil(1.0*num_rows/seg_len))
            logging.info("number of segment files =%s , batch size=%s"%(segments,seg_len))
            #csv.register_dialect('myDialect',lineterminator = '\n')

            from_row=0
            to_row=seg_len
            for i in range(segments):
                logging.info("generate seg No {0} - from: {1}, to: {2}".format(i,from_row,to_row) )
                file_name=loc+target_name+"-"+str(i)+'.csv'
                part_list=res_list[from_row:to_row]
                with open(file_name, 'w', encoding='utf-8', newline='') as writeFile:
                    #writer = csv.writer(writeFile, dialect='myDialect')
                    writer = csv.writer(writeFile)
                    writer.writerows(part_list)
                    logging.info("create file "+file_name)
                    writeFile.close()



                from_row=to_row
                to_row=to_row+seg_len
                if to_row>=num_rows:
                    to_row=num_rows

            return 1
        except (Exception) as error:
            logging.error(format(str(error)))
            return 0


    @staticmethod
    def _read_mysql_ssh(query,conn_configs_key):
        res_list=[]
        try:

            logging.info("open ssh connection")


            server= sshtunnel.SSHTunnelForwarder(
                (CONFIG[conn_configs_key]['ssh_host'],CONFIG[conn_configs_key]['ssh_port']),
                ssh_password=CONFIG[conn_configs_key]['ssh_password'],
                ssh_username=CONFIG[conn_configs_key]['ssh_username'],
                remote_bind_address=(CONFIG[conn_configs_key]['host'],CONFIG[conn_configs_key]['port'])
                )

            server.start()

            logging.info("ssh Connected, port="+str(server.local_bind_port))


            logging.info("query={0}".format(query))
            logging.info("connect to MySQL")
            db=pymysql.connect(host='127.0.0.1', user=CONFIG[conn_configs_key]['user'],password=CONFIG[conn_configs_key]['password'],db=CONFIG[conn_configs_key]['dbname'], port=server.local_bind_port,read_timeout =3)
            cursor= db.cursor()
            mysql_sql=query

            cursor.execute(mysql_sql)

            logging.info("fetch data from MySQL")
            res_list=cursor.fetchall()
            cursor.close()
            db.close()
            server.stop()

                # logging.info("close MySQL connection")
        except (Exception) as e:
            logging.error("ERROR:"+str(e))
            try:
                db.close()
            except:
                    return res_list

            return res_list

        return res_list
