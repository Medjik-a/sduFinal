from airflow.models import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from clickhouse_driver import Client, connect
import psycopg2
import time
import pandas as pd
def sel_and_insert_data(conn_source,conn_dest,sql_sel,sql_insert,start_date,end_date):
  try:
    conn_source.execute(sql_sel+" WHERE modified_date>='{}' AND modified_date<'{}'".format(start_date,end_date))
    while(True):
      res=conn_source.fetchmany(10000)
      if(len(res)==0):
        break
      conn_dest.execute(sql_insert,res)
    return "Ok"
  except:
      return "Exception"
def get_history(schema_source,table_source,conn_source,schema_dest,table_dest,conn_dest):
  conn_dest.execute("TRUNCATE TABLE {}.{}".format(schema_dest,table_dest))
  path="/opt/airflow/dags/eobr/Sql-scripts/"
  with open(path+"Select/"+table_source+".sql") as file1,open(path+"Insert/"+table_dest+".sql") as file2:
    sql1=file1.read()
    sql2=file2.read()
  cursor=conn_source.cursor()
  cursor.execute("SELECT MAX(modified_date) FROM {}.{}".format(schema_source,table_source))
  end_date=cursor.fetchone()[0]
  start_date=datetime(year=end_date.year,month=end_date.month,day=end_date.day,hour=0,minute=0)
  sel_and_insert_data(conn_source=cursor,conn_dest=conn_dest,sql_sel=sql1,sql_insert=sql2,start_date=start_date,end_date=end_date)
  end_date,start_date=start_date,datetime(2021,7,1,0,0,0)
  while start_date<end_date:
      res=sel_and_insert_data(conn_source=cursor,conn_dest=conn_dest,sql_sel=sql1,sql_insert=sql2,start_date=end_date-timedelta(days=1),end_date=end_date)
      if(res=="Ok"):
        end_date=end_date-timedelta(days=1)
      else:
        conn_dest.execute("ALTER TABLE {}.{} DELETE WHERE MODIFIED_DATE>='{}' AND MODIFIED_DATE<'{}'".format(schema_dest,table_dest,modified_date,end_date))
        print("Exception")
        cursor=psycopg2.connect(dbname=post_conn.schema, user=post_conn.login,password=post_conn.password, host=post_conn.host, port=post_conn.port).cursor()
        time.sleep(30)
with DAG(dag_id="eobr_history",start_date=datetime(2022,8,21),catchup=False,tags=["eotinish"],schedule_interval=None) as dag:
  cli_SBD=Client(host="192.168.52.5",port="9000",user="ch_etl", password = "i2oI69T81Y3LcORwygEs", connect_timeout = 3600)
  main_tables=["appeals","av_appeals_applications","av_applicants","appeals_decision"]
  post_conn=Connection.get_connection_from_secrets(conn_id="eotinish")
  postconn=psycopg2.connect(dbname=post_conn.schema, user=post_conn.login,password=post_conn.password, host=post_conn.host, port=post_conn.port)
  end_of_extract=DummyOperator(task_id="end_of_extract")
  with TaskGroup("extract",tooltip="history_data"):
    for table in main_tables:
      get_history_data=PythonOperator(task_id="get_history_data_from_{}".format(table),python_callable=get_history,op_kwargs={"schema_dest":"BTSD_EOBRASHENIYA","table_dest":table.upper(),"conn_dest":cli_SBD,"schema_source":"public","table_source":table,"conn_source":postconn})
      get_history_data>>end_of_extract
     
