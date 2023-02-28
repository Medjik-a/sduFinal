from airflow.models import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from sqlalchemy import create_engine
from airflow.models.connection import Connection
import paramiko
import re
import pandas as pd

tables = ["ME_D_Indicator","ME_D_Period","ME_Plan","ME_D_Type_Period","ME_D_Year","ME_D_Measure","ME_Fact"]
dictionaries = ["ME_D_Indicator","ME_D_Period","ME_D_Type_Period","ME_D_Year","ME_D_Measure"]
remote_path = "/data/filebrowser/data/ME/data/"
local_path = "/opt/airflow/dags/me_pser/"

def list_files(conn_id,path,folder):
  """
  Данная функция используется для проверки файлов на другом сервере (в данном случае так как airflow на 192.168.52.3 мы проверяем имеющиеся файлы на 192.168.52.17,
  папки куда смотрит filebrowser
  """
  conn_secrets=Connection.get_connection_from_secrets(conn_id=conn_id)
  ssh=paramiko.SSHClient()
  ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
  ssh.connect(conn_secrets.host,username=conn_secrets.login,password=conn_secrets.password)
  ftp=ssh.open_sftp()
  return ftp.listdir(f"{remote_path}{folder}/")
def get_engine_from_connection(driver,conn_id,database):
  conn_secrets=Connection.get_connection_from_secrets(conn_id=conn_id)
  return create_engine(f"{driver}://{conn_secrets.login}:{conn_secrets.password}@{conn_secrets.host}/{database}")
def read_excel(filename,folder,engine):
  """
  Загрузка данных с excel в базу данных
  """
  table=re.search('(.*)\.xlsx$',filename).group(1)
  if (table in tables):
    df=pd.read_excel(f"/opt/airflow/dags/me_pser/{folder}/{filename}",engine="openpyxl")
    needed_columns=[col for col in df.columns if(not re.search('Unnamed.*',col))]
    df=df[needed_columns]
    df.dropna(inplace=True)
    engine.execute(f"TRUNCATE TABLE {table}")
    table=table+"_tmp" if table not in dictionaries else table
    df.to_sql(table,con=engine,if_exists="append",index=False)
    if(table not in dictionaries):
      with open(f"/opt/airflow/dags/me_pser/Sql-scripts/{table}.sql") as file:
        script=file.read()
        engine.execute(script)
def form_pser_vitrina(engine):
  """
  Формирование витрины по ПСЭР МЭ
  """
  engine.execute("TRUNCATE TABLE SDM_AUTO_REPORTS.ME_PSER_VITRINA")
  with open(f"/opt/airflow/dags/me_pser/Sql-scripts/ME_PSER_VITRINA.sql") as file:
        script=file.read()
        engine.execute(script)
with DAG(dag_id="me_pser_sftp",start_date=datetime(2022,8,11),catchup=False,tags=["me","pser","sftp"],schedule_interval=None) as dag:
  folder=datetime.now().strftime("%Y-%m")
#  folder="2022-05"
  files=list_files("filebrowser_ssh",remote_path,folder)
  nothing=DummyOperator(task_id="end_of_migrating")
  engine=get_engine_from_connection("clickhouse+native","Clickhouse-17","SDM_AUTO_REPORTS")
  fdataset=PythonOperator(task_id="form_dataset",python_callable=form_pser_vitrina,op_kwargs={"engine":engine})
  for file in files:
    get_file=SFTPOperator(task_id=f"sftp_{file}",ssh_conn_id="filebrowser_ssh",local_filepath=f"{local_path}{folder}/{file}",remote_filepath=f"{remote_path}{folder}/{file}",operation="get",create_intermediate_dirs=True)
    get_file>>nothing
  for file in files:
    read_files=PythonOperator(task_id=f"read_{file}",python_callable=read_excel,op_kwargs={"filename":file,"folder":folder,"engine":engine})
    nothing>>read_files
    read_files>>fdataset
    