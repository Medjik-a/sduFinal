from airflow.models import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.connection import Connection
from clickhouse_driver import Client, connect
import json
"""
Данный dag формирует витрину по проценту заполнения форм из ИС ЕГСУ
"""
def get_connection_or_cursor(conn_id,type,database):
  data=Connection.get_connection_from_secrets(conn_id=conn_id)
  if(type=="conn"):
      if(database=="clickhouse"):
        return connect(host=data.host,port=data.port,user=data.login, password = data.password, connect_timeout = 3600)
      if(database=="postgres"):
        return psycopg2.connect(dbname=data.schema, user=data.login,password=data.password, host=data.host, port=data.port)
  if(type=="cli" and database=="clickhouse"):
    return Client(host=data.host,port=data.port,user=data.login, password = data.password, connect_timeout = 3600)
def get_form_data(table_name,form_name,form_id):
  """
  Данная функция использует шаблон для заливки данных в витрину
  """
  path="/opt/airflow/dags/me_egsu/Sql-scripts/"
  with open(path+"monitoring.txt") as file1:
    sql_select=file1.read().format(table_name=table_name,form_id=form_id,form_name=form_name)
  cursor_17.execute(sql_select)
  while(True):
    res=cursor_17.fetchmany(50000)
    if(len(res)==0):
      break
    conn_6.execute("INSERT INTO ME.MONITORING_EGSU VALUES",res)
    conn_17.execute("INSERT INTO ME.MONITORING_EGSU VALUES",res)  
def truncate_table():
  conn_6.execute("TRUNCATE TABLE ME.MONITORING_EGSU")
  conn_17.execute("TRUNCATE TABLE ME.MONITORING_EGSU")
with DAG(dag_id="egsu_monitoring",start_date=datetime(2022,10,19),catchup=False,schedule_interval=None,tags=["ME","EGSU","MONITORING"]) as dag:
  path="/opt/airflow/dags/me_egsu/Sql-scripts/"
  with open(path+"forms.json") as file1:
    monitoring_forms=json.load(file1)
  cursor_17 = get_connection_or_cursor("Clickhouse-17", "conn", "clickhouse").cursor()
  conn_6 = get_connection_or_cursor("Clickhouse_52.6", "cli", "clickhouse")
  conn_17 = get_connection_or_cursor("Clickhouse-17", "cli", "clickhouse")
  first = PythonOperator(task_id=f"truncate_table", python_callable=truncate_table)
  with TaskGroup(group_id="for_monitoring"):
    for form in monitoring_forms:
      get_form = PythonOperator(task_id=f'get_data_from_form_{form["form_id"]}', python_callable=get_form_data,
                                op_kwargs={"table_name": form["table_name"], "form_id": form["form_id"],
                                           "form_name": form["form_name"]})
      first>>get_form
  