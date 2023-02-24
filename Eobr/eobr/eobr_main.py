from airflow.models import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy import DummyOperator
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from clickhouse_driver import Client, connect
import psycopg2

def get_connection_or_cursor(conn_id,type,database):
  """
  Данная функция используется для получения подключения к необходимой базе (Postgresql-подключение к ИС "Е-обращения",Clickhouse-наши БД)
  :param conn_id: ID поключения из вкладки connection в airflow
  :param type: тип подсоединения для clickhouse
  :param database: необходимая база данных
  :return:
  """
  data=Connection.get_connection_from_secrets(conn_id=conn_id)
  if(type=="conn"):
      if(database=="clickhouse"):
        return connect(host=data.host,port=data.port,user=data.login, password = data.password, connect_timeout = 3600)
      if(database=="postgres"):
        return psycopg2.connect(dbname=data.schema, user=data.login,password=data.password, host=data.host, port=data.port)
  if(type=="cli" and database=="clickhouse"):
    return Client(host=data.host,port=data.port,user=data.login, password = data.password, connect_timeout = 3600)
def get_new_data(schema_source,table_source,schema_dest,table_dest,conn_source,conn_dest):
  """
  Данная функция используется для загрузки по дельте используя поле modified_date в четырех таблицах appeals,appeals_applications,applicants,appeals_decision
  :param schema_source: Схема источника
  :param table_source: Таблица с источника
  :param schema_dest: Схема куда надо грузить
  :param table_dest: Таблица в которую надо грузить
  :param conn_source: Подключение к источнику
  :param conn_dest: Подключение к цели
  """
  path="/opt/airflow/dags/eobr/Sql-scripts" #в данной папке находятся необходимые sql скрипты
  with open(path+f"/Select/{table_source}.sql") as file1,open(path+f"/Insert/{table_dest}.sql") as file2: #Для каждой из 4 таблиц есть два скрипта один для Select,второй для Insert
    sql_select=file1.read()
    sql_insert=file2.read()
  date=conn_dest.execute("SELECT MAX(MODIFIED_DATE) FROM {}.{}".format(schema_dest,table_dest))
  max_date=date[0][0]
  cursor=conn_source.cursor()
  cursor.execute(sql_select+f" WHERE modified_date>'{max_date}'")
  while(True):
    res=cursor.fetchmany(10000)
    if(len(res)==0):
      break
    conn_dest.execute(sql_insert,res)
def form_datasets(schema_dest,table_dest,conn_source,conn_dest):
  """
  До ЦКС данная функция была последней в формировании витрин для визуализации
  :param schema_dest: Схема куда надо грузить
  :param table_dest: Таблица в которую надо грузить
  :param conn_source: Подключение к источнику
  :param conn_dest: Подключение к цели
  """
  cli_17.execute(f"TRUNCATE TABLE {schema_dest}.{table_dest}_tmp")
  conn_6.execute(f"TRUNCATE TABLE {schema_dest}.{table_dest}_tmp")
  conn_6.execute(f"TRUNCATE TABLE {schema_dest}.{table_dest}")
  path="/opt/airflow/dags/eobr/Sql-scripts/Sections" # Здесь содержатся скрипты необходимые скрипты для формирования витрин до соединения с таблицами ЦКС (классы семей и прочее)
  with open(path+f"/Select/{table_dest}.sql") as file1,open(path+f"/Insert/{table_dest}.sql") as file2:
    sql_select=file1.read()
    sql_insert=file2.read()
  conn_source.execute(sql_select)
  while(True):
    res=conn_source.fetchmany(50000)
    if(len(res)==0):
      break
#    conn_dest.execute(sql_insert,res)
    conn_6.execute(sql_insert,res)
    cli_17.execute(sql_insert,res)
def form_new_datasets():
  """
  Здесь идет обьединение с таблицами ЦКС для формирования итогов витрин для визуализации.
  """
  new_tables=["applicants_sec_new","main_sec_new","segmentation_appeals"]
  path="/opt/airflow/dags/eobr/Sql-scripts/Sections"
  conn_6.execute(f"TRUNCATE TABLE MCRIAP_EOBR.segmentation_appeals")
  for table in new_tables:
    with open(path+f"/Insert/{table}.sql") as file1:
      sql_insert=file1.read()
      conn_6.execute(sql_insert)
def form_family_appeals():
  """
  Здесь идет формирование таблицы,которая использовалась на дашборде ЦКС
  """
  cli_17.execute("TRUNCATE TABLE SOC_KARTA.FAMILY_APPEALS")
  cli_17.execute("""INSERT INTO SOC_KARTA.FAMILY_APPEALS
SELECT
ms.iin_bin AS IIN,
sfqi.ID_SK_FAMILY_QUALITY as FAMILY_ID,
sa.FAMILY_CAT as FAMILY_CAT,
ms.start_dt AS REG_DATE,
ms.reg_number as APPEAL_ID,
ms.appeal_type  APPEAL_TYPE,
ms.org_type ORG_TYPE,
ms.org_name  ORG_NAME,
ms.issue ISSUE,
ms.subissue SUBISSUE,
ms.appeal_decision as DECISION
FROM MCRIAP_EOBR.main_sec_tmp ms
LEFT JOIN SOC_KARTA.SK_FAMILY_QUALITY_IIN sfqi ON sfqi.IIN=ms.iin_bin
LEFT JOIN SOC_KARTA.SEGMENTATION_ASSOGIN sa ON sa.ID_SK_FAMILY_QUALITY2=toString(sfqi.ID_SK_FAMILY_QUALITY) 
WHERE IIN<>''""")
def monitoring_checks():
  """
  Данная функция использовалась для мониторинга обновления данных в витринах Е-обращения.
  В связи с проблемами с кликхаусом,в данный момент отключена
  """
  new_tables=["applicants_sec","main_sec"]
  main_tables=["appeals","av_appeals_applications","av_applicants","appeals_decision"]
  cur_time=datetime.now()+timedelta(hours=12)
  for table in new_tables:
    cursor_6.execute(f"SELECT MAX(start_dt),MAX(SDU_LOAD_IN_DT) FROM MCRIAP_EOBR.{table}")
    res=cursor_6.fetchone()
    max_delta,last_update=res
    last_update=last_update+timedelta(hours=6)
    cli_17.execute(f"ALTER TABLE monitor.datasets_update UPDATE last_update='{str(cur_time)[:19]}',last_upload='{str(last_update)[:19]}',max_delta='{str(max_delta)[:19]}' WHERE dataset_name='MCRIAP_EOBR.{table}'")
  cli_17.execute(f"ALTER TABLE monitor.datasets_update UPDATE last_update='{str(cur_time)[:19]}',last_upload='{str(last_update)[:19]}',max_delta='{str(max_delta)[:19]}' WHERE dataset_name='MCRIAP_EOBR.segmentation_appeals'")
  for table in main_tables:
    cursor_5.execute(f"SELECT MAX(MODIFIED_DATE),MAX(SDU_LOAD_IN_DT) FROM BTSD_EOBRASHENIYA.{table.upper()}")
    res=cursor_5.fetchone()
    max_delta,last_update=res
    max_delta+=timedelta(hours=12)
    last_update+=timedelta(hours=6)
    conn_17.execute(f"ALTER TABLE monitor.tables_update UPDATE last_update='{str(cur_time)[:19]}',last_upload='{str(last_update)[:19]}',max_delta='{str(max_delta)[:19]}' WHERE table_dest='BTSD_EOBRASHENIYA.{table.upper()}'")
# Данный DAG работает четыре раза в день по давней договоренности 7:00,11:00,15:00,17:00
with DAG(dag_id="eobr_main",start_date=datetime(2022,2,15),catchup=False,tags=["eotinish"],schedule_interval="0 7,11,15,17 * * *") as dag:
  conn_eobr=get_connection_or_cursor("eotinish","conn","postgres")
  conn_5=get_connection_or_cursor("Clickhouse-5","cli","clickhouse")
  cursor_5=get_connection_or_cursor("Clickhouse-5","conn","clickhouse").cursor()
  cli_17=get_connection_or_cursor("Clickhouse-17","cli","clickhouse")
  cursor_6=get_connection_or_cursor("Clickhouse_52.6","conn","clickhouse").cursor()
  conn_6=get_connection_or_cursor("Clickhouse_52.6","cli","clickhouse")
  main_tables=["appeals","av_appeals_applications","av_applicants","appeals_decision"]
  main_sections=["main_sec","applicants_sec"]
  end_of_extract=DummyOperator(task_id="end_of_extract")
  for table in main_tables:
    task=PythonOperator(task_id=f"extract_from_{table}",python_callable=get_new_data,op_kwargs={"schema_dest":"BTSD_EOBRASHENIYA","table_dest":table.upper(),"conn_dest":conn_5,"schema_source":"public","table_source":table,"conn_source":conn_eobr})
    task>>end_of_extract
  taskt=PythonOperator(task_id=f"form_new_dataset",python_callable=form_new_datasets)
  for section in main_sections:
    task=PythonOperator(task_id=f"form_dataset_{section}",python_callable=form_datasets,op_kwargs={"schema_dest":"MCRIAP_EOBR","table_dest":section,"conn_dest":cli_17,"schema_source":"public","conn_source":cursor_5})
    end_of_extract>>task
    task>>taskt
  family=PythonOperator(task_id="family_appeals",python_callable=form_family_appeals)
  taskt>>family