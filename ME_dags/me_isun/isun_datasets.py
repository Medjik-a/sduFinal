from airflow.models import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow_pentaho.operators.PanOperator import PanOperator
import json

"""
Данный dag используется для загрузки данных с ИС "ИСУН" на 5 сервер(192.168.52.5) при помощи Pentaho трансформаций.Доступы даны только на 3 таблицы.
Дальше идет формирование таблиц для визуализации в Apache superset.
Так как для визуализации используется таблицы с двух ИС-это ИСУН и СУНП данный даг запускается через триггер внутри другого dag'a
"""
with DAG(dag_id="egsu_isun",start_date=datetime(2022,10,12),catchup=False,schedule_interval=None,tags=["ME","ISUN","SUNP"]) as dag:
  tables=["OILDATAS","OILFLOWDATAS","OILTANKDATAS"]
  postavka=PanOperator(
        dag=dag,
        task_id=f'ISUN_POSTAVKA_NEFTI_NA_ZAVODY',
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME/MOF",
        level="Error",
        trans=f'ISUN_POSTAVKA_NEFTI_NA_ZAVODY',
        params={"date": "{{ ds }}"})
  transport=PanOperator(
        dag=dag,
        task_id=f'ISUN_TRANSPORTIROVANO_NEFTI',
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME/MOF",
        level="Error",
        trans=f'ISUN_TRANSPORTIROVANO_NEFTI',
        params={"date": "{{ ds }}"})
  priem=PanOperator(
        dag=dag,
        task_id=f'ISUN_PRIEM_OT_ORG',
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME/MOF",
        level="Error",
        trans=f'ISUN_Ot_Org',
        params={"date": "{{ ds }}"})
  accoil_new = PanOperator(
        dag=dag,
        task_id="ACCOIL_NEW",
        xcom_push=False,
        directory="/KPM/DATA_TRANS/MSSQL/ME/SUNP_DB",
        level="Error",
        trans="gasstationcounter_new",
        params={"date": "{{ ds }}"})
  accoil_tmp = PanOperator(
        dag=dag,
        task_id="ACCOIL_TMP",
        xcom_push=False,
        directory="/KPM/DATA_TRANS/MSSQL/ME/SUNP_DB",
        level="Error",
        trans="accoil_tmp",
        params={"date": "{{ ds }}"})
  sunp_otgruzka= PanOperator(
        dag=dag,
        task_id="SUNP_HRANENIE_OTGUZKA",
        xcom_push=False,
        directory="/KPM/DATA_TRANS/MSSQL/ME/SUNP_DB",
        level="Error",
        trans="SUNP_HRANENIE",
        params={"date": "{{ ds }}"})
  egsu_sunp= PanOperator(
        dag=dag,
        task_id="EGSU_SUNP_JOINED",
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME/MOF",
        level="Error",
        trans="EGSU_SUNP_JOINED",
        params={"date": "{{ ds }}"})
  isun_egsu=PanOperator(
        dag=dag,
        task_id="ISUN_EGSU_JOINED",
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME/MOF",
        level="Error",
        trans="ISUN_EGSU_JOINED",
        params={"date": "{{ ds }}"})
  for table in tables:
    trans=PanOperator(
        dag=dag,
        task_id=f'{table}',
        xcom_push=False,
        directory="/KPM/DATA_TRANS/MSSQL/ME/ISUN/TRANS",  #Трансформации по загрузке данных с ИС ИСУН находятся здесь
        level="Error",
        trans=f'{table}',
        params={"date": "{{ ds }}"})
    trans>>postavka
  postavka>>transport>>priem>>accoil_new>>accoil_tmp>>sunp_otgruzka
  sunp_otgruzka>>egsu_sunp>>isun_egsu