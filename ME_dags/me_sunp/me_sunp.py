from datetime import timedelta,datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_pentaho.operators.PanOperator import PanOperator
from airflow.models.connection import Connection
from airflow.operators.dummy import DummyOperator

DAG_NAME = "me_sunp"
DEFAULT_ARGS = {
    'owner': 'Medjik',
    'depends_on_past': False,
    'start_date': datetime(2022,10,21),
    'max_active_runs':1
}
"""
Данный dag используется для загрузки данных с ИС "СУНП" и дальнейшего формирования таблиц для визуализации в Apache superset.
"""

with DAG(dag_id=DAG_NAME,
         default_args=DEFAULT_ARGS,
         schedule_interval="0 8-20 * * *",tags=["ME","SUNP","CUR_VOLUME_AZS"]) as dag:
    new_data = PanOperator(
        dag=dag,
        task_id="only_new_sunp_trans",
        xcom_push=False,
        directory="/KPM/DATA_TRANS/MSSQL/ME/SUNP_DB",
        level="Error",
        trans="only_new_sunp",
        params={"date": "{{ ds }}"})
    gasstation_new = PanOperator(
        dag=dag,
        task_id="GASSTATION_NEW",
        xcom_push=False,
        directory="/KPM/DATA_TRANS/MSSQL/ME/SUNP_DB",
        level="Error",
        trans="gasstationcounter_new",
        params={"date": "{{ ds }}"})
    gasstation_tmp = PanOperator(
        dag=dag,
        task_id="GASSTATION_TMP",
        xcom_push=False,
        directory="/KPM/DATA_TRANS/MSSQL/ME/SUNP_DB",
        level="Error",
        trans="gasstation_tmp",
        params={"date": "{{ ds }}"})
    sunp_nalichie = PanOperator(
        dag=dag,
        task_id="sunp_nalichie",
        xcom_push=False,
        directory="/KPM/DATA_TRANS/MSSQL/ME/SUNP_DB",
        level="Error",
        trans="SUNP_NALICHIE_V_AZS",
        params={"date": "{{ ds }}"})
    sunp_tank_volume = PanOperator(
        dag=dag,
        task_id="sunp_tank_volume",
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME",
        level="Error",
        trans="SUNP_MAXTANVOLUME",
        params={"date": "{{ ds }}"})
    sunp_postavka = PanOperator(
        dag=dag,
        task_id="sunp_postavka",
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME/TRANSFER_FOR_BOI",
        level="Error",
        trans="SUNP_POSTAVKA_6",
        params={"date": "{{ ds }}"})
    sunp_realizaciya = PanOperator(
        dag=dag,
        task_id="sunp_realizaciya",
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME/TRANSFER_FOR_BOI",
        level="Error",
        trans="SUNP_REALIZACIYA_6",
        params={"date": "{{ ds }}"})
    cur_volume = PanOperator(
        dag=dag,
        task_id="cur_volume",
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME/TRANSFER_FOR_BOI",
        level="Error",
        trans="CUR_VOLUME_AZS_6",
        params={"date": "{{ ds }}"})
    prognoz_table = PanOperator(
        dag=dag,
        task_id="prognoz_6",
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME",
        level="Error",
        trans="PROGNOZ_TABLE",
        params={"date": "{{ ds }}"})
    sunp_dinamika=PanOperator(
        dag=dag,
        task_id="sunp_dinamika",
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME/TRANSFER_FOR_BOI",
        level="Error",
        trans="SUNP_DINAMIKA",
        params={"date": "{{ ds }}"})
    new_data>>gasstation_new>>gasstation_tmp>>sunp_tank_volume>>sunp_nalichie
    sunp_nalichie>>sunp_realizaciya>>sunp_postavka>>cur_volume
    cur_volume>>prognoz_table>>sunp_dinamika