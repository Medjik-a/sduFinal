from airflow.models import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy import DummyOperator
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow_pentaho.operators.PanOperator import PanOperator
with DAG(dag_id="egsu_dictionaries",start_date=datetime(2022,10,20),catchup=False,schedule_interval='0 2 * * *') as dag:
  form_journal=PanOperator(
        dag=dag,
        task_id=f'form_journal',
        xcom_push=False,
        directory="/KPM/DATA_TRANS/ORACLE/ME/EGSU_NEW/TRANSFORM",
        level="Error",
        trans=f'CS_FORM_JOURNAL',
        params={"date": "{{ ds }}"})
  