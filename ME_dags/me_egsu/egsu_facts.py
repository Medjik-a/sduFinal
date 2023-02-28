from airflow.models import DAG
from datetime import datetime,timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow_pentaho.operators.PanOperator import PanOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import json
"""
Данный факт используется для загрузки данных с ИС ЕГСУ,а также формирование витрин для визуализации в Apache Superset.
Так как для визуализации используется витрины с двумя ИС или разная логика формирования витрин,из-за этого в dag происходит вызов других dag.
Данный dag вызывает dag egsu_monitoring,isun_datasets
"""
with DAG(dag_id="egsu_facts",start_date=datetime(2022,10,19),catchup=False,schedule_interval='0 10 * * *',tags=["ME","EGSU","CS_FACTS"]) as dag:
  vypolnenie=["CS_FACT_F02_WITH_DATE","CS_FACT_F09_WITH_DATE","CS_FACT_F10_WITH_DATE","CS_FACT_F27_WITH_DATE","CS_FACT_F46_RAW_OILGAS_WITH_DATE","CS_FACT_F49_WITH_DATE","CS_FACT_F54_WITH_DATE"]
  tables=["CS_FACT_F02","CS_FACT_F05","CS_FACT_F06","CS_FACT_F09","CS_FACT_F10","CS_FACT_F13","CS_FACT_F17","CS_FACT_F27","CS_FACT_F31","CS_FACT_F34","CS_FACT_F38_EXPORT","CS_FACT_F40","CS_FACT_F46_OILGASBRAND","CS_FACT_F46_RAW_OILGAS","CS_FACT_F49","CS_FACT_F52","CS_FACT_F54","CS_FACT_F66"]
  path="/opt/airflow/dags/me_egsu/Sql-scripts/"
  egsu_monitoring=TriggerDagRunOperator(trigger_dag_id="egsu_monitoring",task_id="egsu_monitoring") #Запуск другого dag внутри этого
  form_journal = PanOperator(
    dag=dag,
    task_id=f'form_journal',
    xcom_push=False,
    directory="/KPM/DATA_TRANS/ORACLE/ME/EGSU_NEW/TRANSFORM",
    level="Error",
    trans=f'CS_FORM_JOURNAL',
    params={"date": "{{ ds }}"})
  isun_sunp=TriggerDagRunOperator(trigger_dag_id="egsu_isun",task_id="egsu_isun") #Запуск другого dag внутри этого
  plan_vypolneniya = PanOperator(
    dag=dag,
    task_id=f'PLAN_VYPOLNENIYA',
    xcom_push=False,
    directory="/KPM/DATA_MARTS/ME/MOF",
    level="Error",
    trans=f'PLAN_VYPOLNENIYA',
    params={"date": "{{ ds }}"})
  dobycha = PanOperator(
    dag=dag,
    task_id=f'PLAN_VYPOLNENIYA_DOBYCHA',
    xcom_push=False,
    directory="/KPM/DATA_MARTS/ME/MOF/GRAPHS",
    level="Error",
    trans=f'VYPOLNENIE_PLANA_DOBYCHA',
    params={"date": "{{ ds }}"})
  plan_fact=PanOperator(
    dag=dag,
    task_id=f'PLAN_FACT_GRAPH',
    xcom_push=False,
    directory="/KPM/DATA_MARTS/ME/MOF/GRAPHS",
    level="Error",
    trans=f'PLAN_FACT_GRAPH',
    params={"date": "{{ ds }}"})
  with open(path+"forms.json") as file1:
    monitoring_forms=json.load(file1)
  monitoring_tables=[form["table_name"] for form in monitoring_forms]
  for table in tables:
    cs_fact=PanOperator(
        dag=dag,
        task_id=f'{table}',
        xcom_push=False,
        directory="/KPM/DATA_TRANS/ORACLE/ME/EGSU_NEW/TRANSFORM",
        level="Error",
        trans=f'{table}',
        params={"date": "{{ ds }}"})
    cs_fact_date = PanOperator(
        dag=dag,
        task_id=f'{table+"_WITH_DATE"}',
        xcom_push=False,
        directory="/KPM/DATA_MARTS/ME/MOF/CS_FACTS_WITH_DATE",
        level="Error",
        trans=f'{table+"_WITH_DATE"}',
        params={"date": "{{ ds }}"})
    form_journal>>cs_fact>>cs_fact_date
    table_date=table+'_WITH_DATE'
    if(table_date in monitoring_tables):
      cs_fact_date>>egsu_monitoring
    if (table_date in ["CS_FACT_F46_OILGASBRAND_WITH_DATE", "CS_FACT_F46_RAW_OILGAS_WITH_DATE"]):
        cs_fact_date>>isun_sunp
    if(table_date in vypolnenie):
      cs_fact_date>>plan_vypolneniya>>dobycha>>plan_fact
