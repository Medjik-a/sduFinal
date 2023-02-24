from datetime import datetime
from airflow import DAG
from clickhouse_driver import Client, connect
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection


def TRUNCATE_DICT():
	cursor_CH_SBD.execute("truncate table DM_ZEROS.DIC_KATO_REF")

def KATO_2():
	cursor_CH_SBD.execute("""INSERT INTO DM_ZEROS.DIC_KATO_REF
	(FULL_KATO, KATO_2, KATO_2_NAME, FULL_KATO_NAME, TYPE_OF_LOCATION)
	SELECT 
	if(LENGTH(ID)=2, KATO_CODE, NULL) as FULL_KATO,
	ID as KATO_2,
	NAME_RU as KATO_2_NAME,
	NAME_RU as FULL_KATO_NAME,
	`TYPE` as TYPE_OF_LOCATION
	FROM BTSD_EOBRASHENIYA.LOCATIONS
	WHERE FULL_KATO is not NULL""")


def KATO_4():
	cursor_CH_SBD.execute("""INSERT INTO DM_ZEROS.DIC_KATO_REF
	(FULL_KATO, KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, FULL_KATO_NAME, TYPE_OF_LOCATION)
	SELECT 
	loc2.KATO_CODE as FULL_KATO,
	loc1.ID as KATO_2,
	loc1.NAME_RU as KATO_2_NAME,
	loc2.ID as KATO_4,
	loc2.NAME_RU as KATO_NAME_4,
	loc2.NAME_RU as FULL_KATO_NAME,
	loc2.TYPE as TYPE_OF_LOCATION
	FROM BTSD_EOBRASHENIYA.LOCATIONS loc1
	left join BTSD_EOBRASHENIYA.LOCATIONS loc2 on loc1.ID = loc2.PARENT_ID 
	where loc2.ID in (select toString(arrayJoin(range(1000, 9999, 1))))
	""")

def KATO_REPUBLICAN_CITY():
	cursor_CH_SBD.execute("""INSERT INTO DM_ZEROS.DIC_KATO_REF
	(FULL_KATO, KATO_2, KATO_2_NAME, KATO_6, KATO_6_NAME, FULL_KATO_NAME, TYPE_OF_LOCATION)
	SELECT 
	FULL_KATO, 
	KATO_4 as KATO_2, 
	KATO_4_NAME as KATO_2_NAME,
	KATO_6,
	KATO_6_NAME,
	FULL_KATO_NAME,
	TYPE_OF_LOCATION
	from
		(
			select
			*
			FROM
				(
					SELECT 
					loc2.KATO_CODE as FULL_KATO, 
					loc.ID as KATO_2, 
					loc.NAME_RU  as KATO_2_NAME,
					loc1.ID as KATO_4, 
					loc1.NAME_RU as KATO_4_NAME,
					loc2.ID AS KATO_6,
					loc2.NAME_RU as KATO_6_NAME,
					loc2.NAME_RU as FULL_KATO_NAME,
					loc2.TYPE as TYPE_OF_LOCATION
					FROM BTSD_EOBRASHENIYA.LOCATIONS loc
					left join BTSD_EOBRASHENIYA.LOCATIONS loc1 on loc.ID = loc1.PARENT_ID 
					left join BTSD_EOBRASHENIYA.LOCATIONS loc2 on loc1.ID = loc2.PARENT_ID 
					where loc2.ID in (select toString(arrayJoin(range(100000, 999999, 1))))
				) where KATO_2_NAME = 'Республика Казахстан')""")


def KATO_6():
	cursor_CH_SBD.execute("""INSERT INTO DM_ZEROS.DIC_KATO_REF
	(FULL_KATO, KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, KATO_6, KATO_6_NAME, FULL_KATO_NAME, TYPE_OF_LOCATION)
	SELECT 
	FULL_KATO, 
	KATO_2, 
	KATO_2_NAME,
	KATO_4, 
	KATO_4_NAME,
	KATO_6,
	KATO_6_NAME,
	FULL_KATO_NAME,
	TYPE_OF_LOCATION
	FROM
		(
			SELECT 
			loc2.KATO_CODE as FULL_KATO, 
			loc.ID as KATO_2, 
			loc.NAME_RU  as KATO_2_NAME,
			loc1.ID as KATO_4, 
			loc1.NAME_RU as KATO_4_NAME,
			loc2.ID AS KATO_6,
			loc2.NAME_RU as KATO_6_NAME,
			loc2.NAME_RU as FULL_KATO_NAME,
			loc2.TYPE as TYPE_OF_LOCATION,
			loc2.ID
			FROM BTSD_EOBRASHENIYA.LOCATIONS loc
			left join BTSD_EOBRASHENIYA.LOCATIONS loc1 on loc.ID = loc1.PARENT_ID 
			left join BTSD_EOBRASHENIYA.LOCATIONS loc2 on loc1.ID = loc2.PARENT_ID 
			where loc.ID.ID in (select toString(arrayJoin(range(10, 99, 1))))
		) where KATO_2_NAME != 'Республика Казахстан' and LENGTH(KATO_6) < 7 """)

def KATO_FULL():
	cursor_CH_SBD.execute("""INSERT INTO DM_ZEROS.DIC_KATO_REF
	(FULL_KATO, KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, KATO_6, KATO_6_NAME, FULL_KATO_NAME, TYPE_OF_LOCATION)
	SELECT
	DISTINCT
	FULL_KATO,
	KATO_2,
	KATO_2_NAME,
	KATO_4, 
	KATO_4_NAME,
	KATO_6,
	KATO_6_NAME,
	FULL_KATO_NAME,
	TYPE_OF_LOCATION
	from
		(
			SELECT
			FULL_KATO,
			KATO_2,
			KATO_2_NAME,
			KATO_4, 
			KATO_4_NAME,
			KATO_6,
			KATO_6_NAME,
			FULL_KATO_NAME,
			TYPE_OF_LOCATION,
			toUInt64(ID) as ID_TO_CHECK
			FROM
				(	
					SELECT 
					loc3.KATO_CODE as FULL_KATO, 
					loc.ID as KATO_2, 
					loc.NAME_RU  as KATO_2_NAME,
					loc1.ID as KATO_4, 
					loc1.NAME_RU as KATO_4_NAME,
					loc2.ID AS KATO_6,
					loc2.NAME_RU as KATO_6_NAME,
					loc3.NAME_RU as FULL_KATO_NAME,
					loc3.TYPE as TYPE_OF_LOCATION,
					if(empty(loc3.ID)=1, '1', loc3.ID) as ID
					FROM BTSD_EOBRASHENIYA.LOCATIONS loc
					left join BTSD_EOBRASHENIYA.LOCATIONS loc1 on loc.ID = loc1.PARENT_ID 
					left join BTSD_EOBRASHENIYA.LOCATIONS loc2 on loc1.ID = loc2.PARENT_ID
					left join BTSD_EOBRASHENIYA.LOCATIONS loc3 on loc2.ID = loc3.PARENT_ID

					UNION ALL 
					
					SELECT 
					'334635200' as FULL_KATO,
					'33' as KATO_2, 
					'область Жетісу'  as KATO_2_NAME,
					'3346' as KATO_4, 
					'Панфиловский район' as KATO_4_NAME,
					'334635' AS KATO_6,
					'Улкенагашский с.о.' as KATO_6_NAME,
					'с.Алмалы' as FULL_KATO_NAME,
					'RURAL_COUNTY' as TYPE_OF_LOCATION,
					'3346352' as ID
				) 
			where ID_TO_CHECK BETWEEN 1000000 and 999999999
			and KATO_2_NAME != 'Республика Казахстан'
		) where toInt64(KATO_2) BETWEEN 10 and 99""")


	cursor_CH_SBD.execute("""INSERT INTO DM_ZEROS.DIC_KATO_REF
	(FULL_KATO, KATO_2, KATO_2_NAME, KATO_4, KATO_4_NAME, FULL_KATO_NAME, TYPE_OF_LOCATION)
	SELECT
	DISTINCT
	FULL_KATO,
	KATO_4_C as KATO_2, 
	KATO_4_NAME_C as KATO_2_NAME,
	KATO_6 as KATO_4,
	KATO_6_NAME as KATO_4_NAME,
	FULL_KATO_NAME,
	TYPE_OF_LOCATION
	from
		(
			SELECT
			FULL_KATO,
			KATO_4 as KATO_4_C, 
			KATO_4_NAME KATO_4_NAME_C,
			KATO_6,
			KATO_6_NAME,
			FULL_KATO_NAME,
			TYPE_OF_LOCATION,
			toUInt64(ID) as ID_TO_CHECK
			FROM
				(	
					SELECT 
					loc3.KATO_CODE as FULL_KATO, 
					loc.ID as KATO_2, 
					loc.NAME_RU  as KATO_2_NAME,
					loc1.ID as KATO_4, 
					loc1.NAME_RU as KATO_4_NAME,
					loc2.ID AS KATO_6,
					loc2.NAME_RU as KATO_6_NAME,
					loc3.NAME_RU as FULL_KATO_NAME,
					loc3.TYPE as TYPE_OF_LOCATION,
					if(empty(loc3.ID)=1, '1', loc3.ID) as ID
					FROM BTSD_EOBRASHENIYA.LOCATIONS loc
					left join BTSD_EOBRASHENIYA.LOCATIONS loc1 on loc.ID = loc1.PARENT_ID 
					left join BTSD_EOBRASHENIYA.LOCATIONS loc2 on loc1.ID = loc2.PARENT_ID
					left join BTSD_EOBRASHENIYA.LOCATIONS loc3 on loc2.ID = loc3.PARENT_ID
					where KATO_2_NAME = 'Республика Казахстан'
				) 
			where ID_TO_CHECK BETWEEN 1000000 and 999999999
		) """)


with DAG("eobr_dict_kato_flat_table", description="eobr_dict_kato_flat_table", start_date=datetime(2022, 5, 1), schedule_interval="0 6 * * *", catchup=False) as dag:
	ch_5_sec = Connection.get_connection_from_secrets(conn_id="Clickhouse-5")
	conn_CH_SBD = connect(host=ch_5_sec.host, port=ch_5_sec.port, password=ch_5_sec.password, user=ch_5_sec.login, connect_timeout=3600)

	cursor_CH_SBD = conn_CH_SBD.cursor()

	truncate = PythonOperator(
		owner='Zhantore',
 		task_id='TRUNCATE_DICT',
		python_callable=TRUNCATE_DICT,
	)

	kato_2 = PythonOperator(
		owner='Zhantore',
 		task_id='KATO_2',
		python_callable=KATO_2,
	)

	kato_4 = PythonOperator(
		owner='Zhantore',
 		task_id='KATO_4',
		python_callable=KATO_4,
	)

	republic_city = PythonOperator(
		owner='Zhantore',
 		task_id='KATO_REPUBLICAN_CITY',
		python_callable=KATO_REPUBLICAN_CITY,
	)

	kato_6 = PythonOperator(
		owner='Zhantore',
 		task_id='KATO_6',
		python_callable=KATO_6,
	)

	kato_full = PythonOperator(
		owner='Zhantore',
 		task_id='KATO_FULL',
		python_callable=KATO_FULL,
	)

	truncate>>kato_2>>kato_4 >>republic_city>>kato_6>>kato_full