from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.models.connection import Connection
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client, connect
from sqlalchemy import create_engine
from email.mime.text import MIMEText
from email.header import Header
import pandas as pd
import smtplib

"""
Данный код отправляет имейлы МЦРИАП с данными о кол-ве заявок на госуслуги и наиболее запрашиваемых госуслугах за предыдущий месяц
Для рассылки используется общая почта gos_uslugi_sdu@ie.gov.kz, которая имеет доступ к отправке писем через интернет (mail.ru, gmail.com, и.т.д)
"""


def get_engine_from_connection(driver, conn_id, database):
    """
    Эта функция для создания подключения к базе данных
    :param driver: тип базы данных (clickhouse)
    :param conn_id: id connection из airflow
    :param database: название базы данных
    :return:
    """
    conn_secrets = Connection.get_connection_from_secrets(conn_id=conn_id)
    return create_engine(f"{driver}://{conn_secrets.login}:{conn_secrets.password}@{conn_secrets.host}/{database}")


def get_smtp_server(conn_id):
    """
    Эта функция возвращает объект для взаимодействия с почтой с помощью SMTP
    :param conn_id: id подключения к smtp серверу (sdu_smtp)
    :return:
    """
    data = Connection.get_connection_from_secrets(conn_id=conn_id)
    server = smtplib.SMTP(host=data.host, port=data.port)
    server.ehlo()
    server.login(user=data.login, password=data.password)
    return server


def get_pokazateli(engine, file):
    """
    Эта функция выполняет SQL коды генерирующий необходимую таблицуы (Кол-во заявок на госуслги и их статус, Топ-5 запрашиваемых госуслуг), которые используется для рассылки
    :param engine: подключение к базе данных, на которой нужно исполнить код
    :param file: файл с SQL кодом
    :return:
    """
    dir = "/opt/airflow/dags/gosuglugi_smtp/sql/"

    with open(f"{dir}{file}.txt") as sql_file, open(f"{dir}zayavok.txt") as z:
        sql = sql_file.read()
        zayavok = z.read()
    df = pd.read_sql(sql, engine)

    if df['cnt'].sum() == 0:
        raise Exception('Data is not available')

    string = '<ol>'
    for index, [service, cnt] in df.iterrows():
        string += f"<li>{service} - {cnt} {zayavok}</li>"
    string += '</ol>'

    return string


def send_email(to_addrs, engine):
    """
    Эта функция формирует текст сообщения (в формате HTML) и отправляет его на указанные электронные адресса
    :param to_addrs: адресса, на которые нужно отправить письма
    :param engine: подключение к базе данных, с которой необходимо получить данные для рассылки
    """
    string_1 = get_pokazateli(engine, "sql_1")
    string_2 = get_pokazateli(engine, "sql_2")
    last_day = datetime.today().replace(day=1) - timedelta(days=1)
    first_day = last_day.replace(day=1)
    with open("/opt/airflow/dags/gosuglugi_smtp/sql/message.txt") as message:
        msg = message.read().format(first_day=first_day.strftime("%d.%m.%Y"), last_day=last_day.strftime("%d.%m.%Y"),
                                    string_1=string_1, string_2=string_2)
    with open("/opt/airflow/dags/gosuglugi_smtp/sql/subject.txt") as sub:
        subject = sub.read()
    msg = MIMEText(msg, 'html', 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = "gos_uslugi_sdu@ie.gov.kz"
    msg['To'] = ", ".join(to_addrs)
    server = get_smtp_server("sdu_smtp")
    server.sendmail(msg['From'], to_addrs, msg.as_string())
    server.close()


with DAG(dag_id="gosuslugi_smtp_jan", start_date=datetime(2023, 1, 11, 12, 30), catchup=False,
         tags=["MCRIAP", "SMTP", "GOSUSLUGI"],
         schedule_interval = "@once") as dag:
    engine = get_engine_from_connection("clickhouse+native", "Clickhouse-17", "GOSUSLUGI")
    to_addrs = ["Arsen.Amankeldi@nitec.kz", "Madi.Akhmetov@nitec.kz"]
    send = PythonOperator(task_id="send_email", python_callable=send_email,
                          op_kwargs={"to_addrs": to_addrs, "engine": engine})