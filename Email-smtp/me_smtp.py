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
Данный код отправляет имейлы с данными об уровне заполненности АЗС для сотрудников Министерства Энергетики
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
    conn_secrets = Connection.get_connection_from_secrets(conn_id=conn_id)  # Получить параметры подключения из Airflow
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


def get_azs_volume(engine):
    """
    Эта функция выполняет SQL код генерирующий необходимую таблицу (Уровень заполненности АЗС по регионам), которая используется для рассылки
    :param engine: подключение к базе данных, на которой нужно исполнить код
    :return:
    """
    with open("/opt/airflow/dags/me_smtp/Smtp_files/volume.txt") as sql_file:
        sql = sql_file.read()
    df = pd.read_sql(sql, engine)
    if df.empty:
        raise Exception("No data is available")
    return df.to_html(index=False, justify="left")


def send_email(to_addrs, engine):
    """
    Эта функция формирует текст сообщения (в формате HTML) и отправляет его на указанные электронные адресса
    :param to_addrs: адресса, на которые нужно отправить письма
    :param engine: подключение к базе данных, с которой необходимо получить данные для рассылки
    """
    body = get_azs_volume(engine)
    date = datetime.now()

    with open("/opt/airflow/dags/me_smtp/Smtp_files/header.txt") as message:
        msg = message.read().format(df=body, date=date.strftime("%d.%m.%Y"))  # not necessary here
    with open("/opt/airflow/dags/me_smtp/Smtp_files/subject.txt") as subject_file:
        subject = subject_file.read()
    msg = MIMEText(msg, 'html', 'utf-8')
    msg['Subject'] = Header(subject, 'utf-8')
    msg['From'] = "gos_uslugi_sdu@ie.gov.kz"
    msg['To'] = ", ".join(to_addrs)
    server = get_smtp_server("sdu_smtp")
    server.sendmail(msg['From'], to_addrs, msg.as_string())
    server.close()


with DAG(dag_id="me_smtp", start_date=datetime(2022, 12, 21), catchup=False, tags=["ME", "SMTP", "CUR_VOLUME_AZS"],
         schedule_interval='30 12 * * 1-5') as dag:
    engine = get_engine_from_connection("clickhouse+native", "Clickhouse-17", "ME")
    to_addrs = ["k.khasenov@iacng.kz", "A.anissimov@energo.gov.kz", "minenergo@mail.kz", "Zh.zhakhmetov@energo.gov.kz",
                "A.magauov@energo.gov.kz", "Zhakenov_AS@ukimet.kz", "Maguzumov_ae@ukimet.kz", "Gds@ukimet.kz",
                "Sakhov_zhm@ukimet.kz", "Arsen.Amankeldi@nitec.kz", "Madi.Akhmetov@nitec.kz",
                "zh.zhakhmetova@energo.gov.kz", "s.mergenov@energo.gov.kz", "a.sauatov@energo.gov.kz",
                "B.akchulakov@energo.gov.kz"]
    send = PythonOperator(task_id="send_email", python_callable=send_email,
                          op_kwargs={"to_addrs": to_addrs, "engine": engine})
