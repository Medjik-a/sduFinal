import os
import subprocess
import re
import shutil

path_n = r"C:\Users\Professional\Desktop\Dashboards" #Поменять данный путь под свой админский компьютер,файлы отсюда будут преобразовываться в нужные для загрузки в суперсет
def edit_json(path,file,database_id="2",database_name="ClickHouse"):
    """
    Данная функция используется для преобразования json данных дашборда выгруженных из дашборда.
    Json данные дашборда выгружаются из более старых версий суперсета.
    :path: Путь к файлу
    :file: Название файла
    :database_id: Данный id нужно смотреть через sqllab суперсета
    :database_name: Данное название также надо смотреть через sqllab суперсета
    """
    with open(os.path.join(path,file)) as json_file:
        text=json_file.read()
    text = re.sub('"database_id":\s(.+),', f'"database_id": {database_id},', text)
    text=re.sub('\\\\"database_name\\\\":\s\\\\"[a-zA-Z0-9_]+\\\\"',f'\\\\"database_name\\\\": \\\\"{database_name}\\\\"',text)
    text = re.sub('\"changed_by_fk\":\s(.+),',f'"changed_by_fk": 1,', text)
    text = re.sub('\"created_by_fk\":\s(.+),',f'"created_by_fk": 1,', text)
    name,ext=os.path.splitext(file)
    with open(os.path.join(path,name+'_new'+ext),"w") as file:
        file.write(text)
def edit_zip(path,file):
    """
    Данная функция используется для изменения zip файла дашборда для загрузки в суперсет
    :param path: Путь к файлу
    :param file: Название файла
    """
    subprocess.run(rf"C:\Program Files\WinRAR\WinRAR.exe x {os.path.join(path, file)} {path}")
    files = os.path.join(path, file[:-4])
    edit_dashboard(files)
    edit_databases(files)
    edit_datasets(files)
    edit_charts(files)
    os.chdir(path)
    subprocess.run(rf"C:\Program Files\WinRAR\WinRAR.exe a -r {file[:-4] + '_new.zip'} {file[:-4]}")
def edit_dashboard(path):
    """
    Используется для изменения данных о дашборде в папке dashboards внутри архива
    :param path:
    """
    dashboards_path=os.path.join(path,"dashboards")
    files=os.listdir(dashboards_path)
    for file in files:
        if(file==".yaml"):
            os.rename(os.path.join(dashboards_path,file),os.path.join(dashboards_path,"ID.yaml"))

def edit_databases(path, database_name="ClickHouse",
                   sqlalchemy_uri="clickhouse+native://ch_backend:XXXXXXXXXX@192.168.52.6/default",
                   database_uuid="9f1fb7d5-86df-43ed-97fa-0a346d133983"):
    """
    Используется для изменения данных о базе данных используемой в дашборде
    :param path: Путь к файлу
    :param database_name: Название базы данных из sqllab
    :param sqlalchemy_uri: Также стоит смотреть из sqllab
    :param database_uuid: Также стоит смотреть из sqllab
    """
    databases_path = os.path.join(path, "databases")
    with open(os.path.join(databases_path, "ClickHouse.yaml")) as click:
        text = click.read()
    text = re.sub('uuid:\s(.+)\n', f"uuid: {database_uuid}\n", text)
    text = re.sub('sqlalchemy_uri:\s(.+)\n', f"sqlalchemy_uri: {sqlalchemy_uri}\n", text)
    with open(os.path.join(databases_path, "ClickHouse.yaml"), "w") as click:
        click.write(text)


def edit_datasets(path, database_uuid="9f1fb7d5-86df-43ed-97fa-0a346d133983"):
    """
    Данная функция меняет данные о дотасетах используемых в дашбордах
    :param path: Путь к файлу
    :param database_uuid: Нужно смотреть данный параметр в sqllab суперсета
    """
    datasets_path = os.path.join(path, "datasets", "ClickHouse")
    datasets_files = os.listdir(datasets_path)
    for dataset in datasets_files:
        with open(os.path.join(datasets_path, dataset)) as dataset_file:
            text = dataset_file.read()
        text = re.sub('database_uuid:\s(.+)\n', f"database_uuid: {database_uuid}\n", text)
        text = re.sub('schema:\snull', 'schema: default', text)
        with open(os.path.join(datasets_path, dataset), "w") as dataset_file:
            dataset_file.write(text)


def edit_charts(path):
    """
    Данная функция меняет данные о чартах дашборда для загрузки в суперсет
    :param path:
    """
    charts_path = os.path.join(path, "charts")
    chart_files = os.listdir(charts_path)
    counter = 1
    for chart in chart_files:
        if (re.match('([-]*_\d+)', chart)):
            if ('a'+str(counter) + chart.replace('-', "") in chart_files):
                os.remove(os.path.join(charts_path, chart))
            else:
                os.rename(os.path.join(charts_path, chart),
                          os.path.join(charts_path, 'ID'+chart.replace('-', "").replace(' ','')))
                counter += 1


def migrate(path, file):
    """
    Функция вызывает необходимую функцию преобразования в зависимости от формата данных (JSON или ZIP-архив)
    :param path: Путь к файлу
    :param file: Название файла
    """
    filepath = os.path.join(path, file)
    if file.endswith('.zip') or file.endswith('.rar'):
        edit_zip(path_n,file)
    elif file.endswith('.json'):
         edit_json(path,file)


dashboards=os.listdir(path_n)
for dashboard in dashboards:
    migrate(path_n,dashboard)
