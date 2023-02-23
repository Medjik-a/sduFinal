from logging import root
import docx
import numpy as np
import pandas as pd
import glob
import os
from password_gen import generate_password

'''
Эта функция извлекает имя, фамилию и электронный адресс из docx файлов с заявками в указанной директории
Функция возвращает dataframe содержащий следующие столбцы: name, surname, username, email, role, password и file(указывает файл из которого были извлечены данные)
'''

def parse_docx(root_dir):
    abs_path = os.path.join(root_dir, 'docx')
    filenames = glob.iglob(abs_path + '**/**', recursive=True)
    tables = []  # Лист с dataframes
    for name in filenames:
        try:
            doc = docx.Document(name)
        except FileNotFoundError:
            continue
        except ValueError:
            continue

        # Извлекает все таблицы из docx файла и конвертирует их в dataframe

        for table in doc.tables:
            df = [['' for i in range(len(table.columns))]
                  for j in range(len(table.rows))]
            for i, row in enumerate(table.rows):
                for j, cell in enumerate(row.cells):
                    if cell.text:
                        df[i][j] = cell.text
            try:
                df[2][0] = name
            except IndexError:
                continue
            tables.append(pd.DataFrame(df))

    # Поиск ФИО и email
    store_email = []
    store_fio = []
    store_files = []
    for df in tables:
        try:
            df = df.iloc[:, 0:2]
            df.columns = ['Column1', 'Column2']
            fio = df.loc[df['Column1'] ==
                         'ФИО пользователя', 'Column2'].values[0]
            email = df.loc[df['Column1'] ==
                           'Е-mail (Электронный адрес пользователя является его логином и должен быть уникальным)', 'Column2'].values[0]
            file = df['Column1'][2]
            store_fio.append(fio)
            store_email.append(email)
            store_files.append(file)
        # All tables without relevant info are skipped
        except ValueError:
            continue
        except IndexError:
            continue

    # Форматирует датафрейм
    df = pd.DataFrame()
    df['FIO'] = store_fio
    df['FIO'] = df['FIO'].replace(r'\n', ' ', regex=True)
    df['FIO'] = df['FIO'].astype(str)
    df[['Surname', 'Name', 'Middle_name']
       ] = df.FIO.str.split(' ', expand=True, n=2)
    df = df.drop(['FIO', 'Middle_name'], axis=1)
    df['username'] = store_email
    df['username'] = df['username'].replace(r'\n', '', regex=True)
    df['username'] = df['username'].replace(r'\t', '', regex=True)
    df['email'] = df['username']
    df['role'] = 'Dashboard viewer'
    df['password'] = ''
    # Генерация пароля
    for i in range(df.shape[0]+5):
        try:
            p = generate_password(10, 2, 3, 2, 1)
            while p in df['password']:
                p = generate_password(10, 2, 3, 2, 1)
            df['password'][i] = p
        except IndexError:
            continue

    df = df.replace(r'^\s+', '', regex=True)
    df['email_is_filled'] = df['email'].apply(lambda x: 'Yes' if x else 'No')
    df['file'] = store_files
    df = df.drop_duplicates(
        subset=['Surname', 'Name'], keep='first', ignore_index=True)
    return df

