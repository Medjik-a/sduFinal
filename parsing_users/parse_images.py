from PIL import Image
import pytesseract
import re
import os
import pandas as pd
import shutil
from password_gen import generate_password
Image.MAX_IMAGE_PIXELS = 10000000000

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'


def get_fio(img_file):
    first_name = ""
    last_name = ""
    text = pytesseract.image_to_string(Image.open(img_file), lang="rus")
    text = [*filter(lambda el: el, text.splitlines())]
    text = [re.sub('\s{2,}', ' ', el) for el in text]
    match = [re.search('ФИО пользователя', el) for el in text]
    for i in range(len(match)):
        if (match[i]):
            break
    if (i < len(text)-1):
        if (len(text[i].split(' ')) == 5):
            fio = text[i].split(' ')
            first_name = re.sub('[^а-яА-Я]', "", fio[3]
                                ) if len(fio[3]) > 4 else ""
            last_name = re.sub('[^а-яА-Я]', "", fio[2]
                               ) if len(fio[2]) > 4 else ""
            return first_name, last_name
        else:
            for j in range(i+1, len(text)):
                if (len(text[j].split(' ')) == 3):
                    if (re.search('(орган|гу|район|польз|телеф|уникал|бин|должн|отчес|удал|учет)', text[j].lower())):
                        continue
                    else:
                        fio = text[j].split(' ')
                        first_name = re.sub(
                            '[^а-яА-Я]', "", fio[1]) if len(fio[1]) > 4 else ""
                        last_name = re.sub(
                            '[^а-яА-Я]', "", fio[0]) if len(fio[0]) > 4 else ""
                        break
    return first_name, last_name


def get_email(img_file):
    user_email = ""
    text = pytesseract.image_to_string(Image.open(img_file), lang="eng")
    text = [*filter(lambda el: el, text.splitlines())]
    regex = re.compile(
        r'([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+')
    match = [re.search(regex, el) for el in text]
    email = [*filter(lambda el:el, match)]
    if (email):
        user_email = email[0][0]
    return user_email

# Извлекает данные из одной картинки, возвращает извлеченные данные в виде датафрейма
def parse(img_file):
    first_name, last_name = get_fio(img_file)
    email = get_email(img_file)
    username = email
    password = generate_password(10, 2, 3, 2, 1)
    role = ' Dashboard viewer'
    if email != '':
        filled = 'Yes'
    else:
        filled = 'No'

    entry = pd.DataFrame({'Surname': last_name, 'Name': first_name, 'username': username, 'email': email,
                         'role': role, 'password': password, 'email_is_filled': filled, 'file': img_file}, index=[0])
    return entry

# Извлекает данные из всех изображений из указанной директории 
def parse_all_images(root_dir):
    path = os.path.join(root_dir, 'images')
    files = os.listdir(path)
    df = pd.DataFrame(columns=['Surname', 'Name', 'username',
                      'email', 'role', 'password', 'email_is_filled', 'file'])
    for file in files:
        # print(file, len(df))
        p = path+'\\'+file
        entry = parse(p)
        df = pd.concat([df, entry], ignore_index=True)
    return df

