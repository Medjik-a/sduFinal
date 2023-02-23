import random
import string
from unicodedata import digit

'''
Эта функция для генерации пароля
lenght = длина пароля
n_upper = кол-во заглавных букв
n_lower = кол-во строчных букв
n_digits = кол-во цифр
n_special = кол-во специальных символов
'''
def generate_password(lenght, n_upper, n_lower, n_digits, n_special):
    lowercase = string.ascii_lowercase
    uppercase = string.ascii_uppercase
    numbers = string.digits
    special_characters = string.punctuation
    password_list = []
    combined = lowercase+uppercase+numbers+special_characters
    n_combined = lenght-n_upper - n_lower - n_digits - n_special
    for i in range(0, n_lower):
        r = random.choice(lowercase)
        password_list.append(r)
    for i in range(0, n_upper):
        r = random.choice(uppercase)
        password_list.append(r)
    for i in range(0, n_digits):
        r = random.choice(numbers)
        password_list.append(r)
    for i in range(0, n_special):
        r = random.choice(special_characters)
        password_list.append(r)
    for i in range(0, n_combined):
        r = random.choice(combined)
        password_list.append(r)

    password = ''.join(password_list)
    return password
