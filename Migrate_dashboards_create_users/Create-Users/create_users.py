from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from password_generator import PasswordGenerator
import time
import pandas as pd
#df=pd.DataFrame(columns=["firstname",'last_name','username','email','password'])
df=pd.read_excel(r"C:\Users\960808350048\Desktop\Create-Users\sdu_users.xlsx") # считываем эксель файл по формату как в sdu_users.xlsx
browser = webdriver.Chrome(executable_path=r"C:\Users\960808350048\Desktop\chromedriver.exe") # необходимо скачать драйвера,в данном случае chrome соотвествующей версией
browser.get("http://192.168.52.2:5088/") #Здесь пишем необходимый ip-адрес и порт superset'а
#Ниже учетные данные для superset'а
# # find username/email field and send the username itself to the input field
browser.find_element(By.ID,"username").send_keys("admin")
# #find password input field and insert password as well
browser.find_element(By.ID,"password").send_keys("admin")
#Дальше идет создание учетных записей при помощи библиотеки селениум
# # click login button
browser.find_element(By.CSS_SELECTOR,".btn-primary").click()
for index,row in df.iterrows():
    browser.get("http://192.168.52.2:5088/users/add")
    browser.find_element(By.ID,'first_name').send_keys(row["first_name"])
    browser.find_element(By.ID,'last_name').send_keys(row["last_name"])
    browser.find_element(By.ID,'username').send_keys(row["username"])
    browser.find_element(By.ID,'active').click()
    browser.find_element(By.ID,'email').send_keys(row["email"])
    browser.find_element(By.ID,"s2id_roles").click()
    browser.find_element(By.XPATH,f"//div[text()='{row['role']}']").click()
    browser.find_element(By.ID,'password').send_keys(row["passwd"])
    browser.find_element(By.ID,'conf_password').send_keys(row["passwd"])
    #browser.implicitly_wait(10)
    browser.find_element(By.CSS_SELECTOR,".btn-primary").click()
    time.sleep(7)
# #    for i in range(1400,)
# password = pwo.shuffle_password('d2lgJyvFn3wUo&e5Y9ErOQjzKfALM@GHaVPTx6uIpCqZSs7DtB18kWi&XRcm4hNb#', 10)
# #start=3001
# #end=3500
# for i in range(start,end+1):
# #for i in range(0,1):

