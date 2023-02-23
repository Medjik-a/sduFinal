import os
from docx_pars import parse_docx
from parse_images import parse_all_images
import pandas as pd

# Эта функция генерирует эксель файл из docx, pdf и img
# root_dit = директория с файлами
def create_excel(root_dir):
    df1 = parse_docx(root_dir)
    df2 = parse_all_images (root_dir)
    df=pd.concat([df1, df2], ignore_index=True)
    save_path=os.path.join(root_dir, 'registration_table.xlsx')
    df.to_excel(save_path,index=False)

