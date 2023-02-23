from fileinput import filename
from PIL import Image
import PIL
from PyPDF2 import PdfReader
import os
import glob
from pdf2image import convert_from_path
Image.MAX_IMAGE_PIXELS = 10000000000
poppler = r"C:\Program Files\poppler-22.04.0\bin"

# Эта функция конвертируе пдф файлы в изображения, одна страница - одно изображение
def parse_pdf_to_jpg(abs_path):
    folder_path = os.path.join(abs_path, 'PDF')
    filenames = glob.glob('{path}/*.pdf'.format(path=folder_path))
    for file in filenames:
        pages = convert_from_path(file, poppler_path=poppler)
        for i in range(len(pages)):
            try:
                pages[i].save(f"{file}{i}.jpg", 'JPEG')
            except PIL.Image.DecompressionBombError:
                continue
