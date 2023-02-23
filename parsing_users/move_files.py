import os
import shutil
from site import abs_paths
from password_gen import generate_password

# Эта функция генерирует новое название для дублирующихся файлов


def file_rename(file_path):
    i = generate_password(5, 2, 3, 0, 0)
    path = os.path.split(file_path)[0]
    new_file_path = path+'\\' + \
        'file{n}'.format(n=i) + os.path.splitext(file_path)[-1]
    return new_file_path

# Эта функция перемещает файлы в папки в зависимотси от их формата


def move_file(file_path, abs_path, folder, file):
    try:
        if (os.path.join(file_path, file) != os.path.join(abs_path, folder, file)):
            print(os.path.join(file_path, file), '\n',
                  os.path.join(abs_path, folder, file))
            new_filepath = os.path.join(abs_path, folder, file)
            while (os.path.exists(new_filepath)):
                new_filepath = file_rename(new_filepath)
            shutil.move(os.path.join(file_path, file), new_filepath)
    except Exception as e:
        print(
            f"Can't move file {file} due to {e} from {os.path.join(file_path,file)} to {new_filepath}")

# Эта функция проверяет существование пути и перемещает файл


def check_if_exist_and_move(file_path, abs_path, folder, file):
    if os.path.exists(abs_path + '\\' + folder):
        # This will move the file to the directory
        # where the name 'ext' already exists
        move_file(file_path, abs_path, folder, file)

    else:
        # Создает новую директорию
        os.makedirs(abs_path + '\\' + folder)
        move_file(file_path, abs_path, folder, file)


# Основная функция, пермещает файлы в зависомсти от их формата
def move_fileby_ext(folder_path, absolute_path):
    images = ['jpeg', 'jpg', 'png', 'JPG', 'bmp', 'tiff', 'tif']
    docx = ['docx', 'doc']
    files = os.listdir(folder_path)
    for file in files:
        if os.path.isdir(folder_path + '\\' + file):
            move_fileby_ext(folder_path + '\\' + file, absolute_path)
            continue
        name, ext = os.path.splitext(file)
    # Сохраняется формат файла
        ext = ext[1:]
        if ext == '':
            continue
        elif ext.lower() in ['db', 'rar', 'zip']:
            os.remove(folder_path + r'\\' + file)
# Проверка на изображения
        elif ext.lower() in images:
            check_if_exist_and_move(folder_path, absolute_path, 'images', file)

# Проверка на docx файлы

        elif ext.lower() in docx:
            check_if_exist_and_move(folder_path, absolute_path, 'docx', file)

# Проверка всего остального
        else:
            check_if_exist_and_move(
                folder_path, absolute_path, ext.lower(), file)

# Эта функция удаляет пустые директории
def check_empty_directories(folder_path):
    files = os.listdir(folder_path)
    for file in files:
        if os.path.isdir(folder_path + r'\\' + file):
            if (not os.listdir(folder_path + r'\\' + file)):
                shutil.rmtree(folder_path + r'\\' + file)
                continue
            check_empty_directories(folder_path + r'\\' + file)
