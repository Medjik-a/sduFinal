import subprocess
import os


def file_rename(file):
    """
    function to remove spaces from file name
    :param file:
    :return: new file name without spaces
    """
    new_file = '_'.join(file.split(' '))
    os.rename(file, new_file)
    return new_file


def unzip(folder_path):
    """
    recursive unzipping of archives from specified path
    :param path:
    :return:
    """
    files = os.listdir(folder_path)
    for file in files:
        filepath = file_rename(folder_path + '\\' + file)
        if file.endswith('.zip') or file.endswith('.rar'):
            subprocess.run(rf"C:\Program Files\WinRAR\WinRAR.exe x {filepath} {filepath[:-4]}\\")
            unzip(filepath[:-4])
        if os.path.isdir(filepath):
            unzip(filepath)
    return "End"

