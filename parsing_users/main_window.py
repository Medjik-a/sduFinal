from csv import excel
from PyQt5.QtWidgets import *
from PyQt5.QtCore import pyqtSlot
from unzip_files import file_rename,unzip
from PyQt5.QtWidgets import QMessageBox
from move_files import move_fileby_ext, check_empty_directories
from parse_pdf import parse_pdf_to_jpg
from write_to_excel import create_excel
import sys
class MainWindow(QMainWindow): # главное окно
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi()
    def setupUi(self):
        self.move(500, 500) # положение окна
        self.resize(800, 600) # размер окна
        self.title="Parsing users from files"
        self.setWindowTitle(self.title)
        unzip_button= QPushButton('Unzip recursively directory', self)
        unzip_button.move(100,0)
        unzip_button.setToolTip('Winrar is needed to run this module')
        unzip_button.clicked.connect(self.unzip_folder)
        unzip_button.resize(600,75)
        move_button = QPushButton('Move files', self)
        move_button.resize(600,75)
        move_button.move(100,85)
        move_button.setToolTip('Choose folder')
        move_button.clicked.connect(self.move_files)
        button = QPushButton('Create excel with registration data', self)
        button.resize(600,75)
        button.move(100,170)
        button.setToolTip('Choose folder')
        button.clicked.connect(self.parse_files_to_excel)
        self.show()
    def get_directory(self):
        folder_path = QFileDialog.getExistingDirectory(self, 'Select a directory')
        folder_path = folder_path.replace('/', '\\')
        return folder_path
    @pyqtSlot()
    def unzip_folder(self):
        path=self.get_directory()
        unzip(path)
    def move_files(self):
        path=self.get_directory()
        move_fileby_ext(path,path)
        parse_pdf_to_jpg(path)
        move_fileby_ext(path,path)
        check_empty_directories(path)
    @pyqtSlot()
    def parse_files_to_excel(self):
        create_excel(self.get_directory())
 #       print(folder_path)
#        unzip(folder_path)
#        move_fileby_ext(folder_path,folder_path)
#        for i in range(15):
#            check_empty_directories(folder_path)
#         msg = QMessageBox()
#         msg.setIcon(QMessageBox.Critical)
#         msg.setText("Ended moving files")
#         msg.setInformativeText('More information')
#         msg.setWindowTitle("End of moving")
#         msg.exec_()
        #parse_pdf(folder_path)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    sys.exit(app.exec_())