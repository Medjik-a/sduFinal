from email import message
import sys
import socket
from typing_extensions import Self
from PyQt5.QtWidgets import *
from PyQt5.QtGui import QIntValidator,QDoubleValidator,QFont
from PyQt5.QtCore import Qt
#from experiemnt_create_accounts import create_accoutns
#
class MainWindow(QMainWindow): # главное окно
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi()
    def setupUi(self):
        self.move(500, 500) # положение окна
        self.resize(800, 700) # размер окна
        self.title="Create user profiles"
    #errors

        label_1=QLabel("Username",self)
        label_1.move(10, 0)
        label_1.resize(200, 75)
        label_2=QLabel("Password",self)
        label_2.move(10, 95)
        label_2.resize(200, 75)
        label_3=QLabel("IP", self)
        label_3.move(10, 190)
        label_3.resize(200, 75)
        label_4=QLabel("Port", self)
        label_4.move(10, 285)
        label_4.resize(200, 75)
        label_5=QLabel("Excel file", self)
        label_5.move(10, 380)
        label_5.resize(200, 75)
        label_6=QLabel("Executable path", self)
        label_6.move(10, 475)
        label_6.resize(200, 75)
        
        self.textbox_username = QLineEdit(self)
        self.textbox_username.move(230, 0)
        self.textbox_username.resize(380,75)
        
        self.textbox_password = QLineEdit(self)
        self.textbox_password.move(230, 95)
        self.textbox_password.resize(380,75)
        self.textbox_password.setEchoMode(QLineEdit.Password)
        
        self.textbox_ip=QLineEdit(self)
        self.textbox_ip.move(230, 190)
        self.textbox_ip.resize(380,75)
        
        self.textbox_port=QLineEdit(self)
        self.textbox_port.move(230, 285)
        self.textbox_port.resize(380,75)
        
        self.textbox_xlxs=QLineEdit(self)
        self.textbox_xlxs.move(230, 380)
        self.textbox_xlxs.resize(380,75)
        
        self.textbox_exec_path=QLineEdit(self)
        self.textbox_exec_path.move(230, 475)
        self.textbox_exec_path.resize(380,75)

        start_button= QPushButton('Start program', self)
        start_button.move(100,570)
        start_button.resize(600, 75)
        start_button.clicked.connect(self.start)

        xlxs_button=QPushButton('Select', self)
        xlxs_button.move(630, 380)
        xlxs_button.resize(150,75)
        xlxs_button.clicked.connect(self.select_excel)
        
        exec_button=QPushButton('Select', self)
        exec_button.move(630, 475)
        exec_button.resize(150,75)
        exec_button.clicked.connect(self.select_exec_path)
        self.show()

    def get_directory(self):
        folder_path = QFileDialog.getExistingDirectory(self, 'Select a directory')
        folder_path = folder_path.replace('/', '\\')
        return folder_path
        #@pyqtSlot()
    
    def get_file(self):
        file_path = QFileDialog.getOpenFileName(self, 'Select file')[0]
        return file_path
    #ip port check
    
    def start(self):
        message_error=QMessageBox()
        message_error.setText("IP is not valid.Try again")
        xlxs=self.textbox_xlxs.text()
        exec_path=self.textbox_exec_path.text()
        print(xlxs, exec_path)
        username=self.textbox_username.text()
        password= self.textbox_password.text()
        ip=self.textbox_ip.text()
        port=self.textbox_port.text()
        try: 
            socket.inet_aton(ip)
        except:
            message_error.exec()
            self.textbox_ip.setText("")
            ip = self.textbox_ip.text()
        
        port="http://{ip}:{p}/".format(ip=ip, p=port)
        print(username, password, port)
        #create_accoutns(xlxs, exec_path, port, username, password)

    def select_excel(self):
        excel_path=self.get_file()
        message_error=QMessageBox()
        message_error.setText('Wrong file type. Try again')
        if '.xlsx' not in excel_path:
            message_error.exec()
            excel_path=self.get_file()
        else:
            pass
        self.textbox_xlxs.setText(excel_path)

    def select_exec_path(self):
        exec_path=self.get_file()
        message_error=QMessageBox()
        message_error.setText('Wrong file type. Try again')
        if '.exe' not in exec_path:
            message_error.exec()
            exec_path=self.get_file()
        else:
            pass
        self.textbox_exec_path.setText(exec_path)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MainWindow()
    sys.exit(app.exec_())