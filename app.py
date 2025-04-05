import sys
from PyQt5.QtWidgets import (QApplication, QWidget, QVBoxLayout,
                             QLabel, QPushButton)
from PyQt5.QtCore import QThread, pyqtSignal
from buy_and_hold_backtest import running
from PyQt5 import QtCore,QtWidgets,QtGui
import datetime

class WorkerThread(QThread):
    trigger = pyqtSignal(str)
    finished = pyqtSignal()

    def __init__(self, symbol, s_t, e_t):
        super().__init__()
        self.symbol = symbol
        self.s_t = s_t
        self.e_t = e_t

    def run(self):
        # 'rb2305'
        # '2023-01-06 09:00:00'
        # '2023-01-06 09:00:20'
        running(self.symbol, datetime.datetime.strptime(self.s_t, '%Y-%m-%d %H:%M:%S'), \
                datetime.datetime.strptime(self.e_t, '%Y-%m-%d %H:%M:%S'))

class MyWidget(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()

    def initUI(self):
        # 设置窗口的位置和大小
        self.setGeometry(400, 300, 1600, 1200)  
        # 设置窗口的标题
        self.setWindowTitle('HappyQuant')

        layout = QVBoxLayout()
        self.setLayout(layout)

        self.mylabel = QLabel('e.g.\nrb2305\n2023-01-06 09:00:00\n2023-01-06 09:00:20', self)
        layout.addWidget(self.mylabel)

        self.mybutton = QPushButton('start', self)
        self.mybutton.clicked.connect(self.startThread)
        layout.addWidget(self.mybutton)

    def setUI(self, w):
        # 添加文本标签
        self.label = QtWidgets.QLabel(w)
        # 设置标签的左边距，上边距，宽，高
        self.label.setGeometry(QtCore.QRect(60, 20, 700, 45))
        # 设置文本标签的字体和大小，粗细等
        self.label.setFont(QtGui.QFont("Roman times",15))
        self.label.setText("symbol:")
        #添加设置一个文本框
        self.text = QtWidgets.QLineEdit(w)
        #调整文本框的位置大小
        self.text.setGeometry(QtCore.QRect(400,20,700,45))

        #第二个文本框的设置，同上，注意位置参数
        self.label_2 = QtWidgets.QLabel(w)
        self.label_2.setGeometry(QtCore.QRect(60, 100, 700, 45))
        self.label_2.setFont(QtGui.QFont("Roman times",15))
        self.label_2.setText("start time:")
        self.text_2 = QtWidgets.QLineEdit(w)
        self.text_2.setGeometry(QtCore.QRect(400,100,700,45))

        #第三个文本框的设置，同上，注意位置参数
        self.label_3 = QtWidgets.QLabel(w)
        self.label_3.setGeometry(QtCore.QRect(60, 180, 700, 45))
        self.label_3.setFont(QtGui.QFont("Roman times",15))
        self.label_3.setText("end time:")
        self.text_3 = QtWidgets.QLineEdit(w)
        self.text_3.setGeometry(QtCore.QRect(400,180,700,45))

        w.show()

    def startThread(self):
        self.mybutton.setDisabled(True)
        self.work = WorkerThread(self.text.text(), self.text_2.text(), self.text_3.text())
        self.work.start()
        self.work.trigger.connect(self.updateLabel)
        self.work.finished.connect(self.threadFinished)
        self.updateLabel(str(0))

    def threadFinished(self):
        self.mybutton.setDisabled(False)

    def updateLabel(self, text):
        self.mylabel.setText(text)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    w = MyWidget()
    w.setUI(w)
    w.show()
    sys.exit(app.exec_())