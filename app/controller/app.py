# -*- coding: utf-8 -*-
import os
import sys
current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append('../controller')
sys.path.append('../utillity')
sys.path.append('../setting')

import numpy as np
from XMHandler import start
from threading import Thread


class App(object):
    def test1(self):
        thread1 = Thread(target=start)
        thread1.start()
        thread1.join()


if __name__ == '__main__':
    app = App()
    app.test1()