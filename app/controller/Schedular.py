# -*- coding: utf-8 -*-
import os
import sys
current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append('../utility')
sys.path.append('../model')

from datetime import datetime, timedelta
import Timeframe
import TimeUtility

class Schedular(object):

    def __init__(self):
        self.keys = []
        self.dic = {}
        self.active = True

    def addTask(self, key, timeframe):
        now = datetime.now()
        if timeframe.isMinute:
            t = datetime(now.year, now.month, now.day, now.hour, 0, 0)
            while t <= now:
                t += timeframe.deltaTime(1.0)
        else:
            t = datetime(now.year, now.month, now.day, 0)
            while t <= now:
                t += timedelta(hours=1)
        self.dic[key] = [t, timeframe]

    def shouldDoNow(self, key):
        if self.active == False:
            return False
        try:
            [next_time, timeframe] = self.dic[key]
            now = datetime.now()
            if now > next_time:
                while next_time < now:
                    if timeframe.isMinute:
                        next_time += timeframe.deltaTime(1.0)
                    else:
                        next_time += timedelta(hours=1)
                    self.dic[key] = [next_time, timeframe]
                return True
            else:
                return False
        except:
            return False

    def activate(self):
        self.active = True

    def inactivate(self):
        self.active = False