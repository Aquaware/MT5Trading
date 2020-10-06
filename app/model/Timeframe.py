import os
import sys
current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append('../utility')

from TimeUtility import TimeUtility
import MetaTrader5 as mt5

MINUTE = 'MINUTE'
HOUR = 'HOUR'
DAY = 'DAY'
             # symbol : [(mt5 timeframe constants), number, unit]
TIMEFRAME = {'M1': [mt5.TIMEFRAME_M1,  1, MINUTE],
             'M5': [mt5.TIMEFRAME_M5,  5, MINUTE],
             'M10': [mt5.TIMEFRAME_M10, 10, MINUTE],
             'M15': [mt5.TIMEFRAME_M15, 15, MINUTE],
             'M30': [mt5.TIMEFRAME_M30, 30, MINUTE],
             'H1': [mt5.TIMEFRAME_H1  ,  1, HOUR],
             'H4': [mt5.TIMEFRAME_H4,    4, HOUR],
             'H8': [mt5.TIMEFRAME_H8,    8, HOUR],
             'D1': [mt5.TIMEFRAME_D1,    1, DAY]}

class Timeframe:
    def __init__(self, symbol):
        self.symbol = symbol.upper()
        self.values = TIMEFRAME[self.symbol]

    @property
    def constant(self):
        return self.values[0]

    @property
    def value(self):
        return self.values[1]

    @property
    def unit(self):
        return self.values[2]

    @property
    def isDay(self):
        if self.unit == DAY:
            return True
        else:
            return False

    @property
    def isHour(self):
        if self.unit == HOUR:
            return True
        else:
            return False

    @property
    def isMinute(self):
        if self.unit == MINUTE:
            return True
        else:
            return False

    def deltaTime(self, multiply=1.0):
        if self.unit == MINUTE:
            return TimeUtility.deltaSecond(multiply * self.value * 60)
        elif self.unit == HOUR:
            return TimeUtility.deltaMinute(multiply * self.value * 60)
        elif self.unit == DAY:
            return TimeUtility.deltaHour(multiply * self.value * 24)

    @property
    def symbols(self):
        return list(TIMEFRAME.keys())

    @classmethod
    def timeframes(cls):
        symbols = list(TIMEFRAME.keys())
        l = []
        for symbol in symbols:
            l.append(Timeframe(symbol))
        return l

    @classmethod
    def load(cls, timeframe_constant):
        symbols = list(TIMEFRAME.keys())
        for symbol in symbols:
            v = TIMEFRAME[symbol]
            if v[0] == timeframe_constant:
                return Timeframe(symbol)
        return None