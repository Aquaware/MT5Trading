# -*- coding: utf-8 -*-
import os
import sys
current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append('../model')
sys.path.append('../setting')
sys.path.append('../utility')

import pandas as pd
import pytz
from Postgres import Postgres, Structure
from Setting import Setting
from Timeseries import Timeseries, OHLC, OHLCV
from Timeframe import Timeframe
from TimeUtility import TimeUtility
from datetime import datetime

TIME = 'time'
OPEN = 'open'
HIGH = 'high'
LOW = 'low'
CLOSE = 'close'
VOLUME = 'volume'
SPREAD = 'spread'
BID = 'bid'
ASK = 'ask'
MID = 'mid'

STOCK = 'stock'
TIMEFRAME = 'timeframe'
TBEGIN = 'tbegin'
TEND = 'tend'


TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
MANAGE_TABLE_NAME = 'manage'

def ManageTable(name=MANAGE_TABLE_NAME):
    struct = {STOCK:'varchar(30)', TIMEFRAME:'varchar(10)', TBEGIN:'timestamp', TEND:'timestamp'}
    table = Structure(name, [STOCK, TIMEFRAME], struct)
    return table

def CandleTable(stock, timeframe:Timeframe):
    name = stock + '_' + timeframe.symbol
    struct = {TIME: 'timestamp', OPEN:'real', HIGH:'real', LOW:'real', CLOSE:'real', VOLUME:'real'
              }
    table = Structure(name, [TIME], struct)
    return table

def TickTable(stock):
    name = stock + '_tick'
    struct = {TIME: 'timestamp', BID:'real', ASK:'real', MID:'real', VOLUME:'real'}
    table = Structure(name, [TIME], struct)
    return table

class PriceDatabase(Postgres):
    
    def __init__(self):
        super().__init__(Setting.db_name(), Setting.db_user(), Setting.db_password(), Setting.db_port())
        pass
    
    def fetchItem(self, table, where=None):
        array = self.fetch(table, where)
        return self.value2dic(table, array)

    def fetchAllItem(self, table, asc_order_column):
        array = self.fetchAll(table, asc_order_column)
        return self.values2dic(table, array)

    def value2dic(self, table, values):
        dic = {}
        if len(values) == 0:
            return dic
        for (column, value) in zip(table.all_columns, values[0]):
            if table.typeOf(column) == 'timestamp':
                t1 = value.astimezone(pytz.timezone('Asia/Tokyo'))
                dic[column] = t1
            else:
                dic[column] = value
        return dic
    
    def values2dic(self, table, values):
        dic = {}
        for i in range(len(table.columns)):
            column = table.columns[i]
            d = []
            for v in values:
                d.append(v[i])
            if table.typeOf(column) == 'timestamp':
                dic[column] = self.time2pyTime(d)
            else:
                dic[column] = d
        return dic
    
    def dataTimeRange(self, stock, timeframe:Timeframe):
        table = ManageTable()
        where = {STOCK:stock, TIMEFRAME:timeframe.symbol}
        dic = self.fetchItem(table, where=where)
        return (dic[TBEGIN], dic[TEND])
        
    def priceRange(self, stock, timeframe:Timeframe, begin_time, end_time):
        table = CandleTable(stock, timeframe)
        if begin_time is not None:
            where1 = TIME + " >= cast('" + str(begin_time)+ "' as timestamp) "
        else:
            where1 = ''
        if end_time is not None:
            where2 = TIME + " <= cast('" + str(end_time) + "' as timestamp) "
        else:
            where2 = ''
        if begin_time is not None and end_time is not None:
            where = where1 + ' AND ' + where2
        else:
            where = where1 + where2
            
        items = self.fetchItemsWhere(table, where, TIME)
        times = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []
        values = []
        for item in items:
            t = item[0]
            times.append(t)
            opens.append(item[1])
            highs.append(item[2])
            lows.append(item[3])
            closes.append(item[4])
            volumes.append(item[5])
            values.append([t] + item[1:6])
        dic = {}
        dic[TIME] = times
        dic[OPEN] = opens
        dic[HIGH] = highs
        dic[LOW] = lows
        dic[CLOSE] = closes
        dic[VOLUME] = volumes
        return (dic, values)
    
    def time2pyTime(self, time_list):
        time = []
        for t in time_list:
            t1 = t.astimezone(pytz.timezone('Asia/Tokyo'))
            time.append(t1)
        return time

    def saveToCsv(self, table, filepath, is_candle):
        data = self.fetchAllItem(table, TIME)
        d = []
        if is_candle:
            t = data[TIME]
            o = data[OPEN]
            h = data[HIGH]
            l = data[LOW]
            c = data[CLOSE]
            v = data[VOLUME]
            s = data[SPREAD]
            for tt, oo, hh, ll, cc, vv, ss in zip(t, o, h, l, c, v, s):
                d.append([tt.strftime('%Y-%m-%d %H:%M:%S'), oo, hh, ll, cc, vv, ss])
            columns = [TIME, OPEN, HIGH, LOW, CLOSE, VOLUME, SPREAD]
        else:
            t = data[TIME]
            a = data[ASK]
            b = data[BID]
            m = data[MID]
            v = data[VOLUME]
            for tt, aa, bb, mm, vv in zip(t, a, b, m, v):
                d.append([tt.strftime('%Y-%m-%d %H:%M:%S'), aa, bb, mm, vv])
            columns = [TIME, BID, ASK, MID, VOLUME]

        df = pd.DataFrame(data=d, columns=columns)
        df.to_csv(filepath, index=False)

# ----
def test1():
    stock = 'US30Cash'
    timeframe = 'M1'
    db = PriceDatabase()
    table = CandleTable(stock, timeframe)
    db.saveToCsv(table, './db.csv', True)
    
def test2():
    stock = 'US30Cash'
    timeframe = Timeframe('M1')
    db = PriceDatabase()
    t0 = TimeUtility.jstTime(2020, 9, 1, 21, 30)
    t1 = TimeUtility.jstTime(2020, 10, 13, 5, 30)
    dic, values = db.priceRange(stock, timeframe, t0, t1)
    print(dic)
    df = pd.DataFrame(data=values, columns=dic.keys())
    df.to_csv('./data.csv', index=False)
    
if __name__ == '__main__':
    test2()    
