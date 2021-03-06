# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import os
import sys
current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.append('../model')
sys.path.append('../mt5api')
sys.path.append('../db')
sys.path.append('../utillity')
sys.path.append('../setting')
import glob

import logging
import time
import pandas as pd
from MT5Bind import MT5Bind
from PriceDatabase import PriceDatabase, ManageTable, CandleTable, TickTable
from TimeUtility import TimeUtility
from Timeframe import Timeframe, HOUR, MINUTE, DAY
from Setting import Setting
from threading import Thread
from Schedular import Schedular

logging.basicConfig(level=logging.DEBUG, filename="debug.log", format="%(asctime)s %(levelname)-7s %(message)s")
logger = logging.getLogger("logger")

class XMHandler:
    def __init__(self):
        pass

    def buildManageTable(self):
        manage = ManageTable()
        db = PriceDatabase()
        return db.create(manage)

    def buildCandleTable(self, stock, timeframe: Timeframe):
        table = CandleTable(stock, timeframe)
        db = PriceDatabase()
        return db.create(table)

    def buildTickTable(self, stock):
        table = TickTable(stock)
        db = PriceDatabase()
        return db.create(table)

    def update(self, stock, timeframe: Timeframe, data):
        # remove last data
        data = data[:len(data) - 1]
        for d in data:
            logger.debug('update() ... ' + stock + '-' + timeframe.symbol + ': ' + str(d[0]))
        table = CandleTable(stock, timeframe)
        db = PriceDatabase()
        ret = db.insert(table, data)
        if ret == False:
            print('DB Insert error1')
            return -1
        stored = db.fetchAllItem(table, 'time')
        times = stored['time']
        tbegin = times[0]
        tend = times[-1]

        (begin, end) = self.rangeOfTime(stock, timeframe)
        manage = ManageTable()
        db = PriceDatabase()
        if begin is None:
            ret = db.insert(manage, [[stock, timeframe.symbol, tbegin, tend]])
        else:
            ret = db.update(manage, [stock, timeframe.symbol, tbegin, tend])
        return len(data)

    def updateTicks(self, stock, data):
        # remove last data
        data = data[:len(data) - 1]
        table = TickTable(stock)
        db = PriceDatabase()
        ret = db.insert(table, data)
        if ret == False:
            print('DB Insert error1')
            return -1
        stored = db.fetchAllItem(table, 'time')
        times = stored['time']
        if len(times) == 0:
            tbegin = None
            tend = None
        else:
            tbegin = times[0]
            tend = times[-1]

        (begin, end) = self.rangeOfTicks(stock)
        manage = ManageTable()
        db = PriceDatabase()
        if begin is None:
            ret = db.insert(manage, [[stock, 'tick', tbegin, tend]])
        else:
            ret = db.update(manage, [stock, 'tick', tbegin, tend])
        return len(data)

    def rangeOfTime(self, stock, timeframe:Timeframe):
        db = PriceDatabase()
        manage = ManageTable()
        item = db.fetchItem(manage, {'stock':stock, 'timeframe':timeframe.symbol})
        if len(item) == 0:
            print('Management DB update Error!')
            return (None, None)
        begin = item['tbegin']
        end = item['tend']
        return (begin, end)

    def rangeOfTicks(self, stock):
        db = PriceDatabase()
        manage = ManageTable()
        item = db.fetchItem(manage, {'stock':stock, 'timeframe':'tick'})
        if len(item) == 0:
            print('Management DB update Error!')
            return (None, None)
        begin = item['tbegin']
        end = item['tend']
        return (begin, end)

# -----------------
# Singleton
handler = XMHandler()
loop = True
# -----------------


def keyOfData(stock, timeframe):
    return stock + '-' + timeframe.symbol

def start():
    stocks = Setting.xm_index() + Setting.xm_fx()
    schedular = Schedular()

    for stock in stocks:
        for timeframe in Timeframe.timeframes():
            schedular.addTask(keyOfData(stock, timeframe), timeframe)

    is_initial = True
    while loop:
        for stock in stocks:
            server = MT5Bind(stock)
            for timeframe in Timeframe.timeframes():
                if is_initial or schedular.shouldDoNow(keyOfData(stock, timeframe)):
                    (tbegin, tend) = handler.rangeOfTime(stock, timeframe)
                    data = server.acquireRange(timeframe, tend, TimeUtility.nowJst())
                    logger.debug(stock + ' ' + timeframe.symbol + 'Download Length: ' + str(len(data)) )
                    if len(data) > 1:
                        handler.update(stock, timeframe, data)
                        print(stock, timeframe.symbol, 'Download done ', len(data))
        is_initial = False

def stop():
    loop = False

# --------------------

def build(stocks):
    is_first = True
    for stock in stocks:
        if is_first:
            handler.buildManageTable()
            print('Manage Table build')
            is_first = False
        handler.buildTickTable(stock)
        print(stock + ': Tick Table build')
        for timeframe in Timeframe.timeframes():
            handler.buildCandleTable(stock, timeframe)
            print(stock + ': ' + timeframe.symbol + ' Table build')

def buildTest():
    is_first = True
    timeframes = [Timeframe('M1')]
    for stock in ['US30Cash']:
        if is_first:
            handler.buildManageTable()
            print('Manage Table build')
            is_first = False
        handler.buildTickTable(stock)
        print(stock + ': Tick Table build')
        for timeframe in timeframes:
            handler.buildCandleTable(stock, timeframe)
            print(stock + ': ' + timeframe.symbol + ' Table build')


def firstUpdate(stocks, size=99999):
    for stock in stocks:
        server = MT5Bind(stock)
        for timeframe in Timeframe.timeframes():
            (begin, end) = handler.rangeOfTime(stock, timeframe)
            data = server.acquire(timeframe, size=size)
            if len(data) <= 1:
                print('Error No Data', stock, timeframe.symbol)
                continue

            handler.update(stock, timeframe, data)
            begin, end = handler.rangeOfTime(stock, timeframe)
            print('Done... legth: ', len(data), stock, timeframe.symbol, begin, end)
            logger.debug('firstUpdate() ... ' + stock + '-' + timeframe.symbol + ' begin: ' + str(begin) + ' end: ' + str(end))


def updateTicks(stock, repeat=100000):
    server = MT5Bind(stock)
    tbegin, tend = handler.rangeOfTicks(stock)
    if tend is None:
        t = TimeUtility.jstTime(2018, 1, 1, 0, 0)
    else:
        t = tend

    nothing = 0
    for i in range(repeat):
        data = server.acquireTicks(t, size=20000)
        if len(data) > 1:
            handler.updateTicks(stock, data)
            print(stock, str(TimeUtility.nowJst()), 'Tick Download done ', i, len(data), data[0], data[-1])
            logger.debug('updateTicks() ... ' + stock + ': ' + str(i) + ' Length:' + str(len(data)) + '...' + str(data[0]) + '-' + str(data[-1]))
            tbegin, tend = handler.rangeOfTicks(stock)
            t = tend
            nothing = 0
        else:
            t += TimeUtility.deltaHour(1)
            nothing += 1
            if nothing > 10 * 24:
                break

def downloadTickData(save_dir, stock, year, month, day):
    filepath = save_dir + stock + '_Tick_' + str(year).zfill(4) + '-' + str(month).zfill(2) + '-' + str(day).zfill(2) + '.csv'
    if os.path.isfile(filepath):
        return
    server = MT5Bind(stock)
    t_from = TimeUtility.jstTime(year, month, day, 0, 0)
    t_to = t_from + TimeUtility.deltaDay(1) #TimeUtility.jstTime(year, month, day, 23, 59)
    data = server.acquireTicksRange(t_from, t_to)
    if len(data) > 0:
        df = pd.DataFrame(data=data, columns=['Time', 'Bid', 'Ask', 'Mid', 'Volume'])
        df.to_csv(filepath, index=False)

def deleteLastFile(dir_path):
    l = glob.glob(dir_path)
    if len(l) > 0:
        file = l[-1]
        os.remove(file)
        print('Delete File ...' + file)
        return file
    else:
        return None
    
def taskDownloadTick(stock):
    root = 'd://tick_data/' + stock + '/'
    try:
        os.mkdir(root)    
    except:
        print('!')
    for year in range(2016, 2021):
        dir_path = root + str(year).zfill(4) + '/'
        try:
            os.mkdir(dir_path)    
        except:
            deleteLastFile(dir_path + '/*.csv')
            
        
        for month in range(1, 13):
            for day in range(1, 32):
                try:
                    t = TimeUtility.jstTime(year, month, day, 0, 0)
                    downloadTickData(dir_path, stock, year, month, day)
                except:
                    continue
        print('Done ' + stock + '...' + str(year))
    
def test1():
    stock = 'US30Cash'

    timeframe = Timeframe('M1')
    (begin, end) = handler.rangeOfTime(stock, timeframe)
    #t0 = end + timeframe.deltaTime
    #t1 = TimeUtility.nowJst() - TimeUtility.deltaMinute(1)
    server = MT5Bind(stock)
    data = server.acquire(timeframe, size=500)
    if len(data) <= 1:
        return len(data) - 1
    handler.update(stock, timeframe, data)
    begin, end = handler.rangeOfTime(stock, timeframe)
    print('Done...', stock, timeframe, begin, end)
            
def test2():
    stock = 'US30Cash'
    timeframe = Timeframe('M1')
    (tbegin, tend) = handler.rangeOfTime(stock, timeframe)
    server = MT5Bind(stock)
    now = TimeUtility.nowJst()
    data = server.acquireRange(timeframe, tend, now)
    if len(data) == 0:
        return -1

    if len(data) == 1:
        return 0

    handler.update(stock, timeframe, data)
    begin, end = handler.rangeOfTime(stock, timeframe)
    print('Done...', stock, timeframe, begin, end)

def test3():
    stock = 'US30Cash'
    (tbegin, tend) = handler.rangeOfTicks(stock)
    tfrom = TimeUtility.nowJst()
    server = MT5Bind(stock)
    data = server.acquireTicks(tfrom, size=1000)
    if len(data) <= 1:
        return len(data) - 1

    handler.updateTicks(stock, data)
    begin, end = handler.rangeOfTicks(stock)
    print('Done...', stock, begin, end)

def test4():
    stock = 'US30Cash'
    (tbegin, tend) = handler.rangeOfTicks(stock)
    now = TimeUtility.nowJst()
    server = MT5Bind(stock)
    data = server.acquireTicks(now, size = 10)
    if len(data) <= 1:
        return len(data) - 1

    handler.updateTicks(stock, data)
    begin, end = handler.rangeOfTicks(stock)
    print('Done...', stock, begin, end)

def save(stock, timeframe):
    server = MT5Bind(stock)
    dic = server.scrapeWithDic(timeframe)
    values = dic['data']
    d = []
    for value in values:
        d.append([value['time'], value['open'], value['high'], value['low'], value['close']])
    df = pd.DataFrame(data=d, columns=['Time', 'Open', 'High', 'Low', 'Close'])
    df.to_csv('./' + stock + '_' + timeframe + '.csv', index=False)
    

def ticksThread():
    thread1 = Thread(target=updateTicks)
    thread1.start()
    thread1.join()
        
if __name__ == '__main__':
    #stocks = Setting.xm_index() + Setting.xm_fx()
    #build(stocks)        # Build Tables
    #firstUpdate(stocks)  # Initial Data save to table
    
    #ticksThread()
    #"['US500Cash', 'CHI50Cash', 'GER30Cash', 'USDJPY', 'AUDJPYmicro', 'EURUSD', 'GBPUSD']
    for stock in ['USDJPY', 'AUDJPYmicro', 'EURUSD', 'GBPUSD']:
        taskDownloadTick(stock)
    



        
