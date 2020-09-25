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

import logging
import time
import pandas as pd
from MT5Bind import MT5Bind
from PriceDatabase import PriceDatabase, ManageTable, CandleTable, TickTable
from TimeUtility import TimeUtility
from Timeframe import Timeframe, HOUR, MINUTE, DAY
from Setting import Setting

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

def start():

    now = TimeUtility.nowJst()
    n = len(Timeframe.timeframes())
    last_update = []
    for i in range(n):
        last_update.append(now)

    while loop:
        for stock in Setting.xm_index():
            server = MT5Bind(stock)
            i = 0
            for timeframe in Timeframe.timeframes():
                (tbegin, tend) = handler.rangeOfTime(stock, timeframe)
                data = server.acquireRange(timeframe, tend, TimeUtility.nowJst())
                if len(data) > 1:
                    handler.update(stock, timeframe, data)
                    print(stock, timeframe.symbol, 'Download done ', len(data))
                    last_update[i] = now

            (tbegin, tend) = handler.rangeOfTicks(stock)
            data = server.acquireTicksRange(tend, TimeUtility.nowJst())
            if len(data) > 1:
                handler.updateTicks(stock, data)
                print(stock, 'Tick', 'Download done ', len(data))
        time.sleep(1)

def stop():
    loop = False

# --------------------

def build():
    is_first = True
    for stock in Setting.xm_index():
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


def firstUpdate(size=99999):
    for stock in Setting.xm_index():
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

        #jst = TimeUtility.nowJst() - TimeUtility.deltaDay(50)
        #data = server.acquireTicks(jst, size=1000000)
        #if len(data) <= 1:
        #    continue
        #handler.updateTicks(stock, data)
        #begin, end = handler.rangeOfTicks(stock)
        #print('Done... legth: ', len(data), stock, begin, end)
        #logger.debug('firstUpdate() ... ' + stock + '-' + 'Tick' + ' begin: ' + str(begin) + ' end: ' + str(end))

def firstTicks():
    for stock in Setting.xm_index():
        server = MT5Bind(stock)

        jst = TimeUtility.jstTime(2018, 1, 1, 0, 0)
        data = server.acquireTicks(jst, size=1000000)
        if len(data) <= 1:
            continue
        handler.updateTicks(stock, data)

        for i in range(50):
            tbegin, tend = handler.rangeOfTicks(stock)
            data = server.acquireTicksRange(tend, TimeUtility.nowJst())
            if len(data) > 1:
                handler.updateTicks(stock, data)
                print(stock, 'Tick', 'Download done ', len(data))
            else:
                break


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
    
    
if __name__ == '__main__':
    build()        # Build Tables
    firstUpdate()  # Initial Data save to table
    firstTicks()
    
    #buildTest()
    #test2()
    #test4()
    #save('US30Cash', 'D1')
    #test4()


        
