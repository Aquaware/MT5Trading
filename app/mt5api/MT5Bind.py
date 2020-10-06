# -*- coding: utf-8 -*-
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../model'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../setting'))

import pandas as pd
import MetaTrader5 as mt5
from datetime import datetime
import pytz
from Timeframe import Timeframe
from Timeseries import Timeseries
from TimeUtility import TimeUtility
from Timeframe import DAY, HOUR, MINUTE
from Timeseries import OHLCV, OHLC
from Setting import Setting


class MT5Bind:
    def __init__(self, stock):
        self.stock = stock
        if not mt5.initialize():
            print("initialize() failed")
            mt5.shutdown()
        #print('Version: ', mt5.version())
        pass    
    
    def close(self):
        mt5.shutdown()
        pass
    def convert2Array(self, data):
        out = []
        if data is None:
            return []
        for d in data:
            value = list(d)
            time = TimeUtility.timestamp2jst(value[0])
            out.append([time] + value[1:5] + [float(value[5])])
        return out

    def convert2ArrayTick(self, data):
        out = []
        if data is None:
            return []
        for d in data:
            value = list(d)
            time = TimeUtility.timestamp2jstmsec(value[5])
            out.append([time, value[1], value[2], (float(value[1]) + float(value[2])) / 2.0, 0])
        return out
        
    def roundMinute(self, time, timeframe:Timeframe):
        t = TimeUtility.xmTime(time.year, time.month, time.day, time.hour, time.minute)
        t += TimeUtility.deltaMinute(timeframe.value - 1)
        minute = int(t.minute / timeframe.value) * timeframe.value
        t1 = TimeUtility.xmTime(t.year, t.month, t.day, t.hour, minute)
        return t1
     
    def acquire(self, timeframe:Timeframe, size=99999):
        d = mt5.copy_rates_from_pos(self.stock, timeframe.constant , 0, size)
        data = self.convert2Array(d)
        return data
    
    def acquireWithTimeSeries(self, timeframe:Timeframe, size=99999):
        d = mt5.copy_rates_from_pos(self.stock, timeframe.constant , 0, size)
        data = self.convert2Array(d)
        return self.toTimeSeries(data)
    
    def acquireWithDic(self, timeframe:Timeframe, size=99999):
        d = mt5.copy_rates_from_pos(self.stock, timeframe.constant , 0, size)
        data = self.convert2Array(d)
        array = self.toDicArray(data)
        dic = {}
        dic['name'] = self.stock
        dic['timeframe'] = timeframe.symbol
        dic['length'] = len(data)
        dic['data'] = array
        return dic
    
    def acquireRange(self, timeframe:Timeframe, begin_aware_jst, end_aware_jst):
        utc_from = TimeUtility.jst2seasonalAwaretime(begin_aware_jst)
        utc_to = TimeUtility.jst2seasonalAwaretime(end_aware_jst)
        d = mt5.copy_rates_range(self.stock, timeframe.constant , utc_from, utc_to)
        data = self.convert2Array(d)
        return data
    
    def acquireTicks(self, from_jst, size=100000):
        utc_from = TimeUtility.jst2seasonalAwaretime(from_jst)
        d = mt5.copy_ticks_from(self.stock, utc_from, size, mt5.COPY_TICKS_ALL)
        data = self.convert2ArrayTick(d)
        return data

    def acquireTicksRange(self, from_jst, to_jst):
        utc_from = TimeUtility.jst2seasonalAwaretime(from_jst)
        utc_to = TimeUtility.jst2seasonalAwaretime(to_jst)
        d = mt5.copy_ticks_range(self.stock, utc_from, utc_to, mt5.COPY_TICKS_ALL)
        data = self.convert2ArrayTick(d)
        return data

    def toTimeSeries(self, data, data_type=OHLC):
        time = []
        values = []
        for v in data:
            time.append(v[0])
            if data_type == OHLCV:
                values.append(v[1:6])
            elif data_type == OHLC:
                values.append(v[1:5])
        return Timeseries(time, values, names=data_type)

    def toDicArray(self, data, data_type=OHLCV):
        array = []
        for v in data:
            dic = {}
            dic['time'] = TimeUtility.time2str(v[0])
            for i in range(len(data_type)):
                name = data_type[i]
                dic[name] = v[i + 1]
            array.append(dic)
        return array

    def toDi2(self, data, data_type=OHLC):
        time = []
        dic = {}
        for v in data:
            time.append(TimeUtility.time2str(v[0]))
        dic['time'] = time
        for i in range(len(data_type)):
            values = []
            for v in data:
                values.append(v[i + 1])
            dic[data_type[i]] = values
        return dic
    
# -----
    
def test0():
    if not mt5.initialize():
        print("initialize() failed")
        mt5.shutdown()
    print('Version:', mt5.version())    

    t1 = TimeUtility.xmTime(2020, 9, 18, 15, 12)
    t0 = t1 - TimeUtility.deltaMinute(5)
    values = mt5.copy_rates_range("US30Cash", mt5.TIMEFRAME_M1, t0, t1)
    for value in values:
        t = pd.to_datetime(value[0], unit='s')
        naive_time = t.to_pydatetime()
        print(t, naive_time, TimeUtility.toXm(naive_time), value)
    mt5.shutdown()
    pass

def test():
    # connect to MetaTrader 5
    if not mt5.initialize():
        print("initialize() failed")
        mt5.shutdown()
    print('Version:', mt5.version())
    #dji = mt5.copy_rates_range('US30Cash', mt5.TIMEFRAME_M30, Now() - DeltaDay(2), Now())
    #print(dji)
 
    # request 1000 ticks from EURAUD
    euraud_ticks = mt5.copy_ticks_from("US30Cash", datetime(2020,4,17,23), 1000, mt5.COPY_TICKS_ALL)
    # request ticks from AUDUSD within 2019.04.01 13:00 - 2019.04.02 13:00
    audusd_ticks = mt5.copy_ticks_range("AUDUSD", datetime(2020,1,27,13), datetime(2020,1,28,13), mt5.COPY_TICKS_ALL)
 
    # get bars from different symbols in a number of ways
    eurusd_rates = mt5.copy_rates_from("EURUSD", mt5.TIMEFRAME_M1, datetime(2020,1,28,13), 1000)
    eurgbp_rates = mt5.copy_rates_from_pos("EURGBP", mt5.TIMEFRAME_M1, 0, 1000)
    eurcad_rates = mt5.copy_rates_range("EURCAD", mt5.TIMEFRAME_M1, datetime(2020,1,27,13), datetime(2020,1,28,13))
    #print(eurusd_rates)
    # shut down connection to MetaTrader 5
    mt5.shutdown()
    return

def test1():
    market = 'US30Cash'
    tf = 'M1'
    server = MT5Bind(market)
    timeframe = Timeframe('M1')
    t0 = TimeUtility.jstTime(2020, 5, 1, 0, 0)
    t1 = TimeUtility.jstTime(2020, 7, 1, 0, 0)
    t0 = server.roundMinute(t0, timeframe)
    t1 = server.roundMinute(t1, timeframe)
    print(t0, t1)
    data = server.acquireRange(timeframe, t0, t1)
    print('length: ', len(data), data[0])
    server.close()
    if len(data) > 1:
        df = pd.DataFrame(data=data, columns=['time', 'open', 'high', 'low', 'close', 'volume'])
        df.to_csv('./' + market + '_' + tf + '.csv')
    
def test2():
    now = TimeUtility.nowUtc()
    jst = TimeUtility.toJstTimezone(now)
    xm = TimeUtility.toXmTimezone(now)
    print('JST', jst)
    print('XM', TimeUtility.nowXm())
    print('JST2', TimeUtility.toJst(now))
    jst = TimeUtility.jstTime(2020, 4, 8, 22, 13)
    print('XM2', TimeUtility.toXm(jst))
    
def test3(stock, timeframe_str, size):
    server = MT5Bind(stock)
    d = server.acquire(Timeframe(timeframe_str), size=size)
    if len(d) > 0:
        print(stock, timeframe_str, 'Done: ', len(d), d[0], d[-1])
    else:
        print(stock, timeframe_str, '... No Data')

def test4(stock, size):
    server = MT5Bind(stock)
    jst = TimeUtility.jstTime(2020, 1, 1, 0, 0)
    d = server.acquireTicks(jst, size=size)
    print(stock, 'Done: ', len(d), d[0], d[-1])
    #df = pd.DataFrame(data = d, columns=['Time', 'Bid', 'Ask', 'value1', 'value2', 'volume'])
    #df.to_csv('./US30_tick.csv', index=False)


    
if __name__ == "__main__":
    for currency in Setting.xm_fx():
        test3(currency, 'M1', 100)