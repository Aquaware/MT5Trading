from datetime import datetime, timedelta, timezone
import calendar
import pytz
import pandas as pd

TIMEZONE_TOKYO = pytz.timezone('Asia/Tokyo')
class TimeUtility:

    @classmethod
    def dayOfLastSunday(cls, year, month):
        '''dow: Monday(0) - Sunday(6)'''
        dow = 6
        n = calendar.monthrange(year, month)[1]
        l = range(n - 6, n + 1)
        w = calendar.weekday(year, month, l[0])
        w_l = [i % 7 for i in range(w, w + 7)]
        return l[w_l.index(dow)]

    @classmethod
    def nowUtc(cls):
        now = datetime.now(pytz.timezone('UTC'))
        return now

    @classmethod
    def nowXm(cls):
        now = cls.nowUtc()
        zone = cls.xmTimezone(now)
        return datetime.now(zone)

    @classmethod
    def nowJst(cls):
        now = datetime.now(TIMEZONE_TOKYO)
        return now

    @classmethod
    def toJstTimezone(cls, time):
        return time.astimezone(TIMEZONE_TOKYO)

    @classmethod
    def toXmTimezone(cls, naive_time):
        zone = cls.xmTimezone(naive_time)
        return naive_time.astimezone(zone)

    @classmethod
    def toUtcTimezone(cls, year, month, day, hour, minute):
        local = datetime(year, month, day, hour, minute)
        return pytz.timezone('UTC').localize(local)

    @classmethod
    def jstTime(cls, year, month, day, hour, minute):
        t = datetime(year, month, day, hour, minute, tzinfo=TIMEZONE_TOKYO)
        return t

    @classmethod
    def xmTime(cls, year, month, day, hour, minute):
        local = datetime(year, month, day, hour, minute)
        timezone = cls.xmTimezone(local)
        t = datetime(year, month, day, hour, minute, tzinfo=timezone)
        return t

    @classmethod
    def utcTime(cls, year, month, day, hour, minute):
        t = datetime(year, month, day, hour, minute, tzinfo=pytz.timezone('UTC'))
        return t

    @classmethod
    def jst2seasonalAwaretime(cls, naive_jst):
        timezone = pytz.timezone("Etc/UTC")
        # create 'datetime' objects in UTC time zone to avoid the implementation of a local time zone offset
        if cls.isXmSummerTime(naive_jst):
            delta = cls.deltaHour(6)
        else:
            delta = cls.deltaHour(7)
        t = datetime(naive_jst.year, naive_jst.month, naive_jst.day, naive_jst.hour, naive_jst.minute, 0, tzinfo=timezone) - delta
        return t

    @classmethod
    def timestamp2jstmsec(cls, timestamp):
        t1 = pd.to_datetime(timestamp, unit='ms')
        t2 = t1.to_pydatetime()
        zone = timezone(timedelta(hours=9), name='XM')
        t3 = t2.astimezone(zone)
        t4 = cls.xm2jst(t3)
        return t4

    @classmethod
    def timestamp2jst(cls, timestamp):
        t1 = datetime.utcfromtimestamp(timestamp)
        zone = timezone(timedelta(hours=9), name='XM')
        t2 = t1.astimezone(zone)
        t3 = cls.xm2jst(t2)
        return t3

    @classmethod
    def isXmSummerTime(cls, naive_time):
        t = cls.utcTime(naive_time.year, naive_time.month, naive_time.day, naive_time.hour, naive_time.minute)
        day0 = cls.dayOfLastSunday(naive_time.year, 3)
        tsummer0 = cls.utcTime(naive_time.year, 3, day0, 0, 0)
        day1 = cls.dayOfLastSunday(naive_time.year, 10)
        tsummer1 = cls.utcTime(naive_time.year, 10, day1, 0, 0)
        if t > tsummer0 and t < tsummer1:
            return True
        else:
            return False

    @classmethod
    def xmTimezone(cls, naive_time):
        if cls.isXmSummerTime(naive_time):
            # summer time
            h = 3
        else:
            h = 2
        return timezone(timedelta(hours=h), name='XM')

    @classmethod
    def xm2jst(cls, aware_time):
        if cls.isXmSummerTime(aware_time):
            t = aware_time + cls.deltaHour(6)
        else:
            t = aware_time + cls.deltaHour(7)
        return cls.toJstTimezone(t)

    @classmethod
    def jst2xm(cls, naive_time):
        if cls.isXmSummerTime(naive_time):
            t = naive_time - cls.deltaHour(6)
        else:
            t = naive_time - cls.deltaHour(7)
        return cls.toXmTimezone(t)

    @classmethod
    def deltaDay(cls, days):
        return timedelta(days=days)

    @classmethod
    def deltaHour(cls, hours):
        return timedelta(hours=hours)

    @classmethod
    def deltaMinute(cls, minutes):
        return timedelta(minutes=minutes)

    @classmethod
    def deltaSecond(cls, seconds):
        return timedelta(seconds=seconds)

    @classmethod
    def time2str(cls, time):
        s = str(time.year) + '/' + str(time.month) + '/' + str(time.day)
        s += ' ' + str(time.hour) + ':' + str(time.minute) + ':' + str(time.second)
        return s


# -----
def test():
    jst_naive = datetime(2020, 9, 21, 11, 29)
    print('naive', jst_naive)
    print('now... JST:', TimeUtility.nowJst(), 'XM:', TimeUtility.nowXm(), 'GMT:', TimeUtility.nowUtc())
    utc_aware = TimeUtility.jst2seasonalAwaretime(jst_naive)
    print('utc_aware', utc_aware)

if __name__ == "__main__":
    test()