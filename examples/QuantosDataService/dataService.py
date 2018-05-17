# encoding: UTF-8

import sys
import json
from datetime import datetime, timedelta
from time import time, sleep

from pymongo import MongoClient, ASCENDING

from vnpy.trader.vtObject import VtBarData
from vnpy.trader.app.ctaStrategy.ctaBase import MINUTE_DB_NAME
from vnpy.trader.gateway.tkproGateway.DataApi import DataApi
from multiprocessing.dummy import Pool

# 交易所类型映射
exchangeMap = {}
exchangeMap['CFFEX'] = 'CFE'
exchangeMap['SHFE'] = 'SHF'
exchangeMap['CZCE'] = 'CZC'
exchangeMap['DCE'] = 'DCE'
exchangeMap['SSE'] = 'SH'
exchangeMap['SZSE'] = 'SZ'
exchangeMapReverse = {v: k for k, v in exchangeMap.items()}

# 加载配置
config = open('config.json')
setting = json.load(config)
config.close()

MONGO_HOST = setting['MONGO_HOST']
MONGO_PORT = setting['MONGO_PORT']
SYMBOLS = setting['SYMBOLS']
USERNAME = setting['USERNAME']
TOKEN = setting['TOKEN']
DATA_SERVER = setting['DATA_SERVER']

# 创建API对象
mc = MongoClient(MONGO_HOST, MONGO_PORT)  # Mongo连接
db = mc[MINUTE_DB_NAME]  # 数据库
collections = db.collection_names()


# ----------------------------------------------------------------------
def generateVtBar(row):
    """生成K线"""
    bar = VtBarData()

    symbol, exchange = row['symbol'].split('.')

    bar.symbol = symbol
    bar.exchange = exchangeMapReverse[exchange]
    bar.vtSymbol = '.'.join([bar.symbol, bar.exchange])
    bar.open = row['open']
    bar.high = row['high']
    bar.low = row['low']
    bar.close = row['close']
    bar.volume = row['volume']

    bar.date = str(row['date'])
    bar.time = str(row['time']).rjust(6, '0')

    # 将bar的时间改成提前一分钟
    hour = bar.time[0:2]
    minute = bar.time[2:4]
    sec = bar.time[4:6]
    if minute == "00":
        minute = "59"

        h = int(hour)
        if h == 0:
            h = 24

        hour = str(h - 1).rjust(2, '0')
    else:
        minute = str(int(minute) - 1).rjust(2, '0')
    bar.time = hour + minute + sec

    bar.datetime = datetime.strptime(' '.join([bar.date, bar.time]), '%Y%m%d %H%M%S')

    return bar


# ----------------------------------------------------------------------
def downMinuteBarBySymbol(api, vtSymbol, startDate, endDate=''):
    """下载某一合约的分钟线数据"""
    start = time()
    code, exchange = vtSymbol.split('.')
    print('正在下载合约: {}'.format(code))

    if exchange in ['SSE', 'SZSE']:
        cl = db[vtSymbol]
    else:
        cl = db[code]
    cl.ensure_index([('datetime', ASCENDING)], unique=True)  # 添加索引

    dt = datetime.strptime(startDate, '%Y%m%d')

    if endDate:
        end = datetime.strptime(endDate, '%Y%m%d')
    else:
        end = datetime.now()
    delta = timedelta(1)
    symbol = '.'.join([code, exchangeMap[exchange]])
    latest_day = cl.find().sort('datetime', -1).limit(1)

    if latest_day.count() != 0:
        latest_day = latest_day[0]['datetime']
        latest_day = datetime(latest_day.year, latest_day.month, latest_day.day)
        print("上次合约: {} 更新到了: {}, 正在同步新的内容".format(code, latest_day))
        dt = latest_day

    while dt <= end:
        d = int(dt.strftime('%Y%m%d'))
        df, msg = api.bar(symbol, freq='1M', trade_date=d)
        dt += delta

        if df is None:
            continue

        for ix, row in df.iterrows():
            bar = generateVtBar(row)
            d = bar.__dict__
            flt = {'datetime': bar.datetime}
            cl.replace_one(flt, d, True)
    e = time()
    cost = (e - start) * 1000

    print u'合约%s数据下载完成%s - %s，耗时%s毫秒' % (vtSymbol, startDate, end.strftime('%Y%m%d'), cost)


# ----------------------------------------------------------------------
def downloadAllMinuteBar(api):
    """下载所有配置中的合约的分钟线数据"""
    print '-' * 50
    print u'开始下载合约分钟线数据'
    print '-' * 50

    poll = Pool(10)
    # 添加下载任务
    for symbol in SYMBOLS:
        startDt = symbol.split('.')[0][-4:]
        startDt = datetime(2000 + int(startDt[0:2]) - 1, int(startDt[2:5]), 1)
        startDate = startDt.strftime('%Y%m%d')
        poll.apply_async(downMinuteBarBySymbol, (api, str(symbol), startDate))
        print('')
        sleep(0.2)
    poll.close()
    poll.join()
    print '-' * 50
    print u'合约分钟线数据下载完成'
    print '-' * 50
