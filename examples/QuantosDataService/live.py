# coding=utf-8
import json
import multiprocessing.dummy as mp
import time
import traceback
import random
from flask import Flask, request, abort
from gevent.pywsgi import WSGIServer
from geventwebsocket import WebSocketError
from geventwebsocket.handler import WebSocketHandler
import Queue
import dataService
from vnpy.trader.gateway.tkproGateway.DataApi import DataApi

pool = mp.Pool(8)

config = open('config.json')
setting = json.load(config)
config.close()

DATA_SERVER = setting['DATA_SERVER']
USERNAME = setting['USERNAME']
TOKEN = setting['TOKEN']

api = DataApi(DATA_SERVER)
info, msg = api.login(USERNAME, TOKEN)

if not info:
    print u'数据服务器登录失败，原因：%s' % msg

app = Flask(__name__)

client_list = []


@app.route('/')
def echo():
    if request.environ.get('wsgi.websocket'):
        ws = request.environ['wsgi.websocket']
        print("新的连接加入")
        client_list.append(ws)
        if ws is None:
            abort(404)
        else:
            while True:
                if not ws.closed:
                    message = ws.receive()
                    ws.send(message)


def send_message(clent, message):
    try:
        clent.send(message)
    except WebSocketError:
        client_list.remove(clent)
    except Exception, e:
        traceback.print_exc(e)


def on_quote(k, v):
    bar = dataService.generateVtBar(v)
    d = bar.__dict__
    d['datetime'] = bar.datetime.strftime("%Y-%m-%d-%H")
    message = json.dumps(d)

    for clent in client_list:
        send_message(clent, message)


df, msg = api.bar("rb1901.SHF, hc1901.SHF", freq='1M', trade_date=20180523)


def send_mook_data():
    count_df = len(df)
    while True:
        on_quote(None, df.iloc[random.randrange(0, count_df)])
        time.sleep(random.randint(1, 10) / 10)


if __name__ == '__main__':
    http_server = WSGIServer(('', 5000), app, handler_class=WebSocketHandler)
    pool.apply_async(send_mook_data)
    # code, msg = api.subscribe('hc1810.SHF, rb1810.SHF', func=on_quote)
    http_server.serve_forever()

    # code, msg = api.subscribe('hc1810.SHF, rb1810.SHF', func=on_quote)
