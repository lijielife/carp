# -*- coding: utf-8 -*-
import sys
sys.path.append("../src/")
import api
import datetime
import logging
import time

__api = api.Api()
logging.basicConfig(level=logging.DEBUG)
__log = logging.getLogger("DEBUG")


UP_FORMAT = "\033[0;31m%f\033[0m"
DOWN_FORMAT = "\033[0;32m%f\033[0m"
DEFAULT_FORMAT = "\033[0;37m%f\033[0m"

last = 0

def on_quote(k, v):
    global last
    update = True
    price = v['last']
    if (last < v['last']):
        price = UP_FORMAT % v['last']
    elif (last > v['last']):
        price = DOWN_FORMAT % v['last']
    else:
        update = False
    last = v['last']
    if update is True:
        print(price)
        print(v['time'])
        print(v)

__api.subscribe('000001.SH', on_quote, "")
time.sleep(3000000000)
