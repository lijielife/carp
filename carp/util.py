# -*- coding: utf-8 -*-

import pandas as pd
import tushare as ts
import arrow
import json
import logging
import sys

FREQ_1M = '1M'
FREQ_5M = '5M'
FREQ_15M = '15M'
FREQ_30M = '30M'
FREQ_DAY = 'DAY'
FREQ_WEEK = 'WEEK'
FREQ_MONTH = 'MONTH'



log = logging.getLogger('carp')
log.setLevel(logging.DEBUG)


__stream_handler = logging.StreamHandler()
__stream_handler.setLevel(logging.DEBUG)

__flile_handler = logging.FileHandler('logging.log')
__flile_handler.setLevel(logging.DEBUG)

__formatter = logging.Formatter('%(asctime)s (%(process)d) [%(levelname)s]%(filename)s:%(lineno)d:  %(message)s')

__stream_handler.setFormatter(__formatter)
__flile_handler.setFormatter(__formatter)

log.addHandler(__stream_handler)
log.addHandler(__flile_handler)


def to_json(df, orient = 'records'):
    return json.loads(df.to_json(orient = orient))

