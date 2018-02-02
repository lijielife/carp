# -*- coding: utf-8 -*-

import os
from carp import util


class GlobalConfig(object):

    DEBUG = False
    DEBUG_PATH = 'debug'

    if not os.path.exists(DEBUG_PATH):
        os.makedirs(DEBUG_PATH)

    CACHE_PATH = os.path.join('cache')
    if not os.path.exists(CACHE_PATH):
        os.makedirs(CACHE_PATH)

    DATABASE_ADDR = 'localhost'
    DATABASE_PORT = 27017


class KlineConfig(object):

    SYNC_FREQS = [
                util.FREQ_DAY,
                util.FREQ_WEEK,
            ]

