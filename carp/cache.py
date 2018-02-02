#!/usr/bin/env python
# -*- coding: utf-8 -*-


import redis
import pandas as pd
from carp.util import log

__CACHE_DICT = {}



def cache_create(name):
    if name in __CACHE_DICT.keys():
        return __CACHE_DICT[name]
    else:
        __CACHE_DICT[name] = Cache(name)
        return __CACHE_DICT[name]

class Cache(object):
    # TODO connection pool
    # TODO config dir
    def __init__(self, name):
        self.__name = name
        self.__redis = None

    def __open(self):
        if self.__redis is None:
            self.__redis = redis.StrictRedis(host='localhost', port=6379, db=0)
        return self.__redis.ping()

    def get_connection(self):
        return self.__redis

    def save(self):
        if self.__open():
            self.get_connection().save()

    def dfset(self, key, df):
        if self.__open():
            self.get_connection().set(key, df.to_msgpack(compress='zlib'))


    def dfget(self, key):
        if self.__open() == False:
            return []
        conn = self.get_connection()
        if conn.exists(key):
            return pd.read_msgpack(conn.get(key))
        else:
            log.error('%s isn\'t exit' % key)
            return []

