# -*- coding: utf-8 -*-
import os
import logging
import simplejson as json
import pandas as pd

LOCAL_CACHE_PATH = '/tmp/carp/.cache'

logging.basicConfig(
    format="%(levelname)s/%(filename)s:[%(module)s.%(funcName)s]>%(lineno)d:  %(message)s")
logger_name = "carp"
logger = logging.getLogger(logger_name)
logger.setLevel(logging.DEBUG)


if not os.path.exists(LOCAL_CACHE_PATH):
    os.makedirs(LOCAL_CACHE_PATH)


class JsonLoader(object):
    @staticmethod
    def create(filename=""):
        with open(filename, 'a+') as f:
            pass
        return JsonLoader(filename)

    def __init__(self, filename):
        self.filename = filename
        try:
            with open(self.filename) as f:
                self.data = json.load(f)
        except ValueError:
            self.data = {}

    def put(self, **args):
        self.data = dict(self.data, **args)

    def get(self, key, load_callback=None):
        data = self.data.get(key)
        if load_callback is not None:
            data = load_callback(key, data)
        else:
            logger.info('load %s from %s success' % (key, self.filename))
        return data

    def sync(self):
        with open(self.filename, 'w+') as f:
            json.dump(self.data, f)


class StockStore(object):
    __CACHE_PATH = os.path.join(LOCAL_CACHE_PATH, 'k')

    if not os.path.exists(__CACHE_PATH):
        os.makedirs(__CACHE_PATH)

    @classmethod
    def get_stock_path(cls):
        return cls.__CACHE_PATH

    def __init__(self, symbol):
        self.symbol = symbol
        self.filename = os.path.join(StockStore.__CACHE_PATH, symbol + '.h5')
        self.store = None

    def is_open(self):
        return self.store is not None and self.store.is_open

    def close(self):
        if self.store is not None:
            self.store.close()

    def __open(self):
        if self.store is None:
            self.store = pd.HDFStore(self.filename, format='table')
        return self.store

    def keys(self):
        self.__open()
        return self.store.keys()

    def save(self, key, df, _append=True, **kwargs):
        self.__open()
        if df is None:
            return
        self.store.put(key, df, append=_append, format='table', data_columns=True, **kwargs)

    def get(self, key):
        self.__open()
        return self.store.get(key)

    def select(self, key, **args):
        self.__open()
        return self.store.select(key, **args)

    def attribute(self, key, **kwargs):
        self.__open()
        meta_info = "meta_info"
        if key in self.keys():
            if kwargs:
                self.store.get_storer(key).attrs[meta_info] = kwargs
            else:
                dic = self.store.get_storer(key).attrs[meta_info]
                return {} if dic is None else dic
        else:
            return {}


if __name__ == "__main__":
    s = StockStore('000001.SZ')
    #l = JsonLoader.create('test.json')
    #arg1 = {'test1' : 555, 'test2' : 666}
    # l.sync()
    #s = StockStore("000001.SZ")
