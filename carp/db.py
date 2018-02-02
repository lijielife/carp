# -*- coding: utf-8 -*-
import os
import pandas as pd
import pymongo
from pymongo import MongoClient
from carp import util
from carp.util import log
from carp import config


class MongoDB(object):

    __MONGO_CLIENT = None

    @classmethod
    def open_or_create_client(cls, force = False):
        if force == True and cls.__MONGO_CLIENT is not None:
            cls.__MONGO_CLIENT.close()
            cls.__MONGO_CLIENT = None

        if cls.__MONGO_CLIENT is None:
            try:
                cls.__MONGO_CLIENT = MongoClient(host = config.GlobalConfig.DATABASE_ADDR, \
                        port = config.GlobalConfig.DATABASE_PORT, serverSelectionTimeoutMS = 1000  * 60)
                cls.__MONGO_CLIENT.server_info()
            except pymongo.errors.ServerSelectionTimeoutError as err:
                raise RuntimeError(err)
        return cls.__MONGO_CLIENT

    @classmethod
    def release(cls):
        if cls.__MONGO_CLIENT is not None:
            cls.__MONGO_CLIENT.close()
            cls.__MONGO_CLIENT = None


    def __init__(self, name):
        self.name = name
        self.db = MongoDB.open_or_create_client()[name]

    def get_table(self, name):
        return self.db[name]

    def get_value_from_key(self, table, key):
        cursor = self.get_cursor_from_key(table, key)
        if cursor is not None:
            return cursor[key]
        else:
            log.warn("no %s in %s", key, table)
            return None

    def get_cursor_from_key(self, table, key):
        exists = {key: {'$exists':True}}
        return self.db[table].find_one(exists)




def create_db(name):
    return MongoDB(name)


class H5Store(object):

    __STORES__ = {}

    @staticmethod
    def initialze(filename, force = False):
        exist = H5Store.__STORES__.get(filename) is not None

        if force and exist:
            H5Store.__STORES__[filename].close()
            H5Store.__STORES__[filename] = None

        if H5Store.__STORES__.get(filename) is None:
            H5Store.__STORES__[filename] = pd.HDFStore(filename, format='table', mode = 'r')

        return H5Store.__STORES__[filename]


    @staticmethod
    def create(filename, force = False):
        return H5Store(filename, force)

    def __init__(self, filename, force = False):
        self.filename = filename
        self.store = H5Store.initialze(filename, force)

    def exist(self):
        return os.path.exists(self.filename)

    def is_open(self):
        return self.store is not None and self.store.is_open

    def close(self):
        self.store.close()

    def release(self):
        if self.store is not None:
            self.store.close()
            self.store = None

    #def clean(self, key = ""):
    #    ## FIXME other way
    #    dummy = pd.DataFrame()
    #    if key == "":
    #        [self.save(k, dummy) for k in self.keys()]
    #    else:
    #        self.save(key, dummy)

    def keys(self):
        return self.store.keys()

    def save(self, key, df, append=True, **kwargs):
        if df is None or not isinstance(df , pd.DataFrame):
            log.error('df unknown type')
            return None
        return self.store.put(key, df, append=append, format='table', data_columns=True, **kwargs)

    def get(self, key):
        return self.store.get(key)

    def select(self, key, **args):
        return self.store.select(key, **args)

    def attribute(self, key, **kwargs):
        meta_info = "meta_info"
        if key in self.keys():
            if kwargs:
                self.store.get_storer(key).attrs.meta_info = kwargs
            else:
                try:
                    dic = self.store.get_storer(key).attrs.meta_info
                    return {} if dic is None else dic
                except KeyError:
                    return {}
        else:
            return {}
