# -*- coding: utf-8 -*-

import pandas as pd
from carp import db
from carp.util import log
from carp import util
from carp import trade_calendar



class StocksBasic(object):

    __COLLECTION_NAME = 'stock_basic'
    __KEY_NAME = 'basic'

    def __init__(self, composite):
        self.__composite = composite
        self.collection = self.__composite.mongodb.get_table(self.__COLLECTION_NAME)
        self.__df = None
        self.__date = None
        self.__load_complete = False


    def __reindex(self, df):
        if 'code' in df.keys():
            df.set_index('code', inplace = True)
        else:
            raise ValueError('code column isn\'t in basic dataframe')


    def __load(self, force = False):
        if self.__df is None and not self.__load_complete or force == True:
            cursor = self.collection.find_one({ '_id' : StocksBasic.__KEY_NAME})
            if cursor is not None:
                self.__date = cursor['date']
                self.__df = pd.DataFrame(list(cursor['stocks']))
                self.__reindex(self.__df)
            self.__load_complete = True


    def save(self, df):
        df['code'] = df['code'].astype(str).str.zfill(6)
        data = util.to_json(df)
        last = trade_calendar.last_trade_date(util.FREQ_DAY)
        db_format = {'_id': 'basic', 'date': last.datetime(), 'stocks': data}
        self.collection.replace_one({'_id': 'basic'}, db_format, upsert=True)
        self.__load(force = True)

    def info(self, symbol):
        self.__load()
        if self.__df is None:
            return None
        return self.__df.loc[symbol]

    def newest(self):
        dbdate = self.date()
        if dbdate is None or self.__df is None:
            return False
        else:
            return trade_calendar.compare_v2(dbdate, trade_calendar.last_trade_date(util.FREQ_DAY), freq = util.FREQ_DAY) == 0


    def df(self):
        self.__load()
        return self.__df

    def date(self):
        self.__load()
        if self.__date is None:
            return None
        return trade_calendar.create_v2(self.__date, util.FREQ_DAY)


    def count(self):
        self.__load()
        if self.__df is None:
            return 0
        return len(self.__df.index)

    def clear(self):
        self.collection.drop()


class Composite(object):
    __DATABASE_NAME = 'composite'
    mongodb = db.create_db(__DATABASE_NAME)

    def __init__(self):
        self.basic = StocksBasic(self)


__composite = Composite()

def get_basic():
    return __composite.basic

def update_stocks(df):
    last = __composite.basic.date()

    if not isinstance(df, pd.DataFrame) or df.empty:
        log.warn('stocks list is empty')
        return
    if last is None or not __composite.basic.newest() or __composite.basic.count() == 0:
        ## update
        __composite.basic.save(df)


