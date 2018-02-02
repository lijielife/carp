# -*- coding: utf-8 -*-
import os
import datetime
import arrow
import functools
import pandas as pd
import numpy as np
from jaqs.data import DataApi
from urllib.parse import urlencode
from carp.util import log

try:
    import simplejson as json
except ImportError:
    import json

#__CARP_ROOT_PATH = '/tmp'
#
#LOCAL_CACHE_PATH = os.path.join(__CARP_ROOT_PATH, 'carp')
#LOCAL_STORE_PATH = os.path.join(LOCAL_CACHE_PATH, 'k')
#
#if not os.path.exists(LOCAL_STORE_PATH):
#    os.makedirs(LOCAL_STORE_PATH)
#

#class JsonLoader(object):
#    @staticmethod
#    def create(filename=""):
#        with open(filename, 'a+') as f:
#            pass
#        return JsonLoader(filename)
#
#    def __init__(self, filename):
#        self.filename = filename
#        try:
#            with open(self.filename) as f:
#                self.data = json.load(f)
#        except ValueError:
#            self.data = {}
#
#    def put(self, **args):
#        for key in args:
#            if isinstance(args[key], BaseDataFrame):
#                self.data = dict(self.data, **{key:args[key].to_json(orient='index')})
#            else:
#                self.data = dict(self.data, **{key:args[key]})
#        self.sync()
#
#    def get(self, key, load_callback=None):
#        data = self.data.get(key)
#        if load_callback is not None:
#            data = load_callback(key, data)
#        else:
#            log.info('load %s from %s success' % (key, self.filename))
#        return data
#
#    def sync(self):
#        with open(self.filename, 'w+') as f:
#            json.dump(self.data, f)
#
#
#class StockStore(object):
#
#    def __init__(self, symbol):
#        self.symbol = symbol
#        self.filename = os.path.join(LOCAL_STORE_PATH, symbol + '.h5')
#        self.store = None
#
#    def is_open(self):
#        return self.store is not None and self.store.is_open
#
#    def close(self):
#        if self.store is not None:
#            self.store.close()
#
#    def __open(self):
#        if self.store is None:
#            self.store = pd.HDFStore(self.filename, format='table')
#        return self.store
#
#    def keys(self):
#        self.__open()
#        return self.store.keys()
#
#    def save(self, key, df, _append=True, **kwargs):
#        self.__open()
#        if df is None:
#            return
#        self.store.put(key, df.df(), append=_append, format='table', data_columns=True, **kwargs)
#
#    def get(self, key):
#        self.__open()
#        return self.store.get(key)
#
#    def select(self, key, **args):
#        self.__open()
#        return self.store.select(key, **args)
#
#    def attribute(self, key, **kwargs):
#        self.__open()
#        meta_info = "meta_info"
#        if key in self.keys():
#            if kwargs:
#                self.store.get_storer(key).attrs[meta_info] = kwargs
#            else:
#                try:
#                    dic = self.store.get_storer(key).attrs[meta_info]
#                    return {} if dic is None else dic
#                except KeyError:
#                    return {}
#        else:
#            return {}


#class BaseDataFrame(object):
#    def __init__(self, df):
#        self.__df = df
#
#    def set_index(self, key, inplace=True):
#        if self.__df is None:
#            log.error('df is none')
#        elif isinstance(self.__df, pd.DataFrame) == False:
#            # for debug
#            if isinstance(self.__df, int):
#                log.error('df is int %d' % self.__df)
#            elif isinstance(self.__df, str):
#                log.error('df is string %s' % self.__df)
#            #raise Exception('df is not DataFrame ' + type(self.__df))
#        elif self.__df.empty:
#            log.warning('df is empty')
#        elif key in self.__df.keys():
#            self.__df.set_index(key, inplace=inplace)
#
#    def to_json(self, **kwargs):
#        return self.__df.to_json(**kwargs)
#
#    def __getitem__(self, key):
#        return self.__df[key]
#
#    def index(self, i=""):
#        return self.__df.index if i == "" else self.__df.loc[i]
#
#    @property
#    def empty(self):
#        if self.__df is None:
#            return True
#        elif isinstance(self.__df, pd.DataFrame) == False:
#            return True
#        else:
#            return self.__df.empty
#
#    def df(self):
#        return self.__df
#
#    @staticmethod
#    def format_fields(*fields):
#        return ','.join(fields)
#
#
#class InstrumentinfoDataFrame(BaseDataFrame):
#    INST_TYPE = 'inst_type'  # 证券类别
#    MARKET = 'market'  # 交易所代码
#    SYMBOL = 'symbol'  # 证券代码
#    NAME = 'name'  # 证券名称
#    LIST_DATE = 'list_date'  # 上市日期
#    DELIST_DATE = 'delist_date'  # 退市日期
#    CNSPELL = 'cnspell'  # 拼音简写
#    CURRENCY = 'currency'  # 交易货币
#    STATUS = 'status'  # 上市状态,1:上市 3：退市 8：暂停上市
#    BUYLOT = 'bylot'  # INT 最小买入单位
#    SELLLOT = 'selllot'  # INT 最大买入单位
#    PRICETICK = 'pricetick'  # double 最小变动单位
#    PRODUCT = 'product'  # 合约品种
#    UNDERLYING = 'underlying'  # 对应标的
#    MULTIPLIER = 'multiplier'  # int 合约乘数
#
#    def __init__(self, df):
#        BaseDataFrame.__init__(self, df)
#        self.set_index(InstrumentinfoDataFrame.SYMBOL)
#
#    @classmethod
#    def fields(self):
#        return BaseDataFrame.format_fields(
#            *[self.STATUS,
#              self.LIST_DATE,
#              self.NAME,
#              self.SYMBOL,
#              self.MARKET])
#
#
#class CecsuspDataFrame(BaseDataFrame):
#    SYMBOL = 'symbol'  # string 证券代码
#    ANN_DATE = 'ann_date'  # string 停牌公告日期
#    SUSP_DATE = 'susp_date'  # 停牌开始日期
#    SUSP_TIME = 'susp_time'  # string 停牌开始时间
#    RESU_DATE = 'resu_date'  # string 复牌日期
#    RESU_TIME = 'resu_time'  # string 复牌时间
#    SUSP_REASON = 'susp_reason'  # string 停牌原因
#
#    def __init__(self, df):
#        BaseDataFrame.__init__(self, df)
#        self.set_index(CecsuspDataFrame.SYMBOL)
#
#
#class SecrestrictedDataFrame(BaseDataFrame):
#    SYMBOL = 'symbol'  # string 证券代码
#    LIST_DATE = 'list_date'  # string 本期解禁流通日期
#    LIFTED_REASON = 'lifted_reason'  # 本期解禁原因（来源）
#    LIFTED_SHARES = 'lifted_shares'  # string 本期解禁数量
#    LIFTED_RATIO = 'lifted_ratio'  # string 可流通占A股总数比例
#
#    def __init__(self, df):
#        BaseDataFrame.__init__(self, df)
#        self.set_index(SecrestrictedDataFrame.SYMBOL)
#
#
#class TradecalDataFrame(BaseDataFrame):
#    TRADE_DATE = 'trade_date'  # int YYYYMMDD格式，如20170823
#    ISTRADEDAY = 'istradeday'  # string 是否交易日
#    ISWEEKDAY = 'isweekday'  # string 是否工作日
#    ISWEEKDAY = 'isweekend'  # string 是否周末
#    ISHOLIDAY = 'isholiday'  # string string 否节假日
#
#    def __init__(self, df):
#        BaseDataFrame.__init__(self, df)
#        self.set_index(TradecalDataFrame.TRADE_DATE)
#
#
#class DailyDataFrame(BaseDataFrame):
#    SYMBOL = 'symbol'
#    CODE = 'code'  # string 交易所原始代码
#    TRADE_DATE = 'trade_date'  # int YYYYMMDD格式，如20170823
#    OPEN = 'open'  # double 开盘价
#    HIGH = 'high'  # double 最高价
#    LOW = 'low'  # double 最低价
#    CLOSE = 'close'  # double 收盘价
#    VOLUME = 'volume'  # volume double 成交量
#    TURNOVER = 'turnover'  # turnover double 成交金额
#    VWAP = 'vwap'  # double 成交均价
#    SETTLE = 'settle'  # double 结算价
#    OI = 'oi'  # double 持仓量
#    TRADE_STATUS = 'trade_status'  # string 交易状态（”停牌”或者”交易”）
#    TRADE_SUSPENSION = '停牌'
#    TRADE_TRANSACTION = '交易'
#
#    def __init__(self, df):
#        BaseDataFrame.__init__(self, df)
#        self.set_index(DailyDataFrame.TRADE_DATE)
#
#    @classmethod
#    def fields(self):
#        return BaseDataFrame.format_fields(
#                *[DailyDataFrame.CLOSE,
#                    DailyDataFrame.TRADE_DATE,
#                    DailyDataFrame.OPEN,
#                    DailyDataFrame.HIGH,
#                    DailyDataFrame.LOW,
#                    DailyDataFrame.VOLUME,
#                    DailyDataFrame.TRADE_STATUS])
#
#
#class BarDataFrame(BaseDataFrame):
#    SYMBOL = 'symbol'
#    CODE = 'code'  # string 交易所原始代码
#    DATE = 'date'  # int
#    TIME = 'time'  # int
#    TRADE_DATE = 'trade_date'  # int YYYYMMDD格式，如20170823
#    FREQ = 'freq'  # bar 类型
#    OPEN = 'open'
#    HIGH = 'high'
#    LOW = 'low'
#    CLOSE = 'close'
#    VOLUME = 'volume'
#    TURNOVER = 'turnover'
#    VWAP = 'vwap'
#    OI = 'oi'
#    SETTLE = 'settle'
#
#    def __init__(self, df):
#        BaseDataFrame.__init__(self, df)
#        self.set_index(BarDataFrame.TRADE_DATE)
#

def singleton(cls):
    instance = {}

    def geninstance(*args, **kwargs):
        if cls not in instance:
            instance[cls] = cls(*args, **kwargs)
        return instance[cls]
    return geninstance

@singleton
class Api(object):
    __api = DataApi(addr='tcp://data.tushare.org:8910')

    def __lazy_login(self):
        if self.__login_flag is False:
            ret = self.__api.login(self.id, self.token)
            if ret[0] is None:
                raise Exception('login failed %s - %s' % (self.id, ret))
            else:
                log.info('%s login success' % self.id)
            self.__login_flag = ret

    def __init__(self):
        self.__login_flag = False
        with open('.config.json') as f:
            info = json.load(f)
            self.id = info.get('id')
            self.token = info.get('token')

    # 获取市场股市列表
    def instrumentinfo(self, _fields="", _filter="inst_type=1&status=1&market=SH,SZ"):
        self.__lazy_login()
        df, msg = self.__api.query(view="jz.instrumentInfo", fields=_fields,
                                   filter=_filter,
                                   data_format='pandas')
        log.debug('request jz.instrumentInfo')
        return df

    # 停复牌resu_date 未设置
    # def secsusp(self, _filter="", _fields=""):
    #    self.__lazy_login()
    #    df, msg = self.__api.query(
    #        view="lb.secSusp",
    #        fields=_fields,
    #        filter=_filter,
    #        data_format='pandas')
    #    log.debug('request lb.secSusp')
    #    return CecsuspDataFrame(df)

    # 获取限售股解禁列表
    # TODO
    #def secrestricted(self, _start_date, _end_date, _fields=""):
    #    self.__lazy_login()
    #    filters = urlencode({'start_date': TradeCalendar.date2int(_start_date),
    #                         'end_date': TradeCalendar.date2int(_end_date)})
    #    df, msg = self.__api.query(
    #        view="lb.secRestricted",
    #        fields=_fields,
    #        filter=filters,
    #        data_format='pandas')
    #    log.debug('request lb.secRestricted')
    #    return df

    # 日交易行情
    def daily(self, symbol, start_date, end_date, freq='1d', fields= "", adjust_mode='post'):
        self.__lazy_login()
        df, msg = self.__api.daily(
            symbol=symbol,
            freq=freq,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            adjust_mode=adjust_mode)
        log.debug('request daily %s' % msg)
        return df

    # 交易日历
    def tradecal(self, _fields="trade_date, istradeday"):
        self.__lazy_login()
        df, msg = self.__api.query(
            view="jz.secTradeCal",
            fields=_fields)
        log.debug('request jz.secTradeCal')
        return df

    # 订阅
    def subscribe(self, _symbol, _func, _fields):
        self.__lazy_login()
        sublist, msg = self.__api.subscribe(_symbol, func=_func,
                                            fields=_fields)
        log.debug('request subscribe')
        return sublist

    # bar
    def bar(self, _symbol, _trade_date, _freq="5M", _fields=""):
        self.__lazy_login()
        df, msg = self.__api.bar(symbol=_symbol,
                                 trade_date=_trade_date,
                                 freq=_freq,
                                 fields=_fields)
        log.debug('request bar')
        return df

    # industry failed
    # def industry(self, **industrys):
    #    df, msg = self.__api.query(
    #            view="lb.secIndustry",
    #            fields="",
    #            #filter="industry1_name=金融&industry2_name=金融&industry_src=中证",
    #            #filter="symbol=000001.SH&industry_src=中证",
    #            data_format='pandas')
    #    return df

    # indicator failed
    # def indicator(self):
    #    df, msg = self.__api.query(view="wd.secDailyIndicator",
    #            filter='symbol=000001.SZ&start_date=20170605&end_date=20170701',
    #            fields="",
    #            data_format='pandas')
    #    return df

    # indexinfo failed
    # def indexInfo(self, _symbol, _fields = ""):
    #    df, msg = self.__api.query(
    #            view="lb.indexInfo",
    #            fields=_fields,
    #            filter="",
    #            #filter="index_code=399001",
    #            #filter="symbol=000001.SH",
    #            data_format='pandas')
    #    return df
    #    #return BarDataFrame(df)

    # @staticmethod
    # def format_filter(**filters):
    #     return urlencode(filters)
