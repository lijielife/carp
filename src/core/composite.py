# -*- coding: utf-8 -*-
import os
import pandas as pd
from .api import TradeCalendar, singleton, Api, InstrumentinfoDataFrame, JsonLoader,LOCAL_CACHE_PATH


@singleton
class Composite(object):
    __api = Api()
    __filename = "symbols.json"

    def __init__(self):
        self.symbols_info = self.__get_infos()

    def get_info(self, symbol):
        return self.symbols_info.index(symbol)

    def __get_infos(self):
        filename = os.path.join(LOCAL_CACHE_PATH, self.__filename)
        loader = JsonLoader.create(filename)

        def load_func(key, data):
            if data is None:
                symbols_info = self.__api.instrumentinfo()
                loader.put(infos=symbols_info.to_json(orient='index'))
                return symbols_info
            else:
                return InstrumentinfoDataFrame(pd.read_json(data, orient='index'))

        return loader.get('infos', load_callback=load_func)

    def get_all_symbols(self, left=True, **kwargs):
        return self.symbols_info.index().values


if __name__ == "__main__":
    c = Composite()
    print(c.get_info('300737.SZ'))
