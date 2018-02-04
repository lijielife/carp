# -*- coding: utf-8 -*-
import time
from .api import singleton, logger, TradeCalendar, JsonLoader
from .context import get_all_symbols, create_stock
from .render import WebPage, StockBarRender
from .stock import FREQ_WEEK


class Finder(object):
    def __init__(self, name, symbols=[]):
        self.name = name
        self.symbols = symbols
        self.start, self.end = TradeCalendar.duration(_days = -365)

    def set_finder(self, func):
        self.func = func

    def set_duration(self, start, end):
        self.start = start
        self.end = end


@singleton
class FinderEngine(object):

    def __init__(self):
        self.finders = {}

    def create_finder(self, name, symbols=None):
        if symbols is None:
            symbols = get_all_symbols().copy()
        finder = Finder(name, symbols)
        self.finders[name] = finder
        return finder


    def run(self, f):
        logger.info('begin to run %s' % (f.name))
        result = self.__finder_run(f)
        self.__report_result(f, result)
        logger.info('run %s end' % (f.name))

    def __report_result(self, f_obj, result):
        page = WebPage(f_obj.name)
        for stock in result:
            b = stock.bar(start = f_obj.start, end= f_obj.end, freq=FREQ_WEEK)
            if b is not None:
                page.add(StockBarRender(b))
        page.show()
        #l = JsonLoader.create('result.json')
        #l.put(**{'result': result})

    def __finder_run(self, finder):
        ret = []
        for symbol in finder.symbols:
            _stock = create_stock(symbol)
            logger.info('Finder in %s[%s]' % (_stock.name, _stock.symbol))
            if finder.func(_stock, start=finder.start, end=finder.end):
                logger.info("=============!!!!!!!!!!!!!!!!!!!!!!=============")
                logger.info('%s[%s] is great' % (_stock.name, _stock.symbol))
                logger.info("=============!!!!!!!!!!!!!!!!!!!!!!=============")
                ret.append(_stock)
            else:
                _stock.release()
        return ret

    def reset(self):
        self.finder = {}


if __name__ == '__main__':

    engine = FinderEngine()
    finder = engine.create_finder('test')

    def test_finder(stock, **kwargs):
        print('finder stock %s[%s]' % (stock.name, stock.symbol))
    finder.set_finder(test_finder)
    engine.run(finder)
