# -*- coding: utf-8 -*-
import time
from core.api import singleton, logger, TradeCalendar
import core.stock
import core.context as context
import core.util


class Finder(object):
    def __init__(self, name, symbols=[]):
        self.name = name
        self.symbols = symbols
        self.start = TradeCalendar.day(_days=-365)
        self.end = TradeCalendar.day()

    def set_finder(self, func):
        self.func = func

    def set_duration(self, start, end):
        self.start = start
        self.end = end


@singleton
class FinderEngine(object):

    def __init__(self):
        self.finders = {}
        self.symbol = []

    def create_finder(self, name, symbols=None):
        if symbols is None:
            symbols = self.symbol.copy() if len(self.symbol) > 0 else context.get_all_symbols().copy()
        finder = Finder(name, symbols)
        self.finders[name] = finder
        return finder

    def init_symbols(self, symbols):
        self.symbol = symbols

    def run(self, f):
        logger.info('begin to run %s' % (f.name))
        result = self.__finder_run(f)
        self.__report_result(result)
        logger.info('run %s end' % (f.name))

    def __report_result(self, result):
        logger.info(result)
        l = util.JsonLoader.create('result.json')
        l.put(**{'result': result})
        l.sync()

    def __finder_run(self, finder):
        ret = []
        for symbol in finder.symbols:
            _stock = context.stock(symbol)
            logger.info('Finder in %s[%s]' % (_stock.name, _stock.symbol))
            if finder.func(_stock, start=finder.start, end=finder.end):
                logger.info("=============!!!!!!!!!!!!!!!!!!!!!!=============")
                logger.info('%s[%s] is great' % (_stock.name, _stock.symbol))
                logger.info("=============!!!!!!!!!!!!!!!!!!!!!!=============")
                ret.append(symbol)
            _stock.release()
        return ret

    def reset():
        self.finder = {}
        self.symbol = []


if __name__ == '__main__':

    engine = FinderEngine()
    finder = engine.create_finder('test')

    def test_finder(stock, **kwargs):
        print('finder stock %s[%s]' % (stock.name, stock.symbol))
    finder.set_finder(test_finder)
    engine.run(finder)
