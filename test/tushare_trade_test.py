# -*- coding: utf-8 -*-
import tushare as ts
import logging
from optparse import OptionParser


VERBOSE = False


def log_result(dataframe):
    if dataframe is None or dataframe.empty:
        logging.error('test failed')
    else:
        global VERBOSE
        if VERBOSE:
            logging.info(dataframe)
        logging.info('test success')


def test_get_hist_data():
    dataframe = ts.get_hist_data('sh', '2017-10-11', '2017-11-13', 'D')
    log_result(dataframe)


def test_get_today_all():
    dataframe = ts.get_today_all()
    log_result(dataframe)


def test_get_tick_data():
    # 万科A
    dataframe = ts.get_tick_data('000002', date='2017-11-13')
    log_result(dataframe.head(10))


def test_get_today_ticks():
    dataframe = ts.get_today_ticks('000002')
    log_result(dataframe.head(10))

def test_get_sina_dd():
    dataframe = ts.get_sina_dd('000002', date='2017-11-13', vol=1000)
    log_result(dataframe)

def test_profit_data():
    dataframe = ts.profit_data(top=40, year=2017)
    log_result(dataframe[dataframe.shares >= 8])


def main():
    logging.basicConfig(level=logging.INFO)
    opt = OptionParser()
    opt.add_option('--func', dest='func', type=str,
                   default='all', help='function test')
    opt.add_option('--verbose', dest='verbose',
                   default=False, help='verbose')
    (options, args) = opt.parse_args()
    global VERBOSE
    VERBOSE = options.verbose

    if options.func == 'all':
        #test_get_hist_data()
        #test_get_today_all()
        logging.info('haha')
    else:
        eval('test_' + options.func)()


if __name__ == '__main__':
    main()
