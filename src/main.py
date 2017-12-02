# -*- coding: utf-8 -*-

from core import context, stock, render, api, finder, filter


def stock_finder(stock, **kwargs):
    if stock.isnew():
        return False
    start = kwargs['start']
    end = kwargs['end']
    b = stock.bar(start, end)


def main():
    args = {
        'cache': {
            '1d': 1440,
            '1w': 48 * 4,
        }
    }
    context.sync(**args)
    f = filter.Filter()
    engine = finder.FinderEngine()
    f1 = engine.create_finder('test')
    f1.set_finder(stock_finder)
    engine.run(f1)


if __name__ == "__main__":
    main()
