# -*- coding: utf-8 -*-
import tushare as ts
from matplotlib.pylab import date2num
from matplotlib.lines import Line2D
from matplotlib.ticker import MultipleLocator, FuncFormatter
import matplotlib.finance as mpf
import matplotlib.patches
import logging
import os
import json
import datetime


import numpy as np
import matplotlib.pyplot as plt


def download_share_data(_code, _start, _end):
    json_file = './' + _code + '_' + _start + '_' + _end + '.json'
    if not os.path.exists(json_file):
        dataframe = ts.get_hist_data(_code, start=_start, end=_end, ktype='D')
        # save to json
        dataframe.to_json(json_file, orient='index')
    with open(json_file) as f:
        return json.load(f)


def candlestick_ohlc_own(ax, quotes, width=0.6,  colorup='#f10d39', colordown='#3bc068'):
    OFFSET = width / 2.0
    lines = []
    patches = []
    up = False
    for i, q in enumerate(quotes):
        open, high, low, close = q[1:5]
        #i = i + i * width
        if close >= open:
            up = True
            color = colorup
            lower = open
            height = close - open

            high_line_y1 = low
            high_line_y2 = open

            low_line_y1 = high
            low_line_y2 = close
        else:
            up = False
            color = colordown
            lower = close
            height = open - close
            high_line_y1 = low
            high_line_y2 = close

            low_line_y1 = high
            low_line_y2 = open

        rect = matplotlib.patches.Rectangle(
            xy=(i - OFFSET, lower),
            width=width,
            height=height,
            facecolor=('none' if (up is True) else color),
            edgecolor=('none' if (low is True) else color),
        )

        hline = Line2D(
            xdata=(i, i), ydata=(high_line_y1, high_line_y2),
            color=color,
            linewidth=0.5,
            antialiased=True,
        )
        lline = Line2D(
            xdata=(i, i), ydata=(low_line_y1, low_line_y2),
            color=color,
            linewidth=0.5,
            antialiased=True,
        )

        ax.add_line(hline)
        patches.append(rect)
        ax.add_line(lline)
        ax.add_patch(rect)
    ax.autoscale_view()
    return patches


def wash_with_sort(data):
    keys = data.keys()
    keys = sorted(keys, key=lambda d: date2num(
        datetime.datetime.strptime(d, '%Y-%m-%d')))
    for k in keys[:]:
        t = date2num(datetime.datetime.strptime(k, '%Y-%m-%d'))
        v = data[k]
        if (v['price_change'] == 0 and v['p_change'] == 0):
            keys.remove(k)
            del data[k]
    return keys, data


def covert2ohlc(keys, data):
    data_list = []
    for k in keys:
        v = data[k]
        datas = (k, v['open'], v['high'], v['low'], v['close'])
        data_list.append(datas)
    return data_list


def get_ma1(keys, data):
    return [data[k]['ma5'] for k in keys]


def get_ma2(keys, data):
    return [data[k]['ma10'] for k in keys]


def getmacd(sort_key, data, low=26, fast=12, nema=9):
    deas = []
    difs = []
    macds = []
    prev_ema12 = 0
    prev_ema26 = 0
    prev_dea = 0
    for i, key in enumerate(sort_key):
        if i == 0:
            ema12 = float(data[key]['close'])
            ema26 = float(data[key]['close'])
        else:
            ema12 = prev_ema12 * 11 / 13 + float(data[key]['close']) * 2 / 13
            ema26 = prev_ema26 * 25 / 27 + float(data[key]['close']) * 2 / 27
        dif = ema12 - ema26
        dea = prev_dea * 8 / 10 + dif * 2 / 10
        macd = 2 * (dif - dea)

        prev_ema12 = ema12
        prev_ema26 = ema26
        prev_dea = dea

        deas.append(dea)
        difs.append(dif)
        macds.append(macd)

    # EMA（12）= 前一日EMA（12）×11/13＋今日收盘价×2/13
    # EMA（26）= 前一日EMA（26）×25/27＋今日收盘价×2/27
    # DIFF=今日EMA（12）- 今日EMA（26）
    # DEA（MACD）= 前一日DEA×8/10＋今日DIF×2/10
    # BAR=2×(DIFF－DEA)
    return deas, difs, macds


'''
def test_chan(sort_key, data):

    _sort_key = []
    _data = []
    #origin_high = data[sort_key[0]]['high']
    #origin_low = data[sort_key[0]]['low']

    prev_high = data[sort_key[0]]['high']
    prev_low = data[sort_key[0]]['low']

    _sort_key.append(sort_key[0])
    _data.append(data[sort_key[0]])

    #_sort_key.append(sort_key[1])
    #_data.append(data[sort_key[1]])
    for i, k in enumerate(sort_key[:]):
        if i > 0 and i < len(sort_key) - 1:
            next_high = data[sort_key[i + 1]]['high']
            next_low = data[sort_key[i + 1]]['low']
            high = data[k]['high']
            low = data[k]['low']
            if high >= next_high and low <= next_low:
                # open , high, low, close
                if prev_high > high:
                    open = (high > next_high) if next_high else high
                    close = (low > next_low) if low else next_low
                else:
                    open = (high > next_high) if high else next_high
                    close = (low > next_low) if next_low else low
                _sort_key.append(k)
                _data.append((k, open, open, close, close))
            else:
                _sort_key.append(k)
                _data.append(data[k])
    return _sort_key, _data
'''


def main():
    code = '000002'
    FACECOLOR = '#1e2127'
    jsondata = download_share_data(code, '2017-05-08', '2017-11-12')
    sort_keys, data = wash_with_sort(jsondata)
    ohlc = covert2ohlc(sort_keys, data)
    fig = plt.figure(facecolor=FACECOLOR, figsize=(12, 7))
    ax1 = plt.subplot2grid((5, 3), (0, 0), rowspan=4,
                           colspan=4, axisbg=FACECOLOR)
    candlestick_ohlc_own(ax1, ohlc)
    ax1.plot(get_ma1(sort_keys, data))
    ax1.plot(get_ma2(sort_keys, data))
    ax1.grid(True)
    ax2 = plt.subplot2grid((5, 3), (4, 0), rowspan=1,
                           colspan=4, axisbg=FACECOLOR)

    deas, difs, macds = getmacd(sort_keys, data)
    ax2.plot(deas)
    ax2.plot(difs)
    ax2.fill_between([i for i in range(0, len(sort_keys))],
                     macds, facecolor='#00ffe8', interpolate=True)

    plt.show()


if __name__ == '__main__':
    main()
