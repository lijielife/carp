# -*- coding: utf-8 -*-

from  .api import Api as DataCoreApi
import pandas as pd

__api = DataCoreApi()



'''
qos_core daily dataframe


    Unnamed: 0       close  code freq        high         low  oi        open  \
0            0  3779.40084     2   1d  3880.37232  3771.73212 NaN  3799.85076
1            1  3826.69128     2   1d  3847.14120  3751.28220 NaN  3779.40084
2            2  3848.41932     2   1d  3881.65044  3815.18820 NaN  3826.69128

    presettle  settle     symbol  trade_date trade_status      turnover  \
0         NaN     NaN  000002.SZ    20171212           交易  1.401463e+09
1         NaN     NaN  000002.SZ    20171213           交易  9.981535e+08
2         NaN     NaN  000002.SZ    20171214           交易  1.150698e+09

        volume     vwap
0   46763892.0  3830.39
1   33495394.0  3808.76
2   38206777.0  3849.40

'''

def __fix_datacore_k_df(df):
    if df.empty:
        return df
    inplace = True
    df.drop(['freq', 'code', 'vwap', 'oi', 'presettle', 'settle', 'trade_status'], axis=1, inplace=inplace)
    df['open'] = df['open'] / 100
    df['close'] = df['close'] / 100
    df['low'] = df['low'] / 100
    df['high'] = df['high'] / 100
    ## save to db use str instead of datetime
    df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').astype(str)
    ## read without any index
    #df.set_index('date', inplace = inplace)
    df.drop(['trade_date'], axis=1, inplace=inplace)
    ## TODO volume and vwap turnover 停牌
    return df


def daily(symbol, start_date, end_date, freq):
    df = __api.daily(symbol = symbol, start_date = start_date, end_date = end_date, freq = freq)
    return __fix_datacore_k_df(df)



