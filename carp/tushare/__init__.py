# -*- coding: utf-8 -*-


import pandas as pd
from .api import tushare_bar, tushare_get_stock_basic, tushare_get_profit, \
        tushare_get_today_bar


#Index: 3504 entries, 000882 to 300504
#Data columns (total 22 columns):
#area                3504 non-null object
#bvps                3504 non-null float64
#esp                 3504 non-null float64
#fixedAssets         3504 non-null float64
#gpr                 3504 non-null float64
#holders             3504 non-null float64
#industry            3504 non-null object
#liquidAssets        3504 non-null float64
#name                3504 non-null object
#npr                 3504 non-null float64
#outstanding         3504 non-null float64
#pb                  3504 non-null float64
#pe                  3504 non-null float64
#perundp             3504 non-null float64
#profit              3504 non-null float64
#reserved            3504 non-null float64
#reservedPerShare    3504 non-null float64
#rev                 3504 non-null float64
#timeToMarket        3504 non-null int64
#totalAssets         3504 non-null float64
#totals              3504 non-null float64
#undp                3504 non-null float64
#dtypes: float64(18), int64(1), object(3)
#memory usage: 629.6+ KB

#None
#area                         深圳
#bvps                      10.54
#esp                       1.005
#fixedAssets              685739
#gpr                       31.73
#holders                  208975
#industry                   全国地产
#liquidAssets        8.99749e+07
#name                       万 科Ａ
#npr                        9.47
#outstanding               97.09
#pb                          3.1
#pe                        24.36
#perundp                    5.76
#profit                    34.23
#reserved                 905916
#reservedPerShare           0.82
#rev                        0.04
#timeToMarket           19910129
#totalAssets         1.01838e+08
#totals                   110.39
#undp                6.35703e+06
#Name: 000002, dtype: object

'''
         name industry area       pe  outstanding  totals  totalAssets
code
000908   景峰医药     化学制药   湖南    39.15         5.51    8.80    519593.91
600354   敦煌种业      种植业   甘肃    26.84         4.48    5.28    270930.78
000852   石化机械     化工机械   湖北   639.86         5.98    5.98    699699.06
600929   湖南盐业       食品   湖南    59.91         1.50    9.18    310491.63
000019   深深宝Ａ      软饮料   深圳     0.00         4.16    4.97    113233.67
002848   高斯贝尔     通信设备   湖南   264.21         0.83    1.67    130605.22
'''

def get_stock_basic():
    df = tushare_get_stock_basic()
    if not df.empty:
        df.reset_index(inplace = True)
        ## timeToMarket is 0 means come in to the market right now
        df = df[df['timeToMarket'] != 0]
        ##  use str instead of datetime to save db
        df['timeToMarket'] = pd.to_datetime(df['timeToMarket'].astype(str), format='%Y%m%d').astype(str)
    return df


'''
tushare daily dataframe

              code   open  close   high    low        vol        amount  tor  vr
datetime
2018-02-02  000001  13.91  14.05  14.10  13.63  1176512.0  1.637620e+09
2018-02-01  000001  13.95  14.03  14.30  13.84  2005614.0  2.821584e+09
'''


def get_stock_bar(code, start, end, freq, **kwargs):
    df = tushare_bar(code, start, end, freq, **kwargs)
    if not df.empty:
        # 1. reset datatime index
        # 2. covert DatetimeIndex to str to save to db
        df.reset_index(inplace = True)
        df['datetime'] = df['datetime'].dt.strftime('%Y-%m-%d')
        df.drop(['code', 'vr'], axis=1, inplace = True)
    return df


'''
        code   name  changepercent   trade    open    high     low  \
0     603999   读者传媒         -1.250    7.11    7.20    7.25    7.10
1     603998   方盛制药          0.000    9.43    0.00    0.00    0.00

      settlement       volume  turnoverratio       amount       per       pb  \
0           7.20    1769143.0        0.76786   12702769.0    24.517    2.455
1           9.43          0.0        0.00000          0.0    58.938    4.066
'''


def get_today_all():
    df = tushare_get_today_bar()
    if not df.empty:
        df.reset_index(inplace = True)
    return df










