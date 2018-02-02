# -*- coding: utf-8 -*-
import os
import pandas as pd
import datetime



OUTPUT_FILE = 'test.h5'

def covert_origin_calendar_tocsv(filename):
    df = pd.read_csv(filename)
    print(df.keys())
    df['date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
    df.set_index('date', inplace = True)
    df.drop(['trade_date'], axis=1, inplace=True)
    df.to_csv(filename)

def convert_min(df, freq):
    input_data = []
    #for index in df.set_index(pd.to_datetime(df['date'])).index:
    for index in pd.to_datetime(df['date']):
        tmp = pd.date_range(index, periods=2, freq = 'D')
        df1 = pd.DataFrame({'istradeday':pd.Series(['T', 'T'], index = tmp)})
        df1 = df1.asfreq('%dmin' % freq, 'ffill')
        x1 = df1.ix[df1.index.indexer_between_time(datetime.time(9, 30), datetime.time(11, 30))]
        x2 = df1.ix[df1.index.indexer_between_time(datetime.time(13), datetime.time(15))]

        input_data.append(x1)
        input_data.append(x2)

    dfmin = pd.concat(input_data)
    dfmin.index.names=['date']
    dfmin.reset_index(inplace = True)
    dfmin.drop(['istradeday'], axis = 1, inplace = True)
    return dfmin


def covert_csv_to_hdf_day(df):
    print('.....covert to daily df')
    df.drop(['istradeday'], axis=1, inplace = True)
    print(df)
    print(df.info())
    df.to_hdf(OUTPUT_FILE, key = 'FREQ_DAY', format='table', data_columns = ['date'])
    print('.....covert to day df end')


def covert_csv_to_hdf_min(df, freq):
    df1 = convert_min(df, freq)
    print('.....covert to %d df' % freq)
    print(df1)
    print(df1.info())
    df1.to_hdf(OUTPUT_FILE, key = 'FREQ_%dMIN' % freq, format='table', data_columns = ['date'])
    print('.....covert to %d df end' % freq)


def covert_csv_to_hdf_week(df):
    print('.....covert to weekly df')
    out = []
    week_num = None
    last = None
    for index in pd.to_datetime(df['date']):
        d1 = index.to_pydatetime()
        if week_num is None:
            week_num = d1.isocalendar()[1]

        if d1.isocalendar()[1] == week_num:
            last = d1
        else:
            week_num = d1.isocalendar()[1]
            if len(out) == 0 or last != out[-1]:
                out.append(last)

    df1 = pd.DataFrame({'date': out})
    print(df1)
    df1.to_hdf(OUTPUT_FILE, key = 'FREQ_WEEK', format='table', data_columns = ['date'])



if __name__ == "__main__":
    filename = 'calendar.csv'
    if os.path.exists(filename):
        df = pd.read_csv(filename)
        covert_csv_to_hdf_day(df)
        covert_csv_to_hdf_min(df, 60)
        covert_csv_to_hdf_min(df, 30)
        covert_csv_to_hdf_min(df, 15)
        covert_csv_to_hdf_min(df, 5)
        covert_csv_to_hdf_min(df, 1)
        covert_csv_to_hdf_week(df)
    else:
        print('%s isn\'t exist' % filename)
