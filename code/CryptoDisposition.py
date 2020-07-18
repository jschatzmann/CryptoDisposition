# %% 
#1##################################################################################################
# import transaction file from AIT (tx to exchanges = sells)
####################################################################################################
import numpy as np
import pandas as pd

#strPath = '../data/testfiles/' # test file path
#strFileName = 'txs_to_exchanges_dryrun.csv' # test tx file

strPath = '/Volumes/SAMSUNG/data_btc_ex/' # full tx file
strFileName = 'txs_to_exchanges.csv' # full tx file

strPathFile = strPath + strFileName
df = pd.read_csv(strPathFile)
df


# %%
#2##################################################################################################
# aggregate tx information, sum up values per tx and the number of tx taken place in this block / tmstp
####################################################################################################

# sum up tx amount (containing multiple addresses) per block / timestamp
dfTxPerBlock = df.groupby(
    ['tx_hash', 'height', 'timestamp']).agg(
        valSum = ('value', 'sum')
    ).reset_index()
dfTxPerBlock

# count tx and sum values per tx
dfTxPerBlock1 = dfTxPerBlock.groupby(
    ['height', 'timestamp']).agg(
        valSum = ('valSum', 'sum'),
        txCnt = ('tx_hash','count')
    ).reset_index()
dfTxPerBlock1

# group level based on timestamp (without height, including orig tx mid-hour timestamp)
dfTx = dfTxPerBlock1.groupby(
    ['timestamp']
    ).agg(
        valSum = ('valSum','sum'),
        txCnt = ('txCnt', 'sum')
    ).reset_index()

dfTx = dfTx.rename(columns = {"timestamp": "timestampTx"})
dfTx

# %%
#3##################################################################################################
# read in OHLC data from kaiko covering multiple exchanges, calculate average price references
####################################################################################################

#strPathOhlc = '../data/testfiles/' # testfile relative to script
#strFnOhlc = 'all_btcusd_ohlcv_1h_ex_dryrun.csv'

strPathOhlc = '../data/_ex/' # full file
strFnOhlc = 'all_btcusd_ohlcv_1h_ex.csv'

strPathFileOhlc = strPathOhlc + strFnOhlc
dfOhlc = pd.read_csv(strPathFileOhlc)

# calculate averaged prices over all provided exchanges
dfOhlc1 = dfOhlc.groupby(
     ['timestamp']
 ).agg(
     avg_open = ('open','mean'),
     avg_high = ('high','mean'),
     avg_low = ('low','mean'),
     avg_close = ('close','mean'),
     avg_volume = ('volume','mean'),
 ).reset_index()

# convert 13 digits timestamp to 10 digits to enable dataframe join
dfOhlc1 = dfOhlc1.rename(columns = {"timestamp": "timestamp_long"})
dfOhlc1['timestampOhlc'] = dfOhlc1['timestamp_long'].floordiv(1000) # "remove" last 3 digits
first_col = dfOhlc1.pop('timestampOhlc')
dfOhlc1.insert(0, 'timestampOhlc', first_col) # insert timestamp at df start (readability)
dfOhlc1 = dfOhlc1.drop(['timestamp_long'], 1) # drop old 13 digits timestamp

dfOhlc1

# %%
#4##################################################################################################
# join OHLC kaiko hourly data with the aggregated transactions per timestamp / block
# shift mid-hour transactions to full hour (e.g. 01:02:00 to 02:00:00) to join with OHLC
####################################################################################################

def round_unix_date(dt_series, seconds=60, up=True): # function to round up to full hour (to enable join)
    return dt_series // seconds * seconds + seconds * up

dfTx['tmstTxRnd'] = round_unix_date(dfTx['timestampTx'], 60*60) #60sec = 1min *60 = 60 minutes *60 = 1 hour   

# if a tx timestamp is EXACTLY at a full hour, don't round up to full next hour but keep the current value
# if modulo of timestampTx is 0 - then value is a full hour -> keep value and use in tmstTxRnd (for df join, matchin OHLC and tx)
dfTx.loc[dfTx['timestampTx'] % 3600 == 0, 'tmstTxRnd'] = dfTx['timestampTx']

# group by full hour (tmstTxRnd) and sum up tx amount (valSum) and txCnt
dfTx1 = dfTx.groupby(
    ['tmstTxRnd']
    ).agg(
        valSum = ('valSum','sum'),
        txCnt = ('txCnt', 'sum')
    ).reset_index()

# merge the OHLC kaiko hourly exchange data with the tx data (aggregated on hourly level)
dfMerged = pd.merge(dfOhlc1, dfTx1, 
    how = 'left', 
    left_on = 'timestampOhlc', 
    right_on = 'tmstTxRnd', 
    suffixes=('_tx', '_ohlc'),
    indicator=True)

# add readable timestamp column for testing purposes / validation
#dfMerged['tmstTx_rdbl'] = pd.to_datetime(dfMerged['timestampTx'], unit = 's')
dfMerged['tmstOhlc_rdbl'] = pd.to_datetime(dfMerged['timestampOhlc'], unit = 's')
dfMerged['tmstTxRnd_rdbl'] = pd.to_datetime(dfMerged['tmstTxRnd'], unit = 's')
dfMerged

# testing of examples if match works
#dfMerged.loc[dfMerged['timestampOhlc'] == 1517446800]
#dfMerged.loc[dfMerged['_merge'] == 'both']
#dfOhlc1.loc[dfOhlc1['timestampOhlc'] == 1520049600]

#dfMerged.to_excel(r'/Users/jes/OneDrive - FH JOANNEUM/06 Data/kaiko-ohlcv-1h-year/_dfMerged_export.xlsx', index = False)


# %% 
#5##################################################################################################
# define functions to limit dataframes and calculate t-statistics
####################################################################################################
from datetime import datetime
from dateutil import tz

# functions to limit df to certain time window
def dfMerged_per_year(df, year): # return df reduced to passed year
    # datetime(year, month, day, hour, minute, second, microsecond)
    tmstp_start = datetime(year, 1, 1, 0, 0, 0, 0, tzinfo = tz.UTC)
    tmstp_end = datetime(year, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)
    df = df[df['timestampOhlc'] >= int(tmstp_start.timestamp())]
    df = df[df['timestampOhlc']<= int(tmstp_end.timestamp())]
    return df

def dfMerged_per_timeframe(df, dt_start, dt_end): # return df reduced to passed start / end datetime
    # datetime(year, month, day, hour, minute, second, microsecond)
    df = df[df['timestampOhlc'] >= int(dt_start.timestamp())]
    df = df[df['timestampOhlc']<= int(dt_end.timestamp())]
    return df

def tstat_for_df(df): # return t-statistics (paired / related samples) for passed dataframe
    from scipy import stats
    return stats.ttest_rel(df['ti_LR'],df['ti_GR'], nan_policy='omit')

def tstat_for_df_colums(dfColLR, dfColGR): # return t-statistics (paired / related samples) for passed columns to compare
    from scipy import stats
    return stats.ttest_rel(dfColLR, dfColGR, nan_policy='omit')

# %% 
#6##################################################################################################
# conduct t-test on GR vs LR based on Odeans approach -> compare open price to average price of the day
####################################################################################################

# update data frame, if average of open + close price is higher than open price = GR (gain realised), if lower = LR (loss realised)
dfMerged['ti_avg_OpenCLose'] = dfMerged[['avg_open', 'avg_close']].mean(axis = 1)
dfMerged.loc[dfMerged['ti_avg_OpenCLose'] > dfMerged['avg_open'], 'ti_GR_LR'] = 'GR' 
dfMerged.loc[dfMerged['ti_GR_LR'] == 'GR', 'ti_GR'] = dfMerged['txCnt'] 
dfMerged.loc[dfMerged['ti_avg_OpenCLose'] < dfMerged['avg_open'], 'ti_GR_LR'] = 'LR' 
dfMerged.loc[dfMerged['ti_GR_LR'] == 'LR', 'ti_LR'] = dfMerged['txCnt'] 

dfMerged['ti_GR'] = dfMerged['ti_GR'].fillna(0)
dfMerged['ti_LR'] = dfMerged['ti_LR'].fillna(0)

tstatAll = tstat_for_df(dfMerged)
dt_start = datetime(2012, 10, 1, 0, 0, 0, 0, tzinfo = tz.UTC) # include last quarter to enable TA calculations for 1.1.2013
dt_end = datetime(2019, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)
dfMerged2013to2019 = dfMerged_per_timeframe(dfMerged, dt_start, dt_end)
tstat2013to2019 = tstat_for_df(dfMerged2013to2019)
tstat2013 = tstat_for_df(dfMerged_per_year(dfMerged, 2013))
tstat2014 = tstat_for_df(dfMerged_per_year(dfMerged, 2014))
tstat2015 = tstat_for_df(dfMerged_per_year(dfMerged, 2015))
tstat2016 = tstat_for_df(dfMerged_per_year(dfMerged, 2016))
tstat2017 = tstat_for_df(dfMerged_per_year(dfMerged, 2017))
tstat2018 = tstat_for_df(dfMerged_per_year(dfMerged, 2018))
tstat2019 = tstat_for_df(dfMerged_per_year(dfMerged, 2019))

print('t-stat for full df: ' + str(tstatAll))
print('t-stat for 2013 to 2019: ' + str(tstat2013to2019))
print('t-stat for 2013: ' + str(tstat2013))
print('t-stat for 2014: ' + str(tstat2014))
print('t-stat for 2015: ' + str(tstat2015))
print('t-stat for 2016: ' + str(tstat2016))
print('t-stat for 2017: ' + str(tstat2017))
print('t-stat for 2018: ' + str(tstat2018))
print('t-stat for 2019: ' + str(tstat2019))

# %%
#6##################################################################################################
# conduct t-test for monthly values
####################################################################################################
from datetime import datetime, timedelta
from dateutil import tz, relativedelta
import calendar
import pandas as pd

# define the dataframe that should be used for TA indicator assignment
dfTa = dfMerged2013to2019

dt_start = datetime(2013, 1, 1, 0, 0, 0, 0, tzinfo = tz.UTC)
dt_end = datetime(2019, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)

def add_months(sourcedate, months): # add month to datetime to efficiently iterate though df and create monthly t-stat values
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    return datetime(year, month, day, 0, 0 , 0, tzinfo = tz.UTC)

#df_tstat_results = pd.DataFrame(columns=['Start', 'End','GR', 'LR', 'tstat', 'pval'])
df_tstat_results = pd.DataFrame(columns=['Start', 'End','GR', 'LR', 'tstat', 'pval'])

while dt_start <= dt_end: # iterate through all months, add to tstat result df
    dt_tstat_start = dt_start
    dt_tstat_end = add_months(dt_tstat_start, 1) - timedelta(days = 1) # from the 1st of the month to the last of previous month
    dt_tstat_end = dt_tstat_end.replace(hour = 23, minute = 59, second = 59) # adjust time to end of day

    dfTstat = dfMerged_per_timeframe(dfTa, dt_tstat_start,dt_tstat_end)
    tstat = tstat_for_df(dfTstat)

    new_row ={'Start': str(dt_tstat_start), 'End': str(dt_tstat_end), 'GR': dfTstat['ti_GR'].sum(), 'LR': dfTstat['ti_LR'].sum(), 
    'tstat': tstat.statistic, 'pval': tstat.pvalue}

    df_tstat_results = df_tstat_results.append(new_row, ignore_index=True)

    print ("t-stat date range: " + dt_tstat_start.strftime('%Y.%m.%d %H:%M:%S') + " - " + dt_tstat_end.strftime('%Y.%m.%d %H:%M:%S')
    + " tstat: " + str(tstat.statistic) + " pval: " + str(tstat.pvalue))
    
    dt_start = add_months(dt_start, 1)

# %%

#dfMerged.to_excel(r'/Users/jes/OneDrive - FH JOANNEUM/06 Data/kaiko-ohlcv-1h-year/_dfMerged_LR-GR_export.xlsx', index = False)
#dfMerged2013to2019.to_excel(r'/Users/jes/OneDrive - FH JOANNEUM/06 Data/kaiko-ohlcv-1h-year/_dfMerged_LR-GR_2013to2019_export.xlsx', index = False)
df_tstat_results.to_excel(r'../reports/df_tstat_results.xlsx', index = False)

# %%
#6##################################################################################################
# calculate SMA simple moving average
####################################################################################################
import ta

# define the dataframe that should be used for TA indicator assignment
dfTa = dfMerged2013to2019

#classta.trend.SMAIndicator(close: pandas.core.series.Series, n: int, fillna: bool = False)
indicator_sma2 = ta.trend.SMAIndicator(close=dfTa['avg_close'], n = 2*24) # 2 days = *24
indicator_sma5 = ta.trend.SMAIndicator(close=dfTa['avg_close'], n = 5*24) # 5 days = *24
indicator_sma50 = ta.trend.SMAIndicator(close=dfTa['avg_close'], n = 50*24) # 50 days = *24
indicator_sma150 = ta.trend.SMAIndicator(close=dfTa['avg_close'], n = 150*24) # 150 days = *24
indicator_sma200 = ta.trend.SMAIndicator(close=dfTa['avg_close'], n = 200*24) # 200 days = *24

dfTa['ti_sma2'] = indicator_sma2.sma_indicator()
dfTa['ti_sma5'] = indicator_sma5.sma_indicator()
dfTa['ti_sma50'] = indicator_sma50.sma_indicator()
dfTa['ti_sma150'] = indicator_sma150.sma_indicator()
dfTa['ti_sma200'] = indicator_sma200.sma_indicator()

# reset / initialise columns
dfTa['ti_sma1-50_GR'] = 0
dfTa['ti_sma1-50_LR'] = 0
dfTa['ti_sma1-150_GR'] = 0
dfTa['ti_sma1-150_LR'] = 0
dfTa['ti_sma5-150_GR'] = 0
dfTa['ti_sma5-150_LR'] = 0
dfTa['ti_sma1-200_GR'] = 0
dfTa['ti_sma1-200_LR'] = 0
dfTa['ti_sma2-200_GR'] = 0
dfTa['ti_sma2-200_LR'] = 0


# for model 1 - disposition effect identify bullish + bearish market
# upward trend when SMA short is > SMA long (sell in positive market) = GR
# downward trend when SMA short < SMA long (sell in negative market) = LR
dfTa.loc[dfTa['avg_close'] > dfTa['ti_sma50'], 'ti_sma1-50_GR'] = dfTa['txCnt'] # SMA 1-50
dfTa.loc[dfTa['avg_close'] > dfTa['ti_sma50'], 'ti_sma1-50_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_sma50'], 'ti_sma1-50_LR'] = dfTa['txCnt']
dfTa.loc[dfTa['avg_close'] < dfTa['ti_sma50'], 'ti_sma1-50_GR_LR'] = 'LR'

dfTa.loc[dfTa['avg_close'] > dfTa['ti_sma150'], 'ti_sma1-150_GR'] = dfTa['txCnt'] # SMA 1-150
dfTa.loc[dfTa['avg_close'] > dfTa['ti_sma150'], 'ti_sma1-150_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_sma150'], 'ti_sma1-150_LR'] = dfTa['txCnt'] 
dfTa.loc[dfTa['avg_close'] < dfTa['ti_sma150'], 'ti_sma1-150_GR_LR'] = 'LR'

dfTa.loc[dfTa['ti_sma5'] > dfTa['ti_sma150'], 'ti_sma5-150_GR'] = dfTa['txCnt'] # SMA 5-150
dfTa.loc[dfTa['ti_sma5'] > dfTa['ti_sma150'], 'ti_sma5-150_GR_LR'] = 'GR'
dfTa.loc[dfTa['ti_sma5'] < dfTa['ti_sma150'], 'ti_sma5-150_LR'] = dfTa['txCnt'] 
dfTa.loc[dfTa['ti_sma5'] < dfTa['ti_sma150'], 'ti_sma5-150_GR_LR'] = 'LR'

dfTa.loc[dfTa['avg_close'] > dfTa['ti_sma200'], 'ti_sma1-200_GR'] = dfTa['txCnt'] # SMA 1-200
dfTa.loc[dfTa['avg_close'] > dfTa['ti_sma200'], 'ti_sma1-200_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_sma200'], 'ti_sma1-200_LR'] = dfTa['txCnt'] 
dfTa.loc[dfTa['avg_close'] < dfTa['ti_sma200'], 'ti_sma1-200_GR_LR'] = 'LR'

dfTa.loc[dfTa['ti_sma2'] > dfTa['ti_sma200'], 'ti_sma2-200_GR'] = dfTa['txCnt'] # SMA 2-200
dfTa.loc[dfTa['ti_sma2'] > dfTa['ti_sma200'], 'ti_sma2-200_GR_LR'] = 'GR'
dfTa.loc[dfTa['ti_sma2'] < dfTa['ti_sma200'], 'ti_sma2-200_LR'] = dfTa['txCnt'] 
dfTa.loc[dfTa['ti_sma2'] < dfTa['ti_sma200'], 'ti_sma2-200_GR_LR'] = 'LR'

# %% 
#6##################################################################################################
# trading range breakouts - support / resistence
####################################################################################################
import ta

# define the dataframe that should be used for TA indicator assignment
dfTa = dfMerged2013to2019

# trb - trading range breakouts = support / resistance => Donchian Channel
#classta.volatility.DonchianChannel(high: pandas.core.series.Series, low: pandas.core.series.Series, 
#close: pandas.core.series.Series, n: int = 20, offset: int = 0, fillna: bool = False)
indicator_trb50 = ta.volatility.DonchianChannel(dfTa['avg_high'], dfTa['avg_low'], dfTa['avg_close'], n = (50*24)) # usually 50 = *24 for day view
indicator_trb150 = ta.volatility.DonchianChannel(dfTa['avg_high'], dfTa['avg_low'], dfTa['avg_close'], n = (150*24)) # usually 50 = *24 for day view
indicator_trb200 = ta.volatility.DonchianChannel(dfTa['avg_high'], dfTa['avg_low'], dfTa['avg_close'], n = (200*24)) # usually 50 = *24 for day view
dfTa['ti_trb50_hband'] = indicator_trb50.donchian_channel_hband()
dfTa['ti_trb50_mband'] = indicator_trb50.donchian_channel_mband()
dfTa['ti_trb50_lband'] = indicator_trb50.donchian_channel_lband()

dfTa['ti_trb150_hband'] = indicator_trb50.donchian_channel_hband()
dfTa['ti_trb150_mband'] = indicator_trb50.donchian_channel_mband()
dfTa['ti_trb150_lband'] = indicator_trb50.donchian_channel_lband()

dfTa['ti_trb200_hband'] = indicator_trb50.donchian_channel_hband()
dfTa['ti_trb200_mband'] = indicator_trb50.donchian_channel_mband()
dfTa['ti_trb200_lband'] = indicator_trb50.donchian_channel_lband()

# reset / initialise columns
dfTa['ti_trb50_GR'] = 0
dfTa['ti_trb50_LR'] = 0
dfTa['ti_trb150_GR'] = 0
dfTa['ti_trb150_LR'] = 0
dfTa['ti_trb200_GR'] = 0
dfTa['ti_trb200_LR'] = 0
dfTa['ti_trb50_GR_LR']  = 'N' # neutral when in channel - initial values
dfTa['ti_trb150_GR_LR']  = 'N'
dfTa['ti_trb200_GR_LR']  = 'N'

# for model 1 - disposition effect identify bullish + bearish market
dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb50_hband'], 'ti_trb50_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb50_hband'], 'ti_trb50_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb50_lband'], 'ti_trb50_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb50_lband'], 'ti_trb50_GR_LR'] = 'LR'

dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb150_hband'], 'ti_trb150_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb150_hband'], 'ti_trb150_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb150_lband'], 'ti_trb150_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb150_lband'], 'ti_trb150_GR_LR'] = 'LR'

dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb200_hband'], 'ti_trb200_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb200_hband'], 'ti_trb200_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb200_lband'], 'ti_trb200_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb200_lband'], 'ti_trb200_GR_LR'] = 'LR'

# %%
#6##################################################################################################
# calculate MACD as reference value to decide if LR or GR
####################################################################################################
import ta

# define the dataframe that should be used for TA indicator assignment
dfTa = dfMerged2013to2019

# MACD relative strength indicator
#classta.trend.MACD(close: pandas.core.series.Series, n_slow: int = 26, n_fast: int = 12, n_sign: int = 9, fillna: bool = False)
indicator_macd = ta.trend.MACD(close=dfTa['avg_close'], n_slow=(26*24), n_fast=(12*24), n_sign=9) # usually 26 and 12 days = *24 due to hourly resolution
dfTa['ti_macd'] = indicator_macd.macd()

# reset / initialise columns
dfTa['ti_macd_GR'] = 0
dfTa['ti_macd_LR'] = 0

# for model 1 - disposition effect identify bullish + bearish market
dfTa.loc[dfTa['ti_macd'] > 0, 'ti_macd_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['ti_macd'] > 0, 'ti_macd_GR_LR'] = 'GR'
dfTa.loc[dfTa['ti_macd'] < 0, 'ti_macd_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['ti_macd'] < 0, 'ti_macd_GR_LR'] = 'LR'

# %%
#6##################################################################################################
# calculate ROC - rate of change to decide if LR or GR
####################################################################################################
import ta

# define the dataframe that should be used for TA indicator assignment
dfTa = dfMerged2013to2019

# ROC - rate of change indicator
# classta.momentum.ROCIndicator(close: pandas.core.series.Series, n: int = 12, fillna: bool = False)
indicator_roc = ta.momentum.ROCIndicator(close=dfTa['avg_close'], n = (10*24)) # usually 10 days = *24 due to hourly resolution
dfTa['ti_roc'] = indicator_roc.roc()

# reset / initialise columns
dfTa['ti_roc_GR'] = 0
dfTa['ti_roc_LR'] = 0

# for model 1 - disposition effect identify bullish + bearish market
dfTa.loc[dfTa['ti_roc'] > 0, 'ti_roc_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['ti_roc'] > 0, 'ti_roc_GR_LR'] = 'GR'

dfTa.loc[dfTa['ti_roc'] < 0, 'ti_roc_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['ti_roc'] < 0, 'ti_roc_GR_LR'] = 'LR'

# %%
#6##################################################################################################
# calculate OBV - on balance volume to decide if LR or GR
####################################################################################################
import ta

# define the dataframe that should be used for TA indicator assignment
dfTa = dfMerged2013to2019

# OBV - on balance volume
# classta.volume.OnBalanceVolumeIndicator(close: pandas.core.series.Series, volume: pandas.core.series.Series, fillna: bool = False)
indicator_obv = ta.volume.OnBalanceVolumeIndicator(close=dfTa['avg_close'], volume=dfTa['avg_volume'])
dfTa['ti_obv'] = indicator_obv.on_balance_volume()

indicator_obv_sma2 = ta.trend.SMAIndicator(close=dfTa['ti_obv'], n = 2*24) # 2 days = *24
indicator_obv_sma5 = ta.trend.SMAIndicator(close=dfTa['ti_obv'], n = 5*24) # 5 days = *24
indicator_obv_sma50 = ta.trend.SMAIndicator(close=dfTa['ti_obv'], n = 50*24) # 50 days = *24
indicator_obv_sma150 = ta.trend.SMAIndicator(close=dfTa['ti_obv'], n = 150*24) # 150 days = *24
indicator_obv_sma200 = ta.trend.SMAIndicator(close=dfTa['ti_obv'], n = 200*24) # 200 days = *24

dfTa['ti_obv_sma2'] = indicator_obv_sma2.sma_indicator()
dfTa['ti_obv_sma5'] = indicator_obv_sma5.sma_indicator()
dfTa['ti_obv_sma50'] = indicator_obv_sma50.sma_indicator()
dfTa['ti_obv_sma150'] = indicator_obv_sma150.sma_indicator()
dfTa['ti_obv_sma200'] = indicator_obv_sma200.sma_indicator()


# reset / initialise columns
dfTa['ti_obv_sma1-50_GR'] = 0
dfTa['ti_obv_sma1-50_LR'] = 0
dfTa['ti_obv_sma1-150_GR'] = 0
dfTa['ti_obv_sma1-150_LR'] = 0
dfTa['ti_obv_sma5-150_GR'] = 0
dfTa['ti_obv_sma5-150_LR'] = 0
dfTa['ti_obv_sma1-200_GR'] = 0
dfTa['ti_obv_sma1-200_LR'] = 0
dfTa['ti_obv_sma2-200_GR'] = 0
dfTa['ti_obv_sma2-200_LR'] = 0



# upward trend when SMA short is > SMA long (sell in positive market) = GR
# downward trend when SMA short < SMA long (sell in negative market) = LR
dfTa.loc[dfTa['avg_close'] > dfTa['ti_obv_sma50'], 'ti_obv_sma1-50_GR'] = dfTa['txCnt'] # SMA 1-50
dfTa.loc[dfTa['avg_close'] > dfTa['ti_obv_sma50'], 'ti_obv_sma1-50_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_obv_sma50'], 'ti_obv_sma1-50_LR'] = dfTa['txCnt']
dfTa.loc[dfTa['avg_close'] < dfTa['ti_obv_sma50'], 'ti_obv_sma1-50_GR_LR'] = 'LR'

dfTa.loc[dfTa['avg_close'] > dfTa['ti_obv_sma150'], 'ti_obv_sma1-150_GR'] = dfTa['txCnt'] # SMA 1-150
dfTa.loc[dfTa['avg_close'] > dfTa['ti_obv_sma150'], 'ti_obv_sma1-150_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_obv_sma150'], 'ti_obv_sma1-150_LR'] = dfTa['txCnt'] 
dfTa.loc[dfTa['avg_close'] < dfTa['ti_obv_sma150'], 'ti_obv_sma1-150_GR_LR'] = 'LR'

dfTa.loc[dfTa['ti_obv_sma5'] > dfTa['ti_obv_sma150'], 'ti_obv_sma5-150_GR'] = dfTa['txCnt'] # SMA 5-150
dfTa.loc[dfTa['ti_obv_sma5'] > dfTa['ti_obv_sma150'], 'ti_obv_sma5-150_GR_LR'] = 'GR'
dfTa.loc[dfTa['ti_obv_sma5'] < dfTa['ti_obv_sma150'], 'ti_obv_sma5-150_LR'] = dfTa['txCnt'] 
dfTa.loc[dfTa['ti_obv_sma5'] < dfTa['ti_obv_sma150'], 'ti_obv_sma5-150_GR_LR'] = 'LR'

dfTa.loc[dfTa['avg_close'] > dfTa['ti_obv_sma200'], 'ti_obv_sma1-200_GR'] = dfTa['txCnt'] # SMA 1-200
dfTa.loc[dfTa['avg_close'] > dfTa['ti_obv_sma200'], 'ti_obv_sma1-200_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_obv_sma200'], 'ti_obv_sma1-200_LR'] = dfTa['txCnt'] 
dfTa.loc[dfTa['avg_close'] < dfTa['ti_obv_sma200'], 'ti_obv_sma1-200_GR_LR'] = 'LR'

dfTa.loc[dfTa['ti_obv_sma2'] > dfTa['ti_obv_sma200'], 'ti_obv_sma2-200_GR'] = dfTa['txCnt'] # SMA 2-200
dfTa.loc[dfTa['ti_obv_sma2'] > dfTa['ti_obv_sma200'], 'ti_obv_sma2-200_GR_LR'] = 'GR'
dfTa.loc[dfTa['ti_obv_sma2'] < dfTa['ti_obv_sma200'], 'ti_obv_sma2-200_LR'] = dfTa['txCnt'] 
dfTa.loc[dfTa['ti_obv_sma2'] < dfTa['ti_obv_sma200'], 'ti_obv_sma2-200_GR_LR'] = 'LR'


# %%
#6##################################################################################################
# calculate RSI as reference value to decide if LR or GR
####################################################################################################
import ta

# define the dataframe that should be used for TA indicator assignment
dfTa = dfMerged2013to2019

# RSI relative strength indicator
#classta.momentum.RSIIndicator(close: pandas.core.series.Series, n: int = 14, fillna: bool = False)
indicator_rsi = ta.momentum.RSIIndicator(close=dfTa['avg_close'], n=(14*24)) # usually 14 days = 14 * 24 => 336
dfTa['ti_rsi'] = indicator_rsi.rsi()

# reset / initialise columns
dfTa['ti_rsi_GR'] = 0
dfTa['ti_rsi_LR'] = 0

# for model 1 - disposition effect identify bullish + bearish market
dfTa.loc[dfTa['ti_rsi'] >= 50, 'ti_rsi_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['ti_rsi'] >= 50, 'ti_rsi_GR_LR'] = 'GR'
dfTa.loc[dfTa['ti_rsi'] < 50, 'ti_rsi_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['ti_rsi'] < 50, 'ti_rsi_GR_LR'] = 'LR'

# for model 2 - multiple regression
#dfTa['ti_rsi_bs'] = 'N' # starting point - all values to N neutral
#dfTa.loc[dfTa['ti_rsi'] < 30, 'ti_rsi_bs'] = 'B' # buy signal
#dfTa.loc[dfTa['ti_rsi'] > 70, 'ti_rsi_bs'] = 'S' # sell signal
#dfTa.loc[dfTa['ti_rsi_bs'] == 'B', 'ti_GR'] = dfTa['txCnt'] 
#dfTa.loc[dfTa['ti_rsi_bs'] == 'S', 'ti_LR'] = dfTa['txCnt'] 

#dfTa

print('tstat for RSI :' + str(tstat_for_df(dfTa)))
print('t-stat RSI for 2013: ' + str(tstat_for_df(dfMerged_per_year(dfTa, 2013))))
print('t-stat RSI for 2014: ' + str(tstat_for_df(dfMerged_per_year(dfTa, 2014))))
print('t-stat RSI for 2015: ' + str(tstat_for_df(dfMerged_per_year(dfTa, 2015))))
print('t-stat RSI for 2016: ' + str(tstat_for_df(dfMerged_per_year(dfTa, 2016))))
print('t-stat RSI for 2017: ' + str(tstat_for_df(dfMerged_per_year(dfTa, 2017))))
print('t-stat RSI for 2018: ' + str(tstat_for_df(dfMerged_per_year(dfTa, 2018))))
print('t-stat RSI for 2019: ' + str(tstat_for_df(dfMerged_per_year(dfTa, 2019))))
#dfTa.loc[dfTa['ti_rsi_bs'] == 'B'] # test output

# %%
#6##################################################################################################
# calculate Boellinger Bands reference value to decide if LR or GR
####################################################################################################
import ta

# define the dataframe that should be used for TA indicator assignment
dfTa = dfMerged2013to2019

# RSI relative strength indicator
# classta.volatility.BollingerBands(close: pandas.core.series.Series, n: int = 20, ndev: int = 2, fillna: bool = False)
indicator_bb = ta.volatility.BollingerBands(close=dfMerged["avg_close"], n=(20*24), ndev=2) # usually 20 days = *24 due to hourly resolution

# Add Bollinger Bands features
dfTa['ti_bb_bbm'] = indicator_bb.bollinger_mavg()
dfTa['ti_bb_bbh'] = indicator_bb.bollinger_hband()
dfTa['ti_bb_bbl'] = indicator_bb.bollinger_lband()

# reset / initialise columns
dfTa['ti_bb_GR'] = 0
dfTa['ti_bb_LR'] = 0
dfTa['ti_bb_GR_LR']  = 'N'

# for model 1 - disposition effect identify bullish + bearish market
dfTa.loc[dfTa['avg_close'] < dfTa['ti_bb_bbl'], 'ti_bb_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['avg_close'] < dfTa['ti_bb_bbl'], 'ti_bb_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] > dfTa['ti_bb_bbh'], 'ti_bb_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['avg_close'] > dfTa['ti_bb_bbh'], 'ti_bb_GR_LR'] = 'LR'


# %%
####################################################################################################
# example plots to visualise dfs
####################################################################################################

import matplotlib.pyplot as plt
import seaborn as sns

sns.boxplot(x=dfTa['ti_GR_LR'], y=dfTa['txCnt'], data=pd.melt(dfTa))
plt.show()

#sns.lineplot(x=dfTa['timestampOhlc'], y=dfTa['txCnt'], hue=dfTa['ti_GR_LR'], data=dfTa)
#plt.show()



