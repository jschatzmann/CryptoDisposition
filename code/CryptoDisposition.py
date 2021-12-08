#!/usr/bin/python3
# %% 
#1##################################################################################################
# import transaction file from AIT (tx to exchanges = sells)
####################################################################################################
import numpy as np
import pandas as pd

#%%
#1a read txs_to_exchange.csv into memory

# strPath = '../data/testfiles/' # test file path
#strFileName = 'txs_to_exchanges_dryrun.csv' # test tx file

#strPath = '/Volumes/SAMSUNG/data_btc_ex/' # full tx file
strPath = '../data/' # test file path, excluded in .gitignore
strFileName = 'txs_to_exchanges.csv' # full tx file

strPathFile = strPath + strFileName
df = pd.read_csv(strPathFile)
df

#max timestamp for transaction
max_TxTmstp_value = df['timestamp'].max()
max_TxTmstp_value
#November file: Wed Nov 24 2021 23:58:54 GMT+0000


# %%
#2##################################################################################################
# aggregate tx information, sum up values per tx and the number of tx taken place in this block / tmstp
####################################################################################################

# sum up tx amount (containing multiple addresses) per block / timestamp
dfTxPerBlock = df.groupby(
    #['tx_hash', 'height', 'timestamp']).agg(
    #update 2021 - 'height' changed to 'block_id'
    ['tx_hash', 'block_id', 'timestamp']).agg(
        valSum = ('value', 'sum')
    ).reset_index()
dfTxPerBlock

# count tx and sum values per tx
dfTxPerBlock1 = dfTxPerBlock.groupby(
    #update 2021 - 'height' changed to 'block_id'
    #['height', 'timestamp']).agg(
    ['block_id', 'timestamp']).agg(
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

strPathOhlc = '../data/kaiko-ohlcv-1h-year/_ex/' # full file
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

dfTx['tmstTxRnd'] = round_unix_date(dfTx['timestampTx'], 60*60) #60sec = 1min *60 = 60 minutes => 1 hour

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

dfMerged['tmstTxRndDay'] = round_unix_date(dfMerged['tmstTxRnd'], 60*60*24, False) #60sec = 1min *60 = 60 minutes *24 hours => 1 day // false = flatten time to 00:00 of the day
dfMerged.loc[dfMerged['tmstTxRnd'] % (3600*24) == 0, 'tmstTxRndDay'] = dfMerged['tmstTxRnd']


# add readable timestamp column for testing purposes / validation
#dfMerged['tmstTx_rdbl'] = pd.to_datetime(dfMerged['timestampTx'], unit = 's')
dfMerged['tmstOhlc_rdbl'] = pd.to_datetime(dfMerged['timestampOhlc'], unit = 's')
dfMerged['tmstTxRnd_rdbl'] = pd.to_datetime(dfMerged['tmstTxRnd'], unit = 's')
dfMerged['tmstTxRndDay_rdbl'] = pd.to_datetime(dfMerged['tmstTxRndDay'], unit = 's')

dfMerged

# testing of examples if match works
#dfMerged.loc[dfMerged['timestampOhlc'] == 1517446800]
#dfMerged.loc[dfMerged['_merge'] == 'both']
#dfOhlc1.loc[dfOhlc1['timestampOhlc'] == 1520049600]

#dfMerged.to_excel(r'/Users/jes/OneDrive - FH JOANNEUM/06 Data/kaiko-ohlcv-1h-year/_dfMerged_export.xlsx', index = False)


# %% 
#5##################################################################################################
# configure / define dataframe to be used for TA indicators
####################################################################################################
import ta
import sys

# define the dataframe for the technical indicator to be used (standard is whole available data points)
dfTa = dfMerged
IndicatorTimeWindow = 1

# define the time window for TA indicators, 1 = hourly, 24 = daily
# check if commandline argument 'daily' was given, if not, standard window is 1 = hourly
if (len(sys.argv)>1):
    if(sys.argv[1] == 'daily'):
        IndicatorTimeWindow = 24
        print('Chosen rolling time window is '+ str(IndicatorTimeWindow) +' = daily window')
    else:
        print('Default rolling time window is '+ str(IndicatorTimeWindow) +' = hourly window')
else:
    print('Default rolling time window is '+ str(IndicatorTimeWindow) +' = hourly window')


# df storing LR and GR column names for TA indicator t-tests later
dfLrGrCol = pd.DataFrame(columns=['Type', 'colGR', 'colLR'])

# %%
#6a##################################################################################################
# calculate GR and LR based on Odeans approach -> compare open price to average price of the day
####################################################################################################

if IndicatorTimeWindow == 24: # in case the configuration is set to a daily view (24 hour)
    dfTaOdeanDaily = dfMerged.groupby(
        ['tmstTxRndDay']
        ).agg(
            avg_open = ('avg_open','mean'),
            avg_high = ('avg_high','mean'),
            avg_low = ('avg_low','mean'),
            avg_close = ('avg_close','mean'),
            avg_volume = ('avg_volume','mean'),
            valSum = ('valSum','sum'),
            txCnt = ('txCnt', 'sum')
        ).reset_index()
    dfTaOdeanDaily['tmstTxRndDay_rdbl'] = pd.to_datetime(dfTaOdeanDaily['tmstTxRndDay'], unit = 's')
    dfTaOdeanDaily['timestampOhlc'] = dfTaOdeanDaily['tmstTxRndDay'] # set as OHLC timestamp, enabiling limiting timeframes for t-statistics

    # initialise / set default columd value
    dfTaOdeanDaily['ti_GR'] = 0
    dfTaOdeanDaily['ti_LR'] = 0
    new_row ={'Type': 'Odean_GrLr', 'colGR': 'ti_GR', 'colLR': 'ti_LR'}
    dfLrGrCol = dfLrGrCol.append(new_row, ignore_index=True)

    # update data frame, if DAILY average of open + close price is higher than open price = GR (gain realised), if lower = LR (loss realised)
    dfTaOdeanDaily['ti_avg_OpenCLose'] = dfTaOdeanDaily[['avg_open', 'avg_close']].mean(axis = 1)
    dfTaOdeanDaily.loc[dfTaOdeanDaily['ti_avg_OpenCLose'] > dfTaOdeanDaily['avg_open'], 'ti_GR_LR'] = 'GR' 
    dfTaOdeanDaily.loc[dfTaOdeanDaily['ti_GR_LR'] == 'GR', 'ti_GR'] = dfTaOdeanDaily['txCnt'] 
    dfTaOdeanDaily.loc[dfTaOdeanDaily['ti_avg_OpenCLose'] < dfTaOdeanDaily['avg_open'], 'ti_GR_LR'] = 'LR' 
    dfTaOdeanDaily.loc[dfTaOdeanDaily['ti_GR_LR'] == 'LR', 'ti_LR'] = dfTaOdeanDaily['txCnt'] 

else: # configuration "else" is 1 and therefore hourly view
    # initialise / set default columd value
    dfTa['ti_GR'] = 0
    dfTa['ti_LR'] = 0
    new_row ={'Type': 'Odean_GrLr', 'colGR': 'ti_GR', 'colLR': 'ti_LR'}
    dfLrGrCol = dfLrGrCol.append(new_row, ignore_index=True)

    # update data frame, if HOURLY average of open + close price is higher than open price = GR (gain realised), if lower = LR (loss realised)
    dfTa['ti_avg_OpenCLose'] = dfTa[['avg_open', 'avg_close']].mean(axis = 1)
    dfTa.loc[dfTa['ti_avg_OpenCLose'] > dfTa['avg_open'], 'ti_GR_LR'] = 'GR' 
    dfTa.loc[dfTa['ti_GR_LR'] == 'GR', 'ti_GR'] = dfTa['txCnt'] 
    dfTa.loc[dfTa['ti_avg_OpenCLose'] < dfTa['avg_open'], 'ti_GR_LR'] = 'LR' 
    dfTa.loc[dfTa['ti_GR_LR'] == 'LR', 'ti_LR'] = dfTa['txCnt'] 

# %%
#6b##################################################################################################
# calculate SMA simple moving average
####################################################################################################

#classta.trend.SMAIndicator(close: pandas.core.series.Series, n: int, fillna: bool = False)
indicator_sma2 = ta.trend.SMAIndicator(close=dfTa['avg_close'], n = 2*IndicatorTimeWindow) # 2 days = *24
indicator_sma5 = ta.trend.SMAIndicator(close=dfTa['avg_close'], n = 5*IndicatorTimeWindow) # 5 days = *24
indicator_sma50 = ta.trend.SMAIndicator(close=dfTa['avg_close'], n = 50*IndicatorTimeWindow) # 50 days = *24
indicator_sma150 = ta.trend.SMAIndicator(close=dfTa['avg_close'], n = 150*IndicatorTimeWindow) # 150 days = *24
indicator_sma200 = ta.trend.SMAIndicator(close=dfTa['avg_close'], n = 200*IndicatorTimeWindow) # 200 days = *24

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

new_row = pd.DataFrame({'Type': ['ti_sma1-50', 'ti_sma1-150', 'ti_sma5-150', 'ti_sma1-200', 'ti_sma2-200'],
                        'colGR': ['ti_sma1-50_GR', 'ti_sma1-150_GR', 'ti_sma5-150_GR', 'ti_sma1-200_GR', 'ti_sma2-200_GR'],
                        'colLR': ['ti_sma1-50_LR', 'ti_sma1-150_LR', 'ti_sma5-150_LR', 'ti_sma1-200_LR', 'ti_sma2-200_LR']})
dfLrGrCol = dfLrGrCol.append(new_row, ignore_index=True)


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
#6c##################################################################################################
# trading range breakouts - support / resistence
####################################################################################################

# trb - trading range breakouts = support / resistance => Donchian Channel
#classta.volatility.DonchianChannel(high: pandas.core.series.Series, low: pandas.core.series.Series, 
#close: pandas.core.series.Series, n: int = 20, offset: int = 0, fillna: bool = False)
indicator_trb50 = ta.volatility.DonchianChannel(dfTa['avg_high'], dfTa['avg_low'], dfTa['avg_close'], n = (50*IndicatorTimeWindow)) # usually 50 = *24 for day view
indicator_trb150 = ta.volatility.DonchianChannel(dfTa['avg_high'], dfTa['avg_low'], dfTa['avg_close'], n = (150*IndicatorTimeWindow)) # usually 50 = *24 for day view
indicator_trb200 = ta.volatility.DonchianChannel(dfTa['avg_high'], dfTa['avg_low'], dfTa['avg_close'], n = (200*IndicatorTimeWindow)) # usually 50 = *24 for day view
dfTa['ti_trb50_hband'] = indicator_trb50.donchian_channel_hband()
dfTa['ti_trb50_mband'] = indicator_trb50.donchian_channel_mband()
dfTa['ti_trb50_lband'] = indicator_trb50.donchian_channel_lband()

dfTa['ti_trb150_hband'] = indicator_trb150.donchian_channel_hband()
dfTa['ti_trb150_mband'] = indicator_trb150.donchian_channel_mband()
dfTa['ti_trb150_lband'] = indicator_trb150.donchian_channel_lband()

dfTa['ti_trb200_hband'] = indicator_trb200.donchian_channel_hband()
dfTa['ti_trb200_mband'] = indicator_trb200.donchian_channel_mband()
dfTa['ti_trb200_lband'] = indicator_trb200.donchian_channel_lband()

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

new_row = pd.DataFrame({'Type': ['ti_trb50', 'ti_trb150', 'ti_trb200'],
                        'colGR': ['ti_trb50_GR', 'ti_trb150_GR', 'ti_trb200_GR'],
                        'colLR': ['ti_trb50_LR', 'ti_trb150_LR', 'ti_trb200_LR']})
dfLrGrCol = dfLrGrCol.append(new_row, ignore_index=True)


# for model 1 - disposition effect identify bullish + bearish market
dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb50_mband'], 'ti_trb50_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb50_mband'], 'ti_trb50_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb50_mband'], 'ti_trb50_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb50_mband'], 'ti_trb50_GR_LR'] = 'LR'

dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb150_mband'], 'ti_trb150_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb150_mband'], 'ti_trb150_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb150_mband'], 'ti_trb150_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb150_mband'], 'ti_trb150_GR_LR'] = 'LR'

dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb200_mband'], 'ti_trb200_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['avg_close'] > dfTa['ti_trb200_mband'], 'ti_trb200_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb200_mband'], 'ti_trb200_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['avg_close'] < dfTa['ti_trb200_mband'], 'ti_trb200_GR_LR'] = 'LR'

# %%
#6d##################################################################################################
# calculate MACD as reference value to decide if LR or GR
####################################################################################################

# MACD moving average convergence divergense
#classta.trend.MACD(close: pandas.core.series.Series, n_slow: int = 26, n_fast: int = 12, n_sign: int = 9, fillna: bool = False)
indicator_macd = ta.trend.MACD(close=dfTa['avg_close'], n_slow=(26*IndicatorTimeWindow), n_fast=(12*IndicatorTimeWindow), n_sign=9) # usually 26 and 12 days = *24 due to hourly resolution
dfTa['ti_macd'] = indicator_macd.macd()

# reset / initialise columns
dfTa['ti_macd_GR'] = 0
dfTa['ti_macd_LR'] = 0

new_row = pd.DataFrame({'Type': ['ti_macd'],
                        'colGR': ['ti_macd_GR'],
                        'colLR': ['ti_macd_LR']})
dfLrGrCol = dfLrGrCol.append(new_row, ignore_index=True)

# for model 1 - disposition effect identify bullish + bearish market
dfTa.loc[dfTa['ti_macd'] > 0, 'ti_macd_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['ti_macd'] > 0, 'ti_macd_GR_LR'] = 'GR'
dfTa.loc[dfTa['ti_macd'] < 0, 'ti_macd_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['ti_macd'] < 0, 'ti_macd_GR_LR'] = 'LR'

# %%
#6e##################################################################################################
# calculate ROC - rate of change to decide if LR or GR
####################################################################################################

# ROC - rate of change indicator
# classta.momentum.ROCIndicator(close: pandas.core.series.Series, n: int = 12, fillna: bool = False)
indicator_roc = ta.momentum.ROCIndicator(close=dfTa['avg_close'], n = (10*IndicatorTimeWindow)) # usually 10 days = *24 due to hourly resolution
dfTa['ti_roc'] = indicator_roc.roc()

# reset / initialise columns
dfTa['ti_roc_GR'] = 0
dfTa['ti_roc_LR'] = 0

new_row = pd.DataFrame({'Type': ['ti_roc'],
                        'colGR': ['ti_roc_GR'],
                        'colLR': ['ti_roc_LR']})
dfLrGrCol = dfLrGrCol.append(new_row, ignore_index=True)

# for model 1 - disposition effect identify bullish + bearish market
dfTa.loc[dfTa['ti_roc'] > 0, 'ti_roc_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['ti_roc'] > 0, 'ti_roc_GR_LR'] = 'GR'

dfTa.loc[dfTa['ti_roc'] < 0, 'ti_roc_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['ti_roc'] < 0, 'ti_roc_GR_LR'] = 'LR'

# %%
#6f##################################################################################################
# calculate OBV - on balance volume to decide if LR or GR
####################################################################################################

# OBV - on balance volume
# classta.volume.OnBalanceVolumeIndicator(close: pandas.core.series.Series, volume: pandas.core.series.Series, fillna: bool = False)
indicator_obv = ta.volume.OnBalanceVolumeIndicator(close=dfTa['avg_close'], volume=dfTa['avg_volume'])
dfTa['ti_obv'] = indicator_obv.on_balance_volume()

indicator_obv_sma2 = ta.trend.SMAIndicator(close=dfTa['ti_obv'], n = 2*IndicatorTimeWindow) # 2 days = *24
indicator_obv_sma5 = ta.trend.SMAIndicator(close=dfTa['ti_obv'], n = 5*IndicatorTimeWindow) # 5 days = *24
indicator_obv_sma50 = ta.trend.SMAIndicator(close=dfTa['ti_obv'], n = 50*IndicatorTimeWindow) # 50 days = *24
indicator_obv_sma150 = ta.trend.SMAIndicator(close=dfTa['ti_obv'], n = 150*IndicatorTimeWindow) # 150 days = *24
indicator_obv_sma200 = ta.trend.SMAIndicator(close=dfTa['ti_obv'], n = 200*IndicatorTimeWindow) # 200 days = *24

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

new_row = pd.DataFrame({'Type': ['ti_obv_sma1-50', 'ti_obv_sma1-150', 'ti_obv_sma5-150', 'ti_obv_sma1-200', 'ti_obv_sma2-200'],
                        'colGR': ['ti_obv_sma1-50_GR', 'ti_obv_sma1-150_GR', 'ti_obv_sma5-150_GR', 'ti_obv_sma1-200_GR', 'ti_obv_sma2-200_GR'],
                        'colLR': ['ti_obv_sma1-50_LR', 'ti_obv_sma1-150_LR', 'ti_obv_sma5-150_LR', 'ti_obv_sma1-200_LR', 'ti_obv_sma2-200_LR']})
dfLrGrCol = dfLrGrCol.append(new_row, ignore_index=True)


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
#6g##################################################################################################
# calculate RSI as reference value to decide if LR or GR
####################################################################################################

# RSI relative strength indicator
#classta.momentum.RSIIndicator(close: pandas.core.series.Series, n: int = 14, fillna: bool = False)
indicator_rsi = ta.momentum.RSIIndicator(close=dfTa['avg_close'], n=(14*IndicatorTimeWindow)) # usually 14 days = 14 * 24 => 336
dfTa['ti_rsi'] = indicator_rsi.rsi()

# reset / initialise columns
dfTa['ti_rsi_GR'] = 0
dfTa['ti_rsi_LR'] = 0

new_row = pd.DataFrame({'Type': ['ti_rsi'],
                        'colGR': ['ti_rsi_GR'],
                        'colLR': ['ti_rsi_LR']})
dfLrGrCol = dfLrGrCol.append(new_row, ignore_index=True)

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

# %%
#6h##################################################################################################
# calculate Boellinger Bands reference value to decide if LR or GR
####################################################################################################

# RSI relative strength indicator
# classta.volatility.BollingerBands(close: pandas.core.series.Series, n: int = 20, ndev: int = 2, fillna: bool = False)
indicator_bb = ta.volatility.BollingerBands(close=dfMerged["avg_close"], n=(20*IndicatorTimeWindow), ndev=2) # usually 20 days = *24 due to hourly resolution

# Add Bollinger Bands features
dfTa['ti_bb_bbm'] = indicator_bb.bollinger_mavg()
dfTa['ti_bb_bbh'] = indicator_bb.bollinger_hband()
dfTa['ti_bb_bbl'] = indicator_bb.bollinger_lband()

# reset / initialise columns
dfTa['ti_bb_GR'] = 0
dfTa['ti_bb_LR'] = 0
dfTa['ti_bb_GR_LR']  = 'N'

new_row = pd.DataFrame({'Type': ['ti_bb'],
                        'colGR': ['ti_bb_GR'],
                        'colLR': ['ti_bb_LR']})
dfLrGrCol = dfLrGrCol.append(new_row, ignore_index=True)

# for model 1 - disposition effect identify bullish + bearish market
dfTa.loc[dfTa['avg_close'] < dfTa['ti_bb_bbl'], 'ti_bb_GR'] = dfTa['txCnt'] # upward trend = sell in positive sentiment = GR
dfTa.loc[dfTa['avg_close'] < dfTa['ti_bb_bbl'], 'ti_bb_GR_LR'] = 'GR'
dfTa.loc[dfTa['avg_close'] > dfTa['ti_bb_bbh'], 'ti_bb_LR'] = dfTa['txCnt'] # downard trend = sell in negative sentiment = LR
dfTa.loc[dfTa['avg_close'] > dfTa['ti_bb_bbh'], 'ti_bb_GR_LR'] = 'LR'

# %% 
#7###################################################################################################
# define functions to limit dataframes and calculate t-statistics
####################################################################################################
from datetime import datetime, timedelta
from dateutil import tz
from dateutil.relativedelta import *
import calendar
import pandas as pd

# functions to limit df to certain time window
def df_per_year(df, year): # return df reduced to passed year
    # datetime(year, month, day, hour, minute, second, microsecond)
    tmstp_start = datetime(year, 1, 1, 0, 0, 0, 0, tzinfo = tz.UTC)
    tmstp_end = datetime(year, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)
    df = df[df['timestampOhlc'] >= int(tmstp_start.timestamp())]
    df = df[df['timestampOhlc']<= int(tmstp_end.timestamp())]
    return df

def df_per_timeframe(df, dt_start, dt_end): # return df reduced to passed start / end datetime
    # datetime(year, month, day, hour, minute, second, microsecond)
    df = df[df['timestampOhlc'] >= int(dt_start.timestamp())]
    df = df[df['timestampOhlc']<= int(dt_end.timestamp())]
    return df

#def tstat_for_df(df): # return t-statistics (paired / related samples) for passed dataframe
#    from scipy import stats
#    return stats.ttest_rel(df['ti_LR'],df['ti_GR'], nan_policy='omit')

def tstat_for_df_colums(df, strColLR, strColGR): # return t-statistics (paired / related samples) for passed columns to compare
    from scipy import stats
    return stats.ttest_rel(df[strColLR], df[strColGR], nan_policy='omit')

def add_months(sourcedate, months): # add month to datetime to efficiently iterate though df and create monthly t-stat values
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    return datetime(year, month, day, 0, 0 , 0, tzinfo = tz.UTC)

def tstat_for_indicator(dfTa, strColLR, strColGR, monthDelta): # calculate tstat for rolling time window (monhtDelta) and specific LR + GR colums
    
    df_tstat_results = pd.DataFrame(columns=['Start', 'End','GR', 'LR', 'tstat', 'pval'])
    dt_start = datetime(2013, 1, 1, 0, 0, 0, 0, tzinfo = tz.UTC)
    # Update March 2021 - extend timeframe till 31.12.2020
    #dt_end = datetime(2019, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)
    #dt_end = datetime(2020, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)
    dt_end = datetime(2021, 11, 24, 23, 59, 59, 0, tzinfo = tz.UTC) #update with 2021 data till November 2021

    while dt_start <= dt_end: # iterate through defined time window
        dt_tstat_start = dt_start
        dt_tstat_end = add_months(dt_tstat_start, monthDelta) - timedelta(days = 1) # from the 1st of the month to the last of previous month
        dt_tstat_end = dt_tstat_end.replace(hour = 23, minute = 59, second = 59) # adjust time to end of day

        dfTstat = df_per_timeframe(dfTa, dt_tstat_start,dt_tstat_end)
        tstat = tstat_for_df_colums(dfTstat, strColLR, strColGR)

        new_row ={'Start': str(dt_tstat_start), 'End': str(dt_tstat_end), 'GR': dfTstat[strColGR].sum(), 'LR': dfTstat[strColLR].sum(), 
        'tstat': tstat.statistic, 'pval': tstat.pvalue}

        df_tstat_results = df_tstat_results.append(new_row, ignore_index=True)

        #print ("t-stat date range: " + dt_tstat_start.strftime('%Y.%m.%d %H:%M:%S') + " - " + dt_tstat_end.strftime('%Y.%m.%d %H:%M:%S')
        #+ " tstat: " + str(tstat.statistic) + " pval: " + str(tstat.pvalue))
        
        dt_start = add_months(dt_start, monthDelta)
    
    return df_tstat_results

# %% 
#8###################################################################################################
# conduct t-tests for defined dataframes and date ranges (e.g. monhtly and yearly values)
####################################################################################################

import openpyxl

# create Excel writer to export t-stat dataframes
current_time = datetime.now()
strReportPath = '../results/df_tstat_results_'+current_time.strftime('%Y-%m-%d_%H_%M_%S')+'.xlsx'

dt_start = datetime(2013, 1, 1, 0, 0, 0, 0, tzinfo = tz.UTC)
# Update March 2021 - extend timeframe till 31.12.2020
#dt_end = datetime(2019, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)
#dt_end = datetime(2020, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)
dt_end = datetime(2021, 11, 24, 23, 59, 59, 0, tzinfo = tz.UTC) #update with 2021 data till November 2021
difference_in_years = relativedelta(dt_end,dt_start).years + 1 # added to calculate tstat overall in years via parameter, +1 to include till end-of-year

# create df with descriptive information of the report cover page
dfRepSummary = pd.DataFrame(np.array([['Time Window for TA indicator', IndicatorTimeWindow],
                                     ['Timestampe File created', str(current_time)], 
                                     ['Dataframe Start Time', str(dt_start)],
                                     ['Dataframe End Time', str(dt_end)],
                                     ['----------', '----------']]),
                            columns=['Item', 'Description'])
#new_row ={'Item': 'Time Window for TA indicator', 'End': IndicatorTimeWindow}
#dfRepSummary = dfRepSummary.append(new_row, ignore_index=True)

dfPaperPlots = pd.DataFrame(columns=['Start', 'End', 'GR', 'LR', 'tstat', 'pval', 'TaType'])

with pd.ExcelWriter(strReportPath, mode='w',  engine='openpyxl') as xlsx: #a append, w overwrite (default)
    sheet_name = 'Meta'
    dfRepSummary.to_excel(xlsx, sheet_name)
     
    # set index column width
    ws = xlsx.sheets[sheet_name]
    ws.column_dimensions['B'].width = 25
    ws.column_dimensions['C'].width = 40

    
# iterate all previously defined technical indicator rules, conduct t-test and export to XLS
for index, row in dfLrGrCol.iterrows():
    # catch special case for manual calculated daily Odean values (averages) stored in dfTaOdeanDaily, else use dfTa where other Tech. Ind. values are stored
    if (IndicatorTimeWindow == 24) and (row['Type'] == "Odean_GrLr"): 
        #dfTaOverall = tstat_for_indicator(dfTaOdeanDaily, row['colLR'], row['colGR'], (12*7)) # tstat overall timeframe (7 years 2013 to incl. 2019)
        dfTaOverall = tstat_for_indicator(dfTaOdeanDaily, row['colLR'], row['colGR'], (12*difference_in_years)) # tstat overall timeframe (x years dt_start to incl. dt_end)
        dfTaPerYear = tstat_for_indicator(dfTaOdeanDaily, row['colLR'], row['colGR'], 12) # tstat values per year
        dfTaPerMonth = tstat_for_indicator(dfTaOdeanDaily, row['colLR'], row['colGR'], 1) # tstat values per month
    else:
        #dfTaOverall = tstat_for_indicator(dfTa, row['colLR'], row['colGR'], (12*7)) # tstat overall timeframe (7 years 2013 to incl. 2019)
        dfTaOverall = tstat_for_indicator(dfTa, row['colLR'], row['colGR'], (12*difference_in_years)) # tstat overall timeframe (x years dt_start to incl. dt_end)
        dfTaPerYear = tstat_for_indicator(dfTa, row['colLR'], row['colGR'], 12) # tstat values per year
        dfTaPerMonth = tstat_for_indicator(dfTa, row['colLR'], row['colGR'], 1) # tstat values per month

    # export df to excel report
    with pd.ExcelWriter(strReportPath, mode='a', engine='openpyxl') as xlsx:
        
        sheet_name = str(index+1).zfill(2) + '_' + row['Type']+"All"
        dfTaOverall.to_excel(xlsx, sheet_name=sheet_name)
        ws = xlsx.sheets[sheet_name]
        ws.column_dimensions['B'].width = 22
        ws.column_dimensions['C'].width = 22
        ws.column_dimensions['D'].width = 10
        ws.column_dimensions['E'].width = 10
        ws.column_dimensions['F'].width = 13
        ws.column_dimensions['G'].width = 13

        sheet_name = str(index+1).zfill(2) + '_'+ row['Type']+"Year"
        dfTaPerYear.to_excel(xlsx, sheet_name=sheet_name)
        ws = xlsx.sheets[sheet_name]
        ws.column_dimensions['B'].width = 22
        ws.column_dimensions['C'].width = 22
        ws.column_dimensions['D'].width = 10
        ws.column_dimensions['E'].width = 10
        ws.column_dimensions['F'].width = 13
        ws.column_dimensions['G'].width = 13

        sheet_name = str(index+1).zfill(2) + '_'+ row['Type']+"Month"
        dfTaPerMonth.to_excel(xlsx, sheet_name=sheet_name)
        ws = xlsx.sheets[sheet_name]
        ws.column_dimensions['B'].width = 22
        ws.column_dimensions['C'].width = 22
        ws.column_dimensions['D'].width = 10
        ws.column_dimensions['E'].width = 10
        ws.column_dimensions['F'].width = 13
        ws.column_dimensions['G'].width = 13

    # monthly df appended for later export for paper plots
    dfTaPerMonth['TaType'] = row['Type']
    dfPaperPlots = dfPaperPlots.append(dfTaPerMonth, ignore_index = True)


    # add to summary of cover page what kind of indicator used which columns for t-statistics
    new_row = pd.DataFrame( np.array([['> t-stat overall: ' + row['Type'], str(dfTaOverall.iloc[0]['tstat'])+ " pval: "+ str(dfTaOverall.iloc[0]['pval'])]]),
                            columns=['Item', 'Description'])
    dfRepSummary = dfRepSummary.append(new_row, ignore_index=True)

    new_row = pd.DataFrame( np.array([['-- TA Indicator: ' + row['Type'], 'colGR: ' + row['colGR'] + ' colLR: ' + row['colLR']]]),
                            columns=['Item', 'Description'])
    dfRepSummary = dfRepSummary.append(new_row, ignore_index=True)

with pd.ExcelWriter(strReportPath, mode='a', engine='openpyxl') as xlsx:
    sheet_name = '00_Summary'
    dfRepSummary.to_excel(xlsx, sheet_name=sheet_name)
    ws = xlsx.sheets[sheet_name]
    ws.column_dimensions['B'].width = 27
    ws.column_dimensions['C'].width = 75

wb = openpyxl.load_workbook(strReportPath)
wb._sheets.sort(key=lambda ws: ws.title) # sort 
wb.save(strReportPath)

print('T-statistic export file saved in ' + strReportPath)

# %%
#9###################################################################################################
# export monthly df for visualisation purposes
####################################################################################################
strPlotPath = '../results/_dfPaperPlotsperMonth_export_'+current_time.strftime('%Y-%m-%d_%H_%M_%S')+'.xlsx'
dfPaperPlots.to_excel(strPlotPath, index = False)
print('Dataframe export file for plots file saved in ' + strPlotPath)


# %%
#10###################################################################################################
# Export dataframe to Excel for manual / double checking
####################################################################################################

# limit columns for export
cols_to_keep = ['timestampOhlc', 'avg_open', 'avg_high', 'avg_low', 'avg_close',
       'avg_volume', 'tmstTxRnd', 'valSum', 'txCnt', '_merge', 'tmstOhlc_rdbl',
       'tmstTxRnd_rdbl','ti_trb50_hband', 'ti_trb50_mband',
       'ti_trb50_lband', 'ti_trb150_hband', 'ti_trb150_mband',
       'ti_trb150_lband', 'ti_trb200_hband', 'ti_trb200_mband',
       'ti_trb200_lband', 'ti_trb50_GR', 'ti_trb50_LR', 'ti_trb150_GR',
       'ti_trb150_LR', 'ti_trb200_GR', 'ti_trb200_LR', 'ti_trb50_GR_LR',
       'ti_trb150_GR_LR', 'ti_trb200_GR_LR',]

#dfExport = df_per_timeframe(dfTa,dt_start, dt_end)
#dfExport[cols_to_keep].to_excel(r'../results/_dfTA_export.xlsx', index = False)

#dfTaOdeanDaily.to_excel(r'../results/_dfTAOdean_export.xlsx')
#dfMerged.to_excel(r'../results/_dfTAMerged_export.xlsx')
strTaExport = '../data/_dfTA_export_'+current_time.strftime('%Y-%m-%d_%H_%M_%S')+'.csv' 
dfTa.to_csv(strTaExport)
print('Dataframe export file for offline analysis file saved in ' + strTaExport)


# %%
#x###################################################################################################
# example plots to visualise dfs
####################################################################################################

import matplotlib.pyplot as plt
import seaborn as sns

sns.boxplot(x=dfTa['ti_GR_LR'], y=dfTa['txCnt'], data=pd.melt(dfTa))
plt.show()

#sns.lineplot(x=dfTa['timestampOhlc'], y=dfTa['txCnt'], hue=dfTa['ti_GR_LR'], data=dfTa)
#plt.show()

# %%
