# CryptoDisposition - Disposition Effect in Cryptocurrencies

This repository contains the necessary data files and scripts to test the [disposition effect](https://en.wikipedia.org/wiki/Disposition_effect) in cryptocurrencies (like Bitcoin). The methodology is based on and inspired by the approach of proportion of losses realised (PLR) and proportion of gains realised (PGR) introduced by [Terence Odean](https://onlinelibrary.wiley.com/doi/10.1111/0022-1082.00072). It builds upon hourly OHLC data from Kaiko (values averaged over multiple exchanges) and the transaction data provided by the [GraphSense](https://github.com/graphsense) platform of the [Austrian Institute of Technology](https://www.ait.ac.at/).

The script compares losses realised (LR) versus gains realised (GR) to identify if investors act differently in upwards or downwards trending markets. Finally, LR and GR are compared via t-test.

The process is structured into the following steps:

## Step 1 - OHLC data preparation
_You can skip this step as the aggregated data is stored in this [file](https://github.com/jschatzmann/CryptoDisposition/blob/master/data/_ex/all_btcusd_ohlcv_1h_ex.csv) - the steps are described for the reason of completeness._

[Kaiko OHLC merge script](https://github.com/jschatzmann/CryptoDisposition/blob/master/code/Kaiko_OHLC_merge_exch-files.py)
> Kaiko_OHLC_merge_exch-files.py

1) configure path variable to define the location of the original kaiko OHLC files - [here](https://github.com/jschatzmann/CryptoDisposition/blob/50b884a095eb43ffe4a1ee1c991064e62c6b251e/code/Kaiko_OHLC_merge_exch-files.py#L9)
2) run the script

## Step 2 - join OHLC data and transactions
[Cryto Disposition](https://github.com/jschatzmann/CryptoDisposition/blob/master/code/CryptoDisposition.py)
> CryptoDisposition.py

1) configure path variable for transaction data - [here](https://github.com/jschatzmann/CryptoDisposition/blob/50b884a095eb43ffe4a1ee1c991064e62c6b251e/code/CryptoDisposition.py#L11-L12)
2) configure path variable for  OHLC exchange data - [here](https://github.com/jschatzmann/CryptoDisposition/blob/50b884a095eb43ffe4a1ee1c991064e62c6b251e/code/CryptoDisposition.py#L58-L59)
3) run all cells including the join OHLC and transaction data - [here](https://github.com/jschatzmann/CryptoDisposition/blob/50b884a095eb43ffe4a1ee1c991064e62c6b251e/code/CryptoDisposition.py#L84)

## Step 3 - add technical indicators from TA lib
Adding technical indicators as required, in our case SMA, RSI, OBV, Donchian Channel (trading range breakout), MACD, ROC and Boellinger Bands.
From [here](https://github.com/jschatzmann/CryptoDisposition/blob/50b884a095eb43ffe4a1ee1c991064e62c6b251e/code/CryptoDisposition.py#L129) to [here](https://github.com/jschatzmann/CryptoDisposition/blob/50b884a095eb43ffe4a1ee1c991064e62c6b251e/code/CryptoDisposition.py#L415) 

## Step 4 - conduct t-tests for relevant indicators
Using defined LR and GR columns of specified technical indicators to conduct t-test. Like for SMA:
```
colGR = 'ti_sma1-50_GR'
colLR = 'ti_sma1-50_LR'
```

Results are saved in the df\*Overall, df\*PerYear, and df\*PerMonth dataframes, for SMA e.g.

```
dfSmaOverall = tstat_for_df_colums(df_per_timeframe(dfTa, dt_start, dt_end), colLR, colGR) # tstat overall timeframe
dfSmaPerYear = tstat_for_indicator(dfTa, colLR, colGR, 12) # tstat values per year
dfSmaPerMonth = tstat_for_indicator(dfTa, colLR, colGR, 1) # tstat values per month
```
## Trading window / technical indicator configuration
It is important to mention that the technical indicators usually are based on daily data. Due to the fact that we have hourly data available which is relevant due to the high Bitcoin volatility, the indicator ranges can be adjusted by adding / removing the x24 (hours) multiplicator when adding the indicators. Configure the range from daily to hourly by setting the code line to 1 (=hourly), 24 (=24 hours = daily) [link](https://github.com/jschatzmann/CryptoDisposition/blob/f5b0f740cf0d3772a512204ce105a735144eca77/code/CryptoDisposition.py#L139).

```
IndicatorTimeWindow = 24
```
