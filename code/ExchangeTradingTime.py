# find out the min and max timeframe while a exchange was active
# joining exchange_address_with_cluster.csv with tx_to_exchanges.csv

#%% import tx and exchange clusters
import numpy as np
import pandas as pd

# read tx to exchanges
strPath = '../data/' # test file path, excluded in .gitignore
strFileName = 'txs_to_exchanges.csv' # full tx file

strPathFile = strPath + strFileName
dfTx = pd.read_csv(strPathFile)
dfTx

# read exchange clusters
strFileNameEx = 'exchange_address_with_cluster.csv'
strPathFile = strPath + strFileName
dfEx = pd.read_csv(strPathFile)
dfEx

# %% group by timestamp and cluster ID

dfGrpTx = dfTx.groupby(
     ['exchange']
 ).agg(
     minTmstp = ('timestamp','min'),
     maxTmstp = ('timestamp','max'),
 ).reset_index()

# %%
from datetime import datetime, timedelta
from dateutil import tz, relativedelta
current_time = datetime.now()

dfGrpTx['StartEx'] = pd.to_datetime(dfGrpTx['minTmstp'], unit = 's')
dfGrpTx['EndEx'] = pd.to_datetime(dfGrpTx['maxTmstp'], unit = 's')

dfGrpTx

strTaExport = '../results/_dfExchangeTradingTime_export_'+current_time.strftime('%Y-%m-%d_%H_%M_%S')+'.xlsx' 
dfGrpTx.to_excel(strTaExport, index = False)
print('Exchange Trading Timewindow dataframe export file stored to' + strTaExport)

# %%
