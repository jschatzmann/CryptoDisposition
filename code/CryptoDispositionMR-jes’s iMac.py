####################################################################################################
# Model 2 using Multiple Regression to examine influence of TA indicators on losses and gains
####################################################################################################

# %% 
# define functions and imports
from datetime import datetime, timedelta
from dateutil import tz, relativedelta
from matplotlib.pyplot import annotate
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.genmod.families.links import NegativeBinomial
plt.style.use('ggplot')
import numpy as np
from numpy import *
import statsmodels.api as sm
import statsmodels.formula.api as smf
from patsy import dmatrices
#import patsy
import seaborn as sns
from statsmodels.formula.api import glm
import os, shutil

current_time = datetime.now()

dt_start = datetime(2013, 1, 1, 0, 0, 0, 0, tzinfo = tz.UTC)
dt_end = datetime(2019, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)

def df_per_timeframe(df, dt_start, dt_end): # return df reduced to passed start / end datetime
    # datetime(year, month, day, hour, minute, second, microsecond)
    df = df[df['timestampOhlc'] >= int(dt_start.timestamp())]
    df = df[df['timestampOhlc']<= int(dt_end.timestamp())]
    return df

# remove old tex files if existing
strPath = "../results/02_M2"
for filename in os.listdir(strPath):
    file_path = os.path.join(strPath, filename)
    try:
        if os.path.isfile(file_path) or os.path.islink(file_path):
            os.unlink(file_path)
        elif os.path.isdir(file_path):
            shutil.rmtree(file_path)
    except Exception as e:
        print('Failed to delete %s. Reason: %s' % (file_path, e))

# %%
# import dfTA
# strTime = current_time.strftime('%Y-%m-%d_%H_%M_%S') 
#strTime = '2020-08-08_14_27_18'
strTime = '2020-10-03_12_32_27' # including Odean parameters

dfTa = pd.read_csv(r'../data/_dfTA_export_'+strTime+'.csv')
dfTa = df_per_timeframe(dfTa, dt_start, dt_end)


cols_to_keep = ['timestampOhlc', 'avg_open', 'avg_high', 'avg_low',
       'avg_close', 'avg_volume', 'tmstTxRnd', 'valSum', 'txCnt', '_merge',
       'tmstTxRndDay', 'tmstOhlc_rdbl', 'tmstTxRnd_rdbl', 'tmstTxRndDay_rdbl',

       'ti_GR', 'ti_LR', 'ti_avg_OpenCLose', 'ti_GR_LR',

       'ti_sma2', 'ti_sma5', 'ti_sma50', 'ti_sma150', 'ti_sma200',
       'ti_sma1-50_GR', 'ti_sma1-50_LR', 'ti_sma1-150_GR', 'ti_sma1-150_LR',
       'ti_sma5-150_GR', 'ti_sma5-150_LR', 'ti_sma1-200_GR', 'ti_sma1-200_LR',
       'ti_sma2-200_GR', 'ti_sma2-200_LR', 
       'ti_sma1-50_GR_LR', 'ti_sma1-150_GR_LR', 'ti_sma5-150_GR_LR', 'ti_sma1-200_GR_LR', 'ti_sma2-200_GR_LR', 
       
       'ti_trb50_hband', 'ti_trb50_mband', 'ti_trb50_lband', 'ti_trb150_hband', 'ti_trb150_mband',
       'ti_trb150_lband', 'ti_trb200_hband', 'ti_trb200_mband', 'ti_trb200_lband', 
       
       'ti_trb50_GR', 'ti_trb50_LR', 'ti_trb150_GR', 'ti_trb150_LR', 'ti_trb200_GR', 'ti_trb200_LR', 
       'ti_trb50_GR_LR', 'ti_trb150_GR_LR', 'ti_trb200_GR_LR', 
       
       'ti_macd', 'ti_macd_GR', 'ti_macd_LR', 'ti_macd_GR_LR', 
       
       'ti_roc', 'ti_roc_GR', 'ti_roc_LR','ti_roc_GR_LR', 
       
       'ti_obv', 'ti_obv_sma2', 'ti_obv_sma5', 'ti_obv_sma50', 'ti_obv_sma150', 'ti_obv_sma200', 
       
       'ti_obv_sma1-50_GR', 'ti_obv_sma1-50_LR', 'ti_obv_sma1-150_GR', 'ti_obv_sma1-150_LR',
       'ti_obv_sma5-150_GR', 'ti_obv_sma5-150_LR', 'ti_obv_sma1-200_GR',
       'ti_obv_sma1-200_LR', 'ti_obv_sma2-200_GR', 'ti_obv_sma2-200_LR',

       'ti_obv_sma1-50_GR_LR', 'ti_obv_sma1-150_GR_LR', 'ti_obv_sma5-150_GR_LR', 'ti_obv_sma1-200_GR_LR',
       'ti_obv_sma2-200_GR_LR', 
       
       'ti_rsi', 'ti_rsi_GR', 'ti_rsi_LR', 'ti_rsi_GR_LR', 
       
       'ti_bb_bbm', 'ti_bb_bbh', 'ti_bb_bbl', 'ti_bb_GR', 'ti_bb_LR', 'ti_bb_GR_LR']

# limit columns for MR
'''
cols_to_keep = ['timestampOhlc', 'avg_open', 'avg_high', 'avg_low', 'avg_close',
       'avg_volume', 'tmstTxRnd', 'valSum', 'txCnt', '_merge', 'tmstOhlc_rdbl',
       'ti_sma2', 'ti_sma5','ti_macd', 'ti_rsi'
    ]
'''

dfTaMr = dfTa[cols_to_keep]
# update NaN in txCnt as zero = zero transactions happened
# dfTaMr['txCnt'].sum() = 27550837.0
dfTaMr['txCnt'] = dfTaMr['txCnt'].fillna(0)
# after update sum still the same = 27550837.0

# %%

# %%
##################################################
# Check for Linearity txCnt and valSum
##################################################

# txCnt
depVar = 'txCnt'
strPath = "../results/02_M2/LaTex_M2_01a-Linearity"+depVar+".tex"
FileObj = open(strPath, 'a')
FileObj.write("\section{Check for linearity on variable " +depVar+'}\n\n')
# continuous variables - stepwise remove with the highest p-values
# txCnt - stepwise removal for p>0.05
strModel = depVar +' ~ ti_avg_OpenCLose + ti_sma2 + ti_macd + ti_roc +ti_obv + ti_rsi + ti_bb_bbm + ti_trb50_lband'
FileObj.write("Model: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')
# remove ti_sma2
strModel = depVar +' ~ ti_avg_OpenCLose + ti_macd + ti_roc + ti_obv + ti_rsi + ti_bb_bbm + ti_trb50_lband'
FileObj.write("Model - removed SMA2: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')
FileObj.close()

# valSum
depVar = 'valSum'
strPath = "../results/02_M2/LaTex_M2_01b-Linearity"+depVar+".tex"
FileObj = open(strPath, 'a')
FileObj.write("\section{Check for linearity on variable " +depVar+'}\n\n')
# valSum - stepwise removal for p>0.05
strModel = depVar +' ~ ti_avg_OpenCLose + ti_sma2 + ti_macd + ti_roc + ti_obv + ti_rsi + ti_bb_bbm + ti_trb50_lband'
FileObj.write("Model: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')
# remove ti_bb_bbm
strModel = depVar +' ~ ti_avg_OpenCLose + ti_sma2 + ti_macd + ti_roc + ti_obv + ti_rsi + ti_trb50_lband'
FileObj.write("Model - removed BBM: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')
# remove ti_sma2
strModel = depVar +' ~ ti_avg_OpenCLose + ti_macd + ti_roc + ti_obv + ti_rsi + ti_trb50_lband'
FileObj.write("Model - removed SMA2: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')

FileObj.close()

# %%
# test flight - testing a reduced model

# txCnt
depVar = 'txCnt'
#strPath = "../results/02_M2/LaTex_M2_01a-Linearity"+depVar+".tex"
#FileObj = open(strPath, 'a')
#FileObj.write("\section{Check for linearity on variable " +depVar+'}\n\n')
# continuous variables - stepwise remove with the highest p-values
# txCnt - stepwise removal for p>0.05
strModel = depVar +' ~ ti_avg_OpenCLose + ti_macd + ti_roc +ti_obv + ti_rsi'
strModel = depVar +' ~ ti_avg_OpenCLose + ti_sma2 + ti_macd + ti_roc +ti_obv + ti_rsi + ti_bb_bbm + ti_trb50_lband'
#FileObj.write("Model: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
#FileObj.write(mod.summary().as_latex()+'\n\n')

print(mod.summary())

# %%
# test flight - reduced model plots

plt.rc("figure", figsize=(20,20))
plt.rc("font", size=14)

# this produces our six partial regression plots
#fig = plt.figure(figsize=(20,12))
#fig = sm.graphics.plot_partregress_grid(mod, fig=fig)

# Component-Component plus Residual (CCPR) Plots
#fig = sm.graphics.plot_ccpr(mod, "ti_macd")
#fig.tight_layout(pad=1.0)

# CCPR grid
#fig = sm.graphics.plot_ccpr_grid(mod)
#fig.tight_layout(pad=1.0)

# Single Variable Regression Diagnostics
mod=mod.sample(1000)
fig = sm.graphics.plot_regress_exog(mod, "ti_avg_OpenCLose")
fig.tight_layout(pad=1.0)


# %% empty cell


# %%
# categorical variables - stepwise remove with the highest p-values
dfTaMr['ti_sma1t50_GR_LR'] = dfTaMr['ti_sma1-50_GR_LR']
dfTaMr['ti_obv_sma2t200_GR_LR'] = dfTaMr['ti_obv_sma2-200_GR_LR']
dfTaMr['ti_obv_sma1t50_GR_LR'] = dfTaMr['ti_obv_sma1-50_GR_LR']

# --------------------------------------------------
# categorical
# --------------------------------------------------

# txCnt - categorical
depVar = 'txCnt'
strPath = "../results/02_M2/LaTex_M2_02a-LinearityCategorical"+depVar+".tex"
FileObj = open(strPath, 'a')
FileObj.write("\section{Check for linearity categorical on variable " +depVar+'}\n\n')
# txCnt - stepwise removal for p>0.05
strModel = depVar +' ~ ti_GR_LR + ti_sma1t50_GR_LR + ti_macd_GR_LR + ti_roc_GR_LR + ti_obv_sma1t50_GR_LR + ti_rsi_GR_LR + ti_trb50_GR_LR'
FileObj.write("Model: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')
# remove ti_rsi_GR_LR
strModel = depVar +' ~ ti_GR_LR + ti_sma1t50_GR_LR + ti_macd_GR_LR + ti_roc_GR_LR + ti_obv_sma1t50_GR_LR + ti_trb50_GR_LR'
FileObj.write("Model - removed RSI: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')
FileObj.close()

# valSum - categorical
depVar = 'valSum'
strPath = "../results/02_M2/LaTex_M2_02b-LinearityCategorical"+depVar+".tex"
FileObj = open(strPath, 'a')
FileObj.write("\section{Check for linearity categorical on variable " +depVar+'}\n\n')
# valSum - stepwise removal for p>0.05
strModel = depVar +' ~ ti_GR_LR + ti_sma1t50_GR_LR + ti_macd_GR_LR + ti_roc_GR_LR + ti_obv_sma1t50_GR_LR + ti_rsi_GR_LR + ti_trb50_GR_LR'
FileObj.write("Model: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')
# remove ti_rsi_GR_LR
strModel = depVar +' ~ ti_GR_LR + ti_sma1t50_GR_LR + ti_macd_GR_LR + ti_roc_GR_LR + ti_obv_sma1t50_GR_LR + ti_trb50_GR_LR'
FileObj.write("Model removed RSI: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')
# remove ti_GR_LR (Odean average)
strModel = depVar +' ~ ti_sma1t50_GR_LR + ti_macd_GR_LR + ti_roc_GR_LR + ti_obv_sma1t50_GR_LR + ti_trb50_GR_LR'
FileObj.write("Model removed Odean average: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = smf.ols(strModel, data=dfTaMr).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')

FileObj.close()

# limit the dataframe to 2016 to 2019 to view statistics
#dt_start = datetime(2016, 1, 1, 0, 0, 0, 0, tzinfo = tz.UTC)
#dt_end = datetime(2019, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)
#dfTaMrLimited = df_per_timeframe(dfTaMr, dt_start, dt_end)
#mod = smf.ols(strModel, data=dfTaMr).fit()

# %%
##################################################
# Alternative - Poisson Regression - but mean is not variance, when variance > mean = overdispersed
##################################################

# variance ratio = large values above 1 mean overdispersed
print(var(dfTaMr.txCnt)/mean(dfTaMr.txCnt))
depVar = 'txCnt'

strPath = "../results/02_M2/LaTex_M2_03-Poisson"+depVar+".tex"
FileObj = open(strPath, 'a')
FileObj.write("\section{Poisson on variable " +depVar+'}\n\n')

strModel = depVar +' ~ ti_avg_OpenCLose + ti_sma2 + ti_macd + ti_roc + ti_obv + ti_rsi + ti_bb_bbm + ti_trb50_lband'

# array version
#response, predictors = dmatrices(strModel, dfTaMr, return_type='dataframe')
#mod = sm.GLM(response, predictors, family=sm.families.Poisson()).fit()

# formula version
FileObj.write("Model: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
mod = glm(strModel, dfTaMr, family=sm.families.Poisson()).fit()
FileObj.write(mod.summary().as_latex()+'\n\n')
FileObj.write('AIC: ' + str(round(mod.aic,4)) + ' BIC: ' + str(round(mod.bic,4)) + '\n\n')
# check for adequacy of the GLM from its residuals - see 'Pearson chi2'

FileObj.close()

# %%
# Dispersion Calculation for poisson regression
import statsmodels.formula.api as smf

def ct_response(row):
    "Calculate response observation for Cameron-Trivedi dispersion test"
    y = row['txCnt']
    m = row['tx_mu']
    return ((y - m)**2 - y) / m

ct_data = dfTaMr.copy()
ct_data['tx_mu'] = mod.mu
ct_data['ct_resp'] = ct_data.apply(ct_response, axis=1)
# Linear regression of auxiliary formula
ct_results = smf.ols('ct_resp ~ tx_mu - 1', ct_data).fit()
# Construct confidence interval for alpha, the coefficient of tx_mu
alpha_ci95 = ct_results.conf_int(0.05).loc['tx_mu']
alpha_ci99p999 = ct_results.conf_int(0.001).loc['tx_mu']
print('\nC-T dispersion test: alpha = {:5.3f}, 95% CI = ({:5.3f}, {:5.3f})'
        .format(ct_results.params[0], alpha_ci95.loc[0], alpha_ci95.loc[1])
        + " average: "+ str((alpha_ci95.loc[0] + alpha_ci95.loc[1])/2))
print('\nC-T dispersion test: alpha = {:5.3f}, 99,999% CI = ({:5.3f}, {:5.3f})'
        .format(ct_results.params[0], alpha_ci99p999.loc[0], alpha_ci99p999.loc[1])
        + " average: "+ str((alpha_ci99p999.loc[0] + alpha_ci99p999.loc[1])/2))

# %%
##################################################
# Negative Binomial regression
##################################################

depVar = 'txCnt'
#sns.distplot(dfTaMr['txCnt'])

strPath = "../results/02_M2/LaTex_M2_04-NegBinomial"+depVar+".tex"
FileObj = open(strPath, 'a')
FileObj.write("\section{Negative Binomial on variable " +depVar+'}\n\n')

strModel = depVar +' ~ ti_avg_OpenCLose + ti_sma2 + ti_macd + ti_roc +ti_obv + ti_rsi + ti_bb_bbm + ti_trb50_lband'
FileObj.write("Model: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
nb_results = glm(strModel, dfTaMr, family=sm.families.NegativeBinomial(alpha=0.525)).fit()
FileObj.write(nb_results.summary().as_latex()+'\n\n')
FileObj.write('AIC: ' + str(round(nb_results.aic,4)) + ' BIC: ' + str(round(nb_results.bic,4)) + '\n\n')

# remove SMA2 p=0.999
strModel = depVar +' ~ ti_avg_OpenCLose + ti_macd + ti_roc + ti_obv + ti_rsi + ti_bb_bbm + ti_trb50_lband'
FileObj.write("Model - removed SMA2: \\begin{verbatim}"+strModel+'\\end{verbatim}\n\n')
nb_results = glm(strModel, dfTaMr, family=sm.families.NegativeBinomial(alpha=0.525)).fit()
FileObj.write(nb_results.summary().as_latex()+'\n\n')
FileObj.write('AIC: ' + str(round(nb_results.aic,4)) + ' BIC: ' + str(round(nb_results.bic,4)) + '\n\n')

# optional for test - remove ROC p=0.035
#strModel = depVar +' ~ ti_avg_OpenCLose + ti_macd + ti_obv + ti_rsi + ti_bb_bbm + ti_trb50_lband'
# array based
#response, predictors = dmatrices(strModel, dfTaMr, return_type='dataframe')
#nb_results = sm.GLM(response, predictors,
#                    family=sm.families.NegativeBinomial(alpha=0.46)).fit()
# formula based
#nb_results = glm(strModel, dfTaMr, family=sm.families.NegativeBinomial(alpha=1)).fit()
#print(nb_results.summary())

FileObj.close()

# %%
#################################################
#-----------------------------------------------
# Plots on variables
#-----------------------------------------------
#################################################
# %%
# histogram of the counts
strPath = "../results/HistogramTxCount.pdf"
dfTaMr.txCnt.plot(kind='hist', color='orange', edgecolor='black', figsize=(10,7), bins = 60)
plt.title('Distribution of TxCount (txCnt)', size=18)
plt.xlabel('TxCount', size=12)
plt.ylabel('Frequency', size=12)
plt.savefig(strPath)
plt.show()

# %%
# histogram of the value
dfTaMr.valSum.plot(kind='hist', color='lightgreen', edgecolor='black', figsize=(10,7), bins = 60)
plt.title('Distribution of Value (valSum)', size=24)
plt.xlabel('Tx Value', size=18)
plt.ylabel('Frequency', size=18)

# %%
# combined histograms Tx count for LR and GR (indicator specific, e.g. RSI)
dfTaMr[dfTaMr['ti_rsi_GR_LR'] == 'LR'].txCnt.plot(kind='hist', color='r', edgecolor='black', alpha=0.5, figsize=(10, 7), bins = 60)
dfTaMr[dfTaMr['ti_rsi_GR_LR'] == 'GR'].txCnt.plot(kind='hist', color='g', edgecolor='black', alpha=0.5, figsize=(10, 7), bins = 60)
plt.legend(labels=['RSI LR', 'RSI GR'])
plt.title('Distribution of LR + GR to Tx', size=24)
plt.xlabel('TxCount (amounts)', size=18)
plt.ylabel('Frequency', size=18);

# %%
# histogram of sum of value of LR and GR (indicator specific, e.g. RSI)
dfTaMr[dfTaMr['ti_rsi_GR_LR'] == 'LR'].valSum.plot(kind='hist', color='r', edgecolor='black', alpha=0.5, figsize=(10, 7), bins = 60)
dfTaMr[dfTaMr['ti_rsi_GR_LR'] == 'GR'].valSum.plot(kind='hist', color='g', edgecolor='black', alpha=0.5, figsize=(10, 7), bins = 60)
plt.legend(labels=['RSI LR', 'RSI GR'])
plt.title('Distribution of LR + GR to Tx', size=24)
plt.xlabel('Tx Value (amounts)', size=18)
plt.ylabel('Frequency', size=18);

# %%
# sample size scatter plots to view data points / identify potential correlation
# ti_sma1-150_GR_LR / ti_rsi_GR_LR

#sample_1 = dfTaMr[dfTaMr['ti_sma1-150_GR_LR'] == 'LR'].sample(1000)
#sample_1 = dfTaMr[dfTaMr['ti_sma1-150_GR_LR'] == 'LR'].sample(1000)
#ax1 = sample_1.plot(kind='scatter', x='ti_rsi', y='txCnt', color='r', alpha=0.5, figsize=(10, 7))
#ax1 = sample_1.plot(kind='scatter', x='ti_sma2', y='txCnt', color='r', alpha=0.5, figsize=(10, 7))

'''
'ti_avg_OpenCLose'
'ti_sma2', 'ti_sma5', 'ti_sma50', 'ti_sma150', 'ti_sma200',
'ti_trb50_hband', 'ti_trb50_mband', 'ti_trb50_lband', 'ti_trb150_hband', 'ti_trb150_mband',
'ti_trb150_lband', 'ti_trb200_hband', 'ti_trb200_mband', 'ti_trb200_lband', 
'ti_macd', 'ti_roc', 'ti_obv', 'ti_obv_sma2', 'ti_obv_sma5', 'ti_obv_sma50', 'ti_obv_sma150', 'ti_obv_sma200', 
'ti_rsi', 'ti_bb_bbm', 'ti_bb_bbh', 'ti_bb_bbl'
'''

dfTaMr.sample(8000).plot(kind='scatter', x='txCnt', y='ti_rsi', color='b', alpha=0.5, figsize=(10,7))

#sample_2 = dfTaMr[dfTaMr['ti_sma1-150_GR_LR'] == 'GR'].sample(1000)
#sample_2.plot(kind='scatter', x='ti_rsi', y='txCnt', color='g', alpha=0.5, figsize=(10, 7), ax=ax1)
#sample_2.plot(kind='scatter', x='ti_sma2', y='txCnt', color='g', alpha=0.5, figsize=(10, 7), ax=ax1)

#plt.legend(labels=['LR','GR'])
#plt.title('Relationship between RSI and Tx Count (sample of 500)', size=20)
plt.xlabel('txCnt', size=18)
plt.ylabel('Tech. Indicator', size=18);
plt.xscale('log')
plt.yscale('log')

# %%
##################################################
# scatter plots matrices for valSum and txCnt, visual pre-check for linear relatinships
##################################################
import plotly.express as px
df = dfTaMr
#depVar = 'txCnt'
depVar = 'valSum'
fig = px.scatter_matrix(df,
    #valSum or txCnt
    #dimensions=[depVar, 'ti_avg_OpenCLose', "ti_sma2", "ti_sma5", "ti_sma50", "ti_sma150", "ti_sma200"],
    dimensions=[depVar, 
    'ti_trb50_hband', 'ti_trb50_mband', 'ti_trb50_lband', 
    'ti_trb150_hband', 'ti_trb150_mband', 'ti_trb150_lband', 
    'ti_trb200_hband', 'ti_trb200_mband', 'ti_trb200_lband', 
    ],
    #dimensions=[depVar, 'ti_macd', 'ti_roc', 'ti_obv'],
    #dimensions=[depVar, 'ti_obv', 'ti_obv_sma2', 'ti_obv_sma5', 'ti_obv_sma50', 'ti_obv_sma150', 'ti_obv_sma200'],
    #dimensions=[depVar, 'ti_rsi', 'ti_bb_bbm', 'ti_bb_bbh', 'ti_bb_bbl'],
    color=depVar,
    width=1000, height=1000)

fig.show()

# %% 
##################################################
# Plot 1 - Heatmapz - check for any linear relationship
##################################################

from heatmap import heatmap, corrplot

#depVar = 'txCnt'
depVar = 'valSum'

cols_to_keep=[depVar,'ti_avg_OpenCLose', "ti_sma2", "ti_sma5", "ti_sma50", "ti_sma150", "ti_sma200",
'ti_macd', 'ti_roc', 'ti_obv', 'ti_obv_sma2', 'ti_obv_sma5', 'ti_obv_sma50', 'ti_obv_sma150', 'ti_obv_sma200',
'ti_rsi', 'ti_bb_bbm', 'ti_bb_bbh', 'ti_bb_bbl',
'ti_trb50_hband', 'ti_trb50_mband', 'ti_trb50_lband', 
'ti_trb150_hband', 'ti_trb150_mband', 'ti_trb150_lband', 
'ti_trb200_hband', 'ti_trb200_mband', 'ti_trb200_lband', ]

cols_to_keep=[depVar,'ti_avg_OpenCLose', 'ti_sma2', 'ti_macd','ti_roc', 'ti_obv', 'ti_rsi', 'ti_bb_bbm','ti_trb50_lband' ]

#cols_to_keep=[depVar,'ti_avg_OpenCLose', "ti_sma2", "ti_sma5", "ti_sma50", "ti_sma150", "ti_sma200"]
#cols_to_keep=[depVar, 'ti_macd', 'ti_roc', 'ti_obv']
#cols_to_keep=[depVar, 'ti_obv', 'ti_obv_sma2', 'ti_obv_sma5', 'ti_obv_sma50', 'ti_obv_sma150', 'ti_obv_sma200']
#cols_to_keep=[depVar, 'ti_rsi', 'ti_bb_bbm', 'ti_bb_bbh', 'ti_bb_bbl']
#cols_to_keep = [depVar, 
#    'ti_trb50_hband', 'ti_trb50_mband', 'ti_trb50_lband', 
#    'ti_trb150_hband', 'ti_trb150_mband', 'ti_trb150_lband', 
#    'ti_trb200_hband', 'ti_trb200_mband', 'ti_trb200_lband', 
#    ]

dfCorrPlt = dfTa[cols_to_keep]
f = plt.figure(figsize=(12, 12))
corrplot(dfCorrPlt.corr(), size_scale=300);
# txCnt corr()
#ti_avg_OpenCLose	ti_sma2	ti_obv	ti_bb_bbh	ti_trb50_lband
# 0,2026 	 0,2026 	 0,2614 	 0,2093 	 0,1845 

# valSum corr()
#ti_avg_OpenCLose	ti_sma200	ti_obv	ti_bb_bbm	ti_trb200_lband
#-0,2919 	-0,2936 	 0,3381 	-0,2918 	-0,3101 

# write files
#round(dfCorrPlt.corr(),4).to_excel(r'../results/_dfCorrPlt'+ depVar +'_' +current_time.strftime('%Y-%m-%d_%H_%M_%S')+'.xlsx')
#round(dfCorrPlt.corr(),4).to_latex(r'../results/_dfCorrPlt'+ depVar +'_' +current_time.strftime('%Y-%m-%d_%H_%M_%S')+'.tex')

# Timout
#sns_plot = sns.pairplot(dfCorrPlt, kind="reg")
#sns_plot.savefig("../results/CorrPlotSNS"+ depVar +".pdf")

corr = dfCorrPlt.corr()
corr.style.background_gradient(cmap='coolwarm')

f.savefig("../results/CorrPlot"+ depVar +".pdf")

# %%
##################################################
# Plot 2 - Seaborn version
##################################################

sns.set_theme(style="white")
# Compute the correlation matrix
corr = dfCorrPlt.corr()
# Generate a mask for the upper triangle
mask = np.triu(np.ones_like(corr, dtype=bool))
# Set up the matplotlib figure
f, ax = plt.subplots(figsize=(11, 9))
# Generate a custom diverging colormap
#cmap = sns.diverging_palette(230, 20, as_cmap=True)

x_axis_labels = [depVar, 'OV', 'SMA2', 'MACD', 'ROC', 'OBV', 'RSI', 'BB-MB', 'TRB50-LB'] # labels for x-axis
y_axis_labels = x_axis_labels # labels for y-axis

# Draw the heatmap with the mask and correct aspect ratio
#sns.heatmap(corr, mask=mask, cmap="YlGnBu", vmax=.5, vmin=-.7, center=0, annot=True,
sns.heatmap(corr, mask=mask, cmap="coolwarm", center=0, annot=True, fmt=".3f", 
            xticklabels = x_axis_labels, yticklabels = y_axis_labels,
            square=True, linewidths=1, cbar_kws={"shrink": .5})

f.savefig("../results/CorrPlotSns"+ depVar +".pdf")

# %%
# red line marks the perfect normal distribution, if the blue line lies close to the red,
# it indicates that the residuals are almost normally distributed, 
qqfig = sm.qqplot(mod.resid, line='r')
#qqfig

# %%
# residual plot of standardized residuals
stdres = pd.DataFrame(mod.resid_pearson)
plt.plot(stdres, 'b.', ls='None', alpha=0.3)
l = plt.axhline(y=0, color='r')
plt.ylabel('Standardized Residual')
plt.xlabel('Observation Number');

# influence plot - visualise how much influence a single observation has on the model
#levfig = sm.graphics.influence_plot(mod, size=8)
