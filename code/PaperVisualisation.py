####################################################################################################
# plots for paper publication
####################################################################################################

# %% 
# define functions and imports
from datetime import datetime, timedelta
from dateutil import tz, relativedelta
import calendar
import pandas as pd
import plotly.graph_objects as go
import numpy as np
from numpy import *

# %%
# import df from previous step

# strTime = current_time.strftime('%Y-%m-%d_%H_%M_%S') 
strTime = '2020-09-13_11_52_29'
dfPaper = pd.read_excel(r'../data/_dfPaperPlotsperMonth_export_'+strTime+'.xlsx')


# %%
# line plots for specific indicators
# ti_sma1-50 ti_sma1-150 ti_sma5-150 ti_sma1-200 ti_sma2-200 
# ti_trb50 ti_trb150 ti_trb200 ti_macd ti_roc ti_rsi ti_bb
# ti_obv_sma1-50 ti_obv_sma1-150 ti_obv_sma5-150 ti_obv_sma1-200 ti_obv_sma2-200

dfPlot = dfPaper[(dfPaper['TaType'] == 'ti_rsi')]
month = dfPlot['End']
LR = dfPlot['LR']
GR = dfPlot['GR']

fig = go.Figure()
# Create and style traces
fig.add_trace(go.Scatter(x=month, y=GR, name='GR',
                         line=dict(color='green', width=4,
                              dash='dash') # dash options include 'dash', 'dot', and 'dashdot'
))
fig.add_trace(go.Scatter(x=month, y=LR, name='LR',
                         line=dict(color='red', width=4, dash='dot')))

# Edit the layout
fig.update_layout(title='LR and GR',
                   xaxis_title='Month',
                   yaxis_title='number of gains and losses',
                   width=1000, height=500
)

fig.show()