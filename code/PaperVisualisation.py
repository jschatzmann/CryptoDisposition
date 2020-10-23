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
import plotly.express as px
import plotly.io as pio
import plotly
import numpy as np
from plotly.subplots import make_subplots

#from numpy import *


dt_start = datetime(2013, 1, 1, 0, 0, 0, 0, tzinfo = tz.UTC)
dt_end = datetime(2019, 12, 31, 23, 59, 59, 0, tzinfo = tz.UTC)

def df_per_timeframe(df, dt_start, dt_end): # return df reduced to passed start / end datetime
    # datetime(year, month, day, hour, minute, second, microsecond)
    df = df[df['timestampOhlc'] >= int(dt_start.timestamp())]
    df = df[df['timestampOhlc']<= int(dt_end.timestamp())]
    return df

# %%
# import df from previous step with monthly aggregated data

# strTime = current_time.strftime('%Y-%m-%d_%H_%M_%S') 
strTime = '2020-09-13_11_52_29'
dfPaper = pd.read_excel(r'../data/_dfPaperPlotsperMonth_export_'+strTime+'.xlsx')

# import dfTA for plots containing valSum and txCnt
# strTime = current_time.strftime('%Y-%m-%d_%H_%M_%S') 
strTime = '2020-08-08_14_27_18'

dfTa = pd.read_csv(r'../data/_dfTA_export_'+strTime+'.csv')
dfTa = df_per_timeframe(dfTa, dt_start, dt_end)

# limit to relevant columns
# '''
cols_to_keep = ['timestampOhlc', 'avg_open', 'avg_high', 'avg_low', 'avg_close',
       'avg_volume', 'tmstTxRnd', 'valSum', 'txCnt', '_merge', 'tmstOhlc_rdbl',
       'ti_sma2', 'ti_sma5','ti_macd', 'ti_rsi',
       'ti_macd_GR', 'ti_macd_LR', 'ti_macd_GR_LR', 
    ]
# '''

dfTa = dfTa[cols_to_keep]

# create column for YYYY-MM monthly values of transaction count and valSum
#dfTa['tmstOhlc_rdbl'] = pd.to_datetime(dfTa['tmstOhlc_rdbl'])
#dfTaGrp = dfTa.set_index('tmstOhlc_rdbl').resample('M')['valSum'].sum()
dfTa['GrpYear'] = pd.DatetimeIndex(dfTa['tmstOhlc_rdbl']).year
dfTa['GrpMonth'] = pd.DatetimeIndex(dfTa['tmstOhlc_rdbl']).month

dfTaGrp = dfTa.groupby(
    ['GrpYear', 'GrpMonth']).agg(
        valSum = ('valSum', 'sum'),
        txCnt = ('txCnt', 'sum'),
        avgClose = ('avg_close', 'mean'),
    ).reset_index()
dfTaGrp['GrpYearMonth'] = dfTaGrp['GrpYear'].astype(str) + '-' + dfTaGrp['GrpMonth'].astype(str)

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
                   width=800, height=500
)

fig.show()
# %% 
# Split Violin Plot - confirming disposition effect
# for confirming technical indicators
#[ (df['TaType']== 'Odean_GrLr') | (df['TaType']== 'ti_macd')| (df['TaType']== 'ti_roc')
#|(df['TaType']== 'ti_rsi')| (df['TaType']== 'ti_obv_sma5-150')| (df['TaType']== 'ti_obv_sma2-200') ]

fig = go.Figure()
df = dfPaper

fig.add_trace(go.Violin(x=df['TaType'][ (df['TaType']== 'Odean_GrLr') | (df['TaType']== 'ti_macd')| (df['TaType']== 'ti_roc')| 
                                        (df['TaType']== 'ti_rsi')| (df['TaType']== 'ti_obv_sma5-150')| (df['TaType']== 'ti_obv_sma2-200') ],
                        y=df['GR'][ (df['TaType']== 'Odean_GrLr') | (df['TaType']== 'ti_macd')| (df['TaType']== 'ti_roc')| 
                                        (df['TaType']== 'ti_rsi')| (df['TaType']== 'ti_obv_sma5-150')| (df['TaType']== 'ti_obv_sma2-200') ],
                        legendgroup='GR', scalegroup='Yes', name='GR',
                        side='positive',
                        line_color='green')
)
fig.add_trace(go.Violin(x=df['TaType'][ (df['TaType']== 'Odean_GrLr') | (df['TaType']== 'ti_macd')| (df['TaType']== 'ti_roc')| 
                                        (df['TaType']== 'ti_rsi')| (df['TaType']== 'ti_obv_sma5-150') | (df['TaType']== 'ti_obv_sma2-200') ],
                        y=df['LR'][ (df['TaType']== 'Odean_GrLr') | (df['TaType']== 'ti_macd')| (df['TaType']== 'ti_roc')| 
                                        (df['TaType']== 'ti_rsi')| (df['TaType']== 'ti_obv_sma5-150')| (df['TaType']== 'ti_obv_sma2-200') ],
                        legendgroup='LR', scalegroup='Yes', name='LR',
                        side='negative',
                        line_color='red')
)


fig.update_xaxes(
    ticktext=['Odean average','MACD','ROC','OBV 5-150','OBV 2-200','RSI'],
    tickvals=['Odean_GrLr','ti_macd','ti_roc','ti_obv_sma5-150','ti_obv_sma2-200','ti_rsi'],
)

fig.update_traces(meanline_visible=True)
fig.update_layout(violingap=0, violinmode='overlay', legend=dict(yanchor="top", y=0.99, xanchor="left",x=0.01),)
fig.update_layout(#title='LR and GR',
                   xaxis_title='Technical Indicators',
                   yaxis_title='Number of gains and losses realised',
                   width=800, height=500,
                   xaxis=dict(range=[-0.5, 5.5])
)

fig.show()
fig.write_image("../results/ViolinPlotDispEffConf1.pdf")

# for confirming technical indicators
#[ (df['TaType']== 'ti_sma1-50')| (df['TaType']== 'ti_sma1-150')| (df['TaType']== 'ti_sma5-150')
#| (df['TaType']== 'ti_sma1-200')| (df['TaType']== 'ti_sma2-200')]

fig = go.Figure()

fig.add_trace(go.Violin(x=df['TaType'][ (df['TaType']== 'ti_sma1-50')| (df['TaType']== 'ti_sma1-150')| (df['TaType']== 'ti_sma5-150')
                                        | (df['TaType']== 'ti_sma1-200')| (df['TaType']== 'ti_sma2-200')],
                        y=df['GR'][ (df['TaType']== 'ti_sma1-50')| (df['TaType']== 'ti_sma1-150')| (df['TaType']== 'ti_sma5-150')
                                        | (df['TaType']== 'ti_sma1-200')| (df['TaType']== 'ti_sma2-200')],
                        legendgroup='GR', scalegroup='Yes', name='GR',
                        side='positive',
                        line_color='green')
)
fig.add_trace(go.Violin(x=df['TaType'][ (df['TaType']== 'ti_sma1-50')| (df['TaType']== 'ti_sma1-150')| (df['TaType']== 'ti_sma5-150')
                                        | (df['TaType']== 'ti_sma1-200')| (df['TaType']== 'ti_sma2-200')],
                        y=df['LR'][ (df['TaType']== 'ti_sma1-50')| (df['TaType']== 'ti_sma1-150')| (df['TaType']== 'ti_sma5-150')
                                        | (df['TaType']== 'ti_sma1-200')| (df['TaType']== 'ti_sma2-200')],
                        legendgroup='LR', scalegroup='Yes', name='LR',
                        side='negative',
                        line_color='red')
)

fig.update_xaxes(
    ticktext=['SMA 1-50','SMA 1-150','SMA 5-150','SMA 1-200','SMA 2-200'],
    tickvals=['ti_sma1-50','ti_sma1-150','ti_sma5-150','ti_sma1-200','ti_sma2-200'],
)

fig.update_traces(meanline_visible=True)
fig.update_layout(violingap=0, violinmode='overlay', legend=dict(yanchor="top", y=0.99, xanchor="left",x=0.01),)
fig.update_layout(#title='LR and GR',
                   xaxis_title='Technical Indicators',
                   yaxis_title='Number of gains and losses realised',
                   width=800, height=500,
                   xaxis=dict(range=[-0.5, 4.5])
)

fig.show()
fig.write_image("../results/ViolinPlotDispEffConf2.pdf")


# %% 
# Split Violin Plot - NOT confirming disposition effect
# for TRB and BB non-confirming technical indicators
#[ (df['TaType']== 'ti_trb50') | (df['TaType']== 'ti_trb150')| (df['TaType']== 'ti_trb200')| (df['TaType']== 'ti_bb')]

fig = go.Figure()
df = dfPaper

fig.add_trace(go.Violin(x=df['TaType'][ (df['TaType']== 'ti_trb50') | (df['TaType']== 'ti_trb150')| (df['TaType']== 'ti_trb200')| (df['TaType']== 'ti_bb')],
                        y=df['GR'][ (df['TaType']== 'ti_trb50') | (df['TaType']== 'ti_trb150')| (df['TaType']== 'ti_trb200')| (df['TaType']== 'ti_bb')],
                        legendgroup='GR', scalegroup='Yes', name='GR',
                        side='positive',
                        line_color='green')
)
fig.add_trace(go.Violin(x=df['TaType'][ (df['TaType']== 'ti_trb50') | (df['TaType']== 'ti_trb150')| (df['TaType']== 'ti_trb200')| (df['TaType']== 'ti_bb')],
                        y=df['LR'][ (df['TaType']== 'ti_trb50') | (df['TaType']== 'ti_trb150')| (df['TaType']== 'ti_trb200')| (df['TaType']== 'ti_bb')],
                        legendgroup='LR', scalegroup='Yes', name='LR',
                        side='negative',
                        line_color='red')
)

fig.update_traces(meanline_visible=True)
fig.update_layout(violingap=0, violinmode='overlay', legend=dict(yanchor="top", y=0.99, xanchor="left",x=0.01),)
fig.update_layout(#title='LR and GR',
                   xaxis_title='Technical Indicators',
                   yaxis_title='Number of gains and losses realised',
                   width=800, height=500,
                   xaxis=dict(range=[-0.3, 3.5])
)

fig.show()
fig.write_image("../results/ViolinPlotDispEffNotConf1.pdf")

# for OBV non-confirming technical indicator
# [ (df['TaType']== 'ti_obv_sma1-50')| (df['TaType']== 'ti_obv_sma1-150')| (df['TaType']== 'ti_obv_sma1-200')]

fig = go.Figure()
fig.add_trace(go.Violin(x=df['TaType'][ (df['TaType']== 'ti_obv_sma1-50')| (df['TaType']== 'ti_obv_sma1-150')| (df['TaType']== 'ti_obv_sma1-200')],
                        y=df['GR'][ (df['TaType']== 'ti_obv_sma1-50')| (df['TaType']== 'ti_obv_sma1-150')| (df['TaType']== 'ti_obv_sma1-200')],
                        legendgroup='GR', scalegroup='Yes', name='GR',
                        side='positive',
                        line_color='green')
)
fig.add_trace(go.Violin(x=df['TaType'][ (df['TaType']== 'ti_obv_sma1-50')| (df['TaType']== 'ti_obv_sma1-150')| (df['TaType']== 'ti_obv_sma1-200')],
                        y=df['LR'][ (df['TaType']== 'ti_obv_sma1-50')| (df['TaType']== 'ti_obv_sma1-150')| (df['TaType']== 'ti_obv_sma1-200')],
                        legendgroup='LR', scalegroup='Yes', name='LR',
                        side='negative',
                        line_color='red')
)

fig.update_traces(meanline_visible=True)
fig.update_layout(violingap=0, violinmode='overlay', legend=dict(yanchor="top", y=0.99, xanchor="left",x=0.01),)
fig.update_layout(#title='LR and GR',
                   xaxis_title='Technical Indicators',
                   yaxis_title='Number of gains and losses realised',
                   width=800, height=500,
                   xaxis=dict(range=[-0.3, 2.5])
)

fig.show()
fig.write_image("../results/ViolinPlotDispEffNotConf2.pdf")

# %%
# FYI only - histogram view of GR and LR -> covered via violin plots
#fig = px.histogram(dfTa, x="txCnt", y="valSum", color="ti_macd_GR_LR", marginal="rug")
#fig = px.histogram(dfTa, x="txCnt")

fig = go.Figure()
fig.add_trace(go.Histogram(x=dfTa['txCnt'][dfTa['ti_macd_GR_LR']=='GR'], 
#fig.add_trace(go.Histogram(x=dfTa['valSum'][dfTa['ti_macd_GR_LR']=='GR'], 
     name='MACD GR',
     xbins=dict(
        start=1,
        end=1000,
        size=50
        )
    )
)
fig.add_trace(go.Histogram(x=dfTa['txCnt'][dfTa['ti_macd_GR_LR']=='LR'], 
#fig.add_trace(go.Histogram(x=dfTa['valSum'][dfTa['ti_macd_GR_LR']=='LR'], 
     name='MACD LR',
     xbins=dict(
        start=1,
        end=1000,
        size=50
        )
    )
)

# Overlay both histograms
fig.update_layout(barmode='overlay')
# Reduce opacity to see both histograms
fig.update_traces(opacity=0.75)
fig.show()

# %%
# show specific tech. ind. with their tstat over time

pio.templates.default = "plotly"

fig = make_subplots(rows=2, cols=1, row_heights=[0.8, 0.2], shared_xaxes=True,)

#fig = go.Figure()
fig.add_trace(go.Scatter(x=dfPaper['End'][dfPaper['TaType'] == 'Odean_GrLr'], 
               y=dfPaper['tstat'][dfPaper['TaType'] == 'Odean_GrLr'],
               mode='lines+markers',
               marker_symbol='circle-open',
               name='Odean'
               )
)

fig.add_trace(go.Scatter(x=dfPaper['End'][dfPaper['TaType'] == 'ti_rsi'], 
               y=dfPaper['tstat'][dfPaper['TaType'] == 'ti_rsi'],
               name='RSI',
               mode='lines+markers',
               marker_symbol='square-open'
               )
)

fig.add_trace(go.Scatter(x=dfPaper['End'][dfPaper['TaType'] == 'ti_roc'], 
               y=dfPaper['tstat'][dfPaper['TaType'] == 'ti_roc'],
               name='ROC',
               mode='lines+markers',
               marker_symbol='triangle-up-open'
               )
)

def df_to_plotly(df):
    return {'z': df.values.tolist(),
            'x': df.columns.tolist(),
            'y': df.index.tolist()}

myVar = 'pval'

# change the pvals for plus tstat to 10 declutter the heatmap and highlight the relevant periods
dfPaper['pval'][(dfPaper['tstat']>0)] = 10

z = dfPaper[[myVar]][dfPaper['TaType']=='Odean_GrLr'].T
z = z.rename(columns=dfPaper['End'])

tmp = dfPaper[[myVar]][dfPaper['TaType']=='ti_rsi'].T
tmp = tmp.rename(columns=dfPaper['End'])
z = z.append(tmp, ignore_index=True)

tmp = dfPaper[[myVar]][dfPaper['TaType']=='ti_roc'].T
tmp = tmp.rename(columns=dfPaper['End'])
z = z.append(tmp, ignore_index=True)

#x = dfPaper['End']
y = ['Odean', 'RSI', 'ROC']

fig.add_trace(go.Heatmap(df_to_plotly(z),
                                y=y,
                                zmin=0,
                                zmax=0.05,
                                autocolorscale=False,
                                colorscale = 'Greens',
                                reversescale=True,
                                ),               
                                row=2, col=1,                                
)

fig.update_layout(#title='LR and GR',
                   xaxis_title='Timeline',
                   yaxis_title='t-stat',
                   width=800, height=500,
                   legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),
)
#fig.update_traces(plot_bgcolor='white', row=2, col=1)
fig.show()
fig.write_image("../results/LinePlotTechIndHeatmap.pdf")

# %%

# %%
# Timeline view combined - show valSum, average BTC price and txCnt over time

fig = make_subplots(rows=2, cols=1, row_heights=[0.7, 0.3], shared_xaxes=True,
    specs=[[{"secondary_y": True}],
    [{"secondary_y": False}]])

strSctrMrkClr = 'rgba(255, 209, 4, 1)'

fig.append_trace(go.Bar(x=dfTaGrp['GrpYearMonth'], 
                    y=dfTaGrp['txCnt'],
                    name='Sell tx count',
                    marker_color=dfTaGrp['valSum'],
                    marker=dict(
                        #color='rgba(3, 41, 207, 0.6)',
                        #color='rgba(255, 209, 4, 0.6)',
                        color=dfTaGrp['valSum'],
                        colorscale='Viridis',
                        showscale=True,
                        colorbar=dict(title='BTC value<br>per Tx') , 
                        )
                    ),
            #secondary_y=True,
            row=2, col=1

)

fig.append_trace(go.Scatter(x=dfTaGrp['GrpYearMonth'], 
                y=dfTaGrp['valSum'],
                name='BTC tx value',
                mode="lines+markers",
                #marker_color=dfTaGrp['txCnt'],
                marker=dict(
                    color=strSctrMrkClr,
                    )
               #marker_symbol='triangle-up-open',
               ),
               #secondary_y = False,
               row=1, col=1
)

strBtcPriceMrkClr = 'rgb(255, 77, 77)'
fig.add_trace(go.Scatter(x=dfTaGrp['GrpYearMonth'], 
                y=dfTaGrp['avgClose'],
                name='BTC price',
                mode="lines+markers",
                #marker_color=dfTaGrp['txCnt'],
                marker=dict(
                    color=strBtcPriceMrkClr,
                    ),
               marker_symbol='triangle-up-open',
               ),
               secondary_y = True,
               row=1, col=1
)

#fig.update_yaxes(title_text="<b>BTC Value</b>", secondary_y=False)
#fig.update_yaxes(title_text="BTC Value", row=1, col=1)
#fig.update_yaxes(title_text="BTC Tx", row=2, col=1)

fig.update_layout(#title='LR and GR', xaxis_title='Timeline',
                width=800, height=500, legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1),)

fig.add_annotation(dict(x='2013-04-01',y='6.540610e+13',xref="x",yref="y",text='65.40T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2013-11-01',y='1.116955e+14',xref="x",yref="y",text='111.695T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2015-01-01',y='1.515492e+14',xref="x",yref="y",text='151.549T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2015-11-01',y='2.022435e+14',xref="x",yref="y",text='202.243T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2017-05-01',y='1.374741e+14',xref="x",yref="y",text='137.474T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
#fig.add_annotation(dict(x='2017-12-01',y='8.545978e+13',xref="x",yref="y",text='85.459T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
#fig.add_annotation(dict(x='2019-05-01',y='1.839967e+13',xref="x",yref="y",text='18.399T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))

fig.add_annotation(dict(x='2015-11-01',y='356.597138',xref="x",yref="y2",text='356.597',showarrow=True,bordercolor=strBtcPriceMrkClr,borderwidth=1,borderpad=1,bgcolor=strBtcPriceMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2017-12-01',y='14999.633039',xref="x",yref="y2",text='14999.633',showarrow=True,bordercolor=strBtcPriceMrkClr,borderwidth=1,borderpad=1,bgcolor=strBtcPriceMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2019-07-01',y='10665.273624',xref="x",yref="y2",text='10665.273',showarrow=True,bordercolor=strBtcPriceMrkClr,borderwidth=1,borderpad=1,bgcolor=strBtcPriceMrkClr,opacity=0.8))

#text=dfTaGrp['valSum'][dfTaGrp['GrpYearMonth']=='2013-1'].astype(str).get(0),
fig.show()
fig.write_image("../results/ValSumTxCntOverTime.pdf")
# %%
# Barchart plot for summary table
import pandas as pd 

dfPltSmry = {'Year': ['2013', '2014', '2015', '2016', '2017', '2018', '2019'], 
        'TxCount': [1133870,2570202,3237750,5576926,9646044,3197089,2188956], 
        'TxValue': [672346867841114,799953084150816,1589634906031250,1375871146832480,1087300645683670,312183016448292,158919356183924]}

years = dfPltSmry['Year']

#fig = go.Figure()
fig = make_subplots(specs=[[{"secondary_y": True}]])

#strBarClor = 'rgba(55, 83, 109, 0.7)'
strBarClor = '#404788'

fig.add_trace(go.Bar(x=years,
                y=dfPltSmry['TxCount'],
                name='TxCount',
                marker_color=strBarClor,
                text=dfPltSmry['TxCount'],
                textposition='outside',
                texttemplate = "%{y:,}",

                ),
                secondary_y=False
                )

strSctrMrkClr = 'rgba(255, 209, 4, 1)'
#y = dfPltSmry['TxValue']
fig.add_trace(go.Scatter(x=years, 
                y=dfPltSmry['TxValue'],
                name='TxValue',
                #mode="lines+markers+text",
                mode="lines+markers",
                marker=dict(
                    color=strSctrMrkClr,
                    size=12,
                    ),
               marker_symbol='circle',
               line=dict(color=strSctrMrkClr, width=4),
               text=dfPltSmry['TxValue'],
               #texttemplate = "%{y:s.2}",
               #textposition='auto',
               ),
               secondary_y = True,
               row=1, col=1
)

fig.add_annotation(dict(x='2013',y=dfPltSmry['TxValue'][0],xref="x",yref="y2",text='672.346T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2014',y=dfPltSmry['TxValue'][1],xref="x",yref="y2",text='799.953T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2015',y=dfPltSmry['TxValue'][2],xref="x",yref="y2",text='1.58963e15',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2016',y=dfPltSmry['TxValue'][3],xref="x",yref="y2",text='1.37587e15',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2017',y=dfPltSmry['TxValue'][4],xref="x",yref="y2",text='1.08730e15',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2018',y=dfPltSmry['TxValue'][5],xref="x",yref="y2",text='312.183T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))
fig.add_annotation(dict(x='2019',y=dfPltSmry['TxValue'][6],xref="x",yref="y2",text='158.919T',showarrow=True,bordercolor=strSctrMrkClr,borderwidth=1,borderpad=1,bgcolor=strSctrMrkClr,opacity=0.8))

fig.update_layout(
    #title='Per-year view of sell transactions (TxCount) and amount (TxValue)',
    #xaxis_tickfont_size=14,
    yaxis=dict(
        title='Number of Tx',
        #titlefont_size=16,
        #tickfont_size=14,
    ),
    yaxis2=dict(
        title='Satoshis',
        tickfont=dict(
            color=strSctrMrkClr,
        ),
    ),
    legend=dict(
        x=0,
        y=1.0,
        bgcolor='rgba(255, 255, 255, 0)',
        bordercolor='rgba(255, 255, 255, 0)'
    ),
    #barmode='group',
    bargap=0.15, # gap between bars of adjacent location coordinates.
    bargroupgap=0.1, # gap between bars of the same location coordinate.
    width=800, height=500,
)
fig.show()
fig.write_image("../results/SummaryTxCntValue.pdf")
# %%
