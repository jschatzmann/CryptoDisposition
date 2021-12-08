install.packages("ggpubr")
install.packages("readxl")
install.packages("openxls")

library(ggpubr)
library(readxl)
library(dplyr)
library(gridExtra)
library(openxlsx)

# clear all variables
rm(list=ls()) 

#strTime = '2020-09-13_11_52_29' # including 2019 data
#strTime = '2021-03-28_15_52_23' # including 2020 data
strTime = '2021-11-27_14_12_01' # including 2021 data till November
#dfPaper = pd.read_excel(r'../data/_dfPaperPlotsperMonth_export_'+strTime+'.xlsx')

#'/Users/jes/OneDrive\ -\ FH\ JOANNEUM/02\ Statistik/CryptoDispGitHub/CryptoDisposition/data/_dfPaperPlotsperMonth_export_2021-03-28_15_52_23.xlsx'
strPath <- paste('/Users/jes/OneDrive\ -\ FH\ JOANNEUM/02\ Statistik/CryptoDispGitHub/CryptoDisposition/data//_dfPaperPlotsperMonth_export_',strTime,'.xlsx', sep="",collapse = NULL)
strPath
dfPaper <- read_excel(strPath)

#filter(df, (a==1 & b>15) | (a==2 & b>5))

# list technical indicator names
dfPaper %>% 
  group_by(TaType) %>%
  summarise()

renameTechInd <- function(){
  # Odean_GrLr, ti_macd, ti_roc, ti_obv_sma5-150, ti_obv_sma2-200, ti_rsi
  # ti_sma1-50, ti_sma1-150, ti_sma5-150, ti_sma1-200, ti_sma2-200
  dfPaper$TaType[dfPaper$TaType == "Odean_GrLr"] <- "Odean Average"
  dfPaper$TaType[dfPaper$TaType == "ti_macd"] <- "MACD"
  dfPaper$TaType[dfPaper$TaType == "ti_roc"] <- "ROC"
  dfPaper$TaType[dfPaper$TaType == "ti_obv_sma5-150"] <- "OBV5-150"
  dfPaper$TaType[dfPaper$TaType == "ti_obv_sma2-200"] <- "OBV2-200"
  dfPaper$TaType[dfPaper$TaType == "ti_rsi"] <- "RSI"
  
  dfPaper$TaType[dfPaper$TaType == "ti_sma1-50"] <- "SMA1-50"
  dfPaper$TaType[dfPaper$TaType == "ti_sma1-150"] <- "SMA1-150"
  dfPaper$TaType[dfPaper$TaType == "ti_sma5-150"] <- "SMA5-150"
  dfPaper$TaType[dfPaper$TaType == "ti_sma1-200"] <- "SMA1-200"
  dfPaper$TaType[dfPaper$TaType == "ti_sma2-200"] <- "SMA2-200"
  return(dfPaper)
}

##############################################################################
##### Violin Plots #####

# call rename function
dfPaper <- renameTechInd()

# create df specific for GR and LR to stack them together
#dfGR <- dfPaper[dfPaper$TaType == 'Odean_GrLr',]
dfGR <- filter(dfPaper, (TaType=='Odean Average') | (TaType=='MACD')| (TaType=='ROC')| 
                 (TaType=='OBV5-150')| (TaType=='OBV2-200')| (TaType=='RSI'))
dfGR2 <- filter(dfPaper, (TaType=='SMA1-50') | (TaType=='SMA1-150')| (TaType=='SMA5-150')| 
                  (TaType=='SMA1-200')| (TaType=='SMA2-200'))

dfLR <- filter(dfPaper, (TaType=='Odean Average') | (TaType=='MACD')| (TaType=='ROC')| 
                 (TaType=='OBV5-150')| (TaType=='OBV2-200')| (TaType=='RSI'))
dfLR2 <- filter(dfPaper, (TaType=='SMA1-50') | (TaType=='SMA1-150')| (TaType=='SMA5-150')| 
                  (TaType=='SMA1-200')| (TaType=='SMA2-200'))


# add GR or LR indicator
dfGR$GRorLR <- "GR"
dfGR$counts <- dfGR$GR
dfGR2$GRorLR <- "GR"
dfGR2$counts <- dfGR2$GR

dfLR$GRorLR <- "LR"
dfLR$counts <- dfLR$LR
dfLR2$GRorLR <- "LR"
dfLR2$counts <- dfLR2$LR


# stack dataframes above each other for graphs
dfPaperPlot <- rbind(dfGR, dfLR)
#nrow(dfPaperPlot)

# lancet palette - red #ED0000FF / green #42B540FF
p1 <- ggviolin(dfPaperPlot, x = "TaType", y = "counts", color = "GRorLR", 
         palette = c('#42B540FF', '#ED0000FF'), add = c("boxplot"))

p1 <- ggpar(p1,
      #main = "Gains Realised vs Losses Realised \n per Technical Indicator",
      xlab = "Technical Indicator", ylab = "Number of gains and losses realised",
      legend.title = ''
      )
p1 <- p1 + scale_y_continuous(labels=function(x) format(x,scientific = TRUE))
p1

# for SMA dataframe
dfPaperPlot <- rbind(dfGR2, dfLR2)
p2 <- ggviolin(dfPaperPlot, x = "TaType", y = "counts", color = "GRorLR", 
              palette = c('#42B540FF', '#ED0000FF'), add = c("boxplot"))

p2 <- ggpar(p2,
      #main = "Gains Realised vs Losses Realised \n per Technical Indicator",
      xlab = "Technical Indicator", ylab = "Number of gains and losses realised",
      legend.title = ''
)
p2 <- p2 + scale_y_continuous(labels=function(x) format(x, scientific = TRUE))
p2


gt <- arrangeGrob(p1, ncol = 1, nrow = 1)
as_ggplot(gt)
#800 pts = 282 mm, 500 pts = 176 mm
ggsave("01a_ViolinPlots1.pdf", device = "pdf", width = 282, height = 176, units = "mm")

gt <- arrangeGrob(p2, ncol = 1, nrow = 1)
as_ggplot(gt)
ggsave("01a_ViolinPlots2.pdf", device = "pdf", width = 282, height = 176, units = "mm")


##############################################################################
##### Per year view TxValue and TxCount Plots #####
strPath <- paste('/Users/jes/OneDrive\ -\ FH\ JOANNEUM/02\ Statistik/CryptoDispGitHub/CryptoDisposition/data//_dfTAGrp',strTime,'.xlsx', sep="",collapse = NULL)
strPath
dfPaper <- read_excel(strPath)

dfPaper <- dfPaper %>% 
  group_by(GrpYear) %>%
  summarise("SumTx" = sum(txCnt), "SumVal" = sum(valSum), "MeanVal" = mean(valSum))

# lancet blue #00468BFF / violett #925E9FFF
p1 <- ggbarplot(dfPaper, y = "SumTx", x = "GrpYear", label = TRUE, label.pos ="out", color = "#00468BFF", lab.col = "white")
#p1 <- ggbarplot(dfPaper, y = "SumTx", x = "GrpYear", color = "#00468BFF")
p1 <- ggpar(p1, xlab = "Year", ylab = "Number of Tx")
p1 <- p1 + scale_y_continuous(labels=function(x) format(x, scientific = TRUE))
p1 <- p1 + geom_text(aes(label = format(..y.., big.mark = ",")), vjust=-0.5)
p1 <- ggpar(p1,
            xlab = "", ylab = "Nr. of Transactions",
            legend.title = ''
)
p1

p2 <- ggline(dfPaper, y = "SumVal", x = "GrpYear", color = '#925E9FFF')
#p2 <- ggbarplot(dfPaper, y = "SumVal", x = "GrpYear", color = '#925E9FFF', label = TRUE, label.pos = "out")
p2 <- ggpar(p2, xlab = "Year", ylab = "Satoshis")
p2

#ggarrange(p1, p2, ncol=1, nrow = 2) %>%
#  ggexport(filename = "02_TxValueTxCountPerYear.png", width = 1500, height = 700)

gt <- arrangeGrob(p1, p2, ncol = 1, nrow = 2)
as_ggplot(gt)
ggsave("02_TxValueTxCountPerYear.pdf", device = "pdf", width = 282, height = 210, units = "mm")

# ---- check transaction value summed vs transaction value mean
px <- ggline(dfPaper, y = "SumVal", x = "GrpYear", color = '#925E9FFF')
px <- ggline(dfPaper, y = "MeanVal", x = "GrpYear", color = 'red')
#p2 <- ggbarplot(dfPaper, y = "SumVal", x = "GrpYear", color = '#925E9FFF', label = TRUE, label.pos = "out")
px <- ggpar(px, xlab = "Year", ylab = "Satoshis")
px


##############################################################################
##### Monthly aggregated view of Bitcoins and number of sell tx
#rm(list=ls()) 
strPath <- paste('/Users/jes/OneDrive\ -\ FH\ JOANNEUM/02\ Statistik/CryptoDispGitHub/CryptoDisposition/data//_dfTAGrp',strTime,'.xlsx', sep="",collapse = NULL)
strPath
dfPaper <- read_excel(strPath)
# create YYYY-M-D text
dfPaper$GrpYearMonthDay = paste(dfPaper$GrpYearMonth, "-1", sep = "")
# convert to date for chart scale
dfPaper$GrpYearMonthDay = as.Date(dfPaper$GrpYearMonthDay, tryFormas = c("%Y-%m-%d", "%Y-%M-%D"))



#dfPaper$GrpYearMonth = substr(dfPaper$End, 1,7)

p1 <- ggline(dfPaper, y = "valSum", x = "GrpYearMonthDay", color = '#925E9FFF')
p1 <- ggpar(p1, xlab = "", ylab = "BTC Tx Value", xtickslab.rt = 90, font.xtickslab = c(9, "plain", "black"))#, xticks.by = 5)
p1 <- p1 + scale_y_continuous(labels=function(x) format(x, scientific = TRUE))
p1 <- p1 + scale_x_date(date_breaks = "3 months") + theme(axis.text.x = element_blank())  #+ guides(x="none")

p2 <- ggline(dfPaper, y = "avgClose", x = "GrpYearMonthDay", color = '#ED000099')
p2 <- ggpar(p2, xlab = "", ylab = "BTC Price", xtickslab.rt = 90, font.xtickslab = c(9, "plain", "black"))
p2 <- p2 + scale_y_continuous(labels=function(x) format(x, scientific = TRUE))
p2 <- p2 + scale_x_date(date_breaks = "3 months") + theme(axis.text.x = element_blank())
p3 <- ggbarplot(dfPaper, y = "txCnt", x = "GrpYearMonthDay", color = "#00468BFF")
p3 <- ggpar(p3, xlab = "Quarter", ylab = "Sell Tx Count", xtickslab.rt = 90, font.xtickslab = c(9, "plain", "black"))
p3 <- p3 + scale_y_continuous(labels=function(x) format(x, scientific = TRUE))
p3 <- p3 + scale_x_date(date_breaks = "3 months", date_labels = "%Y-%m")
#set_palette(p3, "grey")

#ggarrange(p1,p2,p3, ncol = 1, nrow = 3, align = "hv") %>%
#  ggexport(filename = "03_MonthlyViewBtcSellTx.png", width = 1500, height = 700)

gt <- arrangeGrob(p1, p2, p3, ncol = 1, nrow = 3)
as_ggplot(gt)
#ValSumTxCntOverTime.pdf
ggsave("ValSumTxCntOverTime.pdf", device = "pdf", width = 282, height = 176, units = "mm")
#ggsave("03_MonthlyViewBtcSellTx.pdf", device = "pdf", width = 282, height = 176, units = "mm")


##############################################################################
##### Tech Ind comparison
strPath <- paste('/Users/jes/OneDrive\ -\ FH\ JOANNEUM/02\ Statistik/CryptoDispGitHub/CryptoDisposition/data//_dfPaperPlotsperMonth_export_',strTime,'.xlsx', sep="",collapse = NULL)
dfPaper <- read_excel(strPath)

# rename TechInd for pretty names after df reload
dfPaper <- renameTechInd()

dfPaper$EndPlot = substr(dfPaper$End, 1,10)
dfPaper$EndPlotYear = substr(dfPaper$End, 1,4)
dfPaper$GrpYearMonthDay = as.Date(dfPaper$Start, tryFormas = c("%Y-%m-%d", "%Y-%M-%D"))

dfPaperPlot <- filter(dfPaper, (TaType=='Odean Average') | (TaType=='ROC')| (TaType=='RSI'))

#p1 <- ggline(dfPaperPlot, x="EndPlot", y="tstat", linetype = "TaType", color = "TaType", palette = "aaas",
p1 <- ggline(dfPaperPlot, x="GrpYearMonthDay", y="tstat", linetype = "TaType", color = "TaType", palette = "aaas", 
            xtickslab.rt = 90, font.xtickslab = c(10, "plain", "black"))
p1 <- ggpar(p1, xlab = "Month", ylab = "t-stat", legend.title = '')
p1 <- p1 + scale_x_date(date_breaks = "3 months", date_labels = "%Y-%m")
p1

# create significance level groups
dfPaperPlot <- dfPaperPlot %>% mutate(PvalGroup =
                     case_when(pval <= 0.001 ~ "0.001", 
                               pval <= 0.01 ~ "0.01",
                               pval <= 0.05 ~ "0.05",
                               pval > 0.05 ~ "nonsig")
)

# p2a <- ggdotchart(filter(dfPaperPlot, (TaType=='Odean Average')), x = "EndPlot", y = "PvalGroup", ggtheme = theme_bw(), group = "TaType", 
#                  sorting = "none") #, dot.size = "pval")
# p2b <- ggdotchart(filter(dfPaperPlot, (TaType=='ROC')), x = "EndPlot", y = "PvalGroup", ggtheme = theme_bw(), group = "TaType", 
#                   sorting = "none") #, dot.size = "pval")
# p2c <- ggdotchart(filter(dfPaperPlot, (TaType=='RSI')), x = "EndPlot", y = "PvalGroup", ggtheme = theme_bw(), group = "TaType", 
#                   sorting = "none") #, dot.size = "pval")
# p2 <- ggpar(p2, ylab = "Significance")

#p2 <- ggballoonplot(dfPaperPlot, x = "EndPlot", y = "TaType", fill = "PvalGroup", size = 3, legend = "top", shape = 23,
p2 <- ggballoonplot(dfPaperPlot, x = "GrpYearMonthDay", y = "TaType", fill = "PvalGroup", size = 3, legend = "top", shape = 23, 
                    palette = c("#43B540FF","#42B54099","#FDAF9199","white"))
p2 <- p2 + scale_x_date(date_breaks = "3 months", date_labels = "%Y-%m")

# widht = 800, height = 500
pEx <- ggarrange(p1, p2, ncol = 1, nrow = 2, align = "hv", heights = c(2,1))# %>%
#ggexport(filename = "04_TstatAndPvalues3Indicators.png", width = 1500, height = 700)

gt <- arrangeGrob(pEx, ncol = 1, nrow = 1)
as_ggplot(gt)
#LinePlotTechIndHeatmap.pdf
ggsave("LinePlotTechIndHeatmap.pdf", device = "pdf", width = 282*1.2, height = 176*1.2, units = "mm")
#ggsave("04_TstatAndPvalues3Indicators.pdf", device = "pdf", width = 282*1.2, height = 176*1.2, units = "mm")


##############################################################################
# calculate / validate average tx sell value based on raw data from python
strPath <- paste('/Users/jes/OneDrive\ -\ FH\ JOANNEUM/02\ Statistik/CryptoDispGitHub/CryptoDisposition/data//_dfTA_export_',strTime,'.csv', sep="",collapse = NULL)
strPath
dfTAExport <- read.csv(strPath)

head(dfTAExport)

dfTAExport$YearMonth = substr(dfTAExport$tmstOhlc_rdbl, 1,10) #get year-month YYYY-MM
dfTAExport$Year = substr(dfTAExport$tmstOhlc_rdbl, 1,4) #get year YYYY


dfTAExportGrp <- dfTAExport %>% 
  group_by(Year) %>%
  summarise("SumTx" = sum(txCnt, na.rm = TRUE), "SumVal" = sum(valSum, na.rm = TRUE), "MeanVal" = mean(valSum, na.rm = TRUE))

dfTAExportGrp

strPath <- paste('/Users/jes/OneDrive\ -\ FH\ JOANNEUM/02\ Statistik/CryptoDispGitHub/CryptoDisposition/data//_dfTA_export_GrpPerYear_',strTime,'.xlsx', sep="",collapse = NULL)
strPath
write.xlsx(dfTAExportGrp, strPath, sheetName = "dfTAExportGrpPerYear", 
           col.names = TRUE, row.names = TRUE, append = FALSE)
