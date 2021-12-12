
# %%
import os
import glob
import pandas as pd
from pandas.core import indexing
#from pandas.io.parsers import count_empty_vals
import shutil
#move all .gz files to the current dir
#find . -name '*.gz' -exec mv {} ./ \;

# %%
# remove old 2020 (incomplete) files original working directory kaiko-ohlcv-1h-year

strPath = '../data/kaiko-ohlcv-1h-year'

for filepath in glob.glob(os.path.join(strPath, '*2020.csv')):
    filename = os.path.basename(filepath)
    print("removing old 2020 file: " + filename + '\n')
    os.remove(strPath+'/'+filename)

# %%
##################################################
# Update March 2021 - select OHLCV columns from COHLV-VWAP data files
##################################################

# updated exchange files header format
#Timestamp	Epoch timestamp in milliseconds. You can learn more about timestamps, including how to convert them to human readable form, here. The timestamp for all aggregates corresponds with the beginning of the time interval.
#Count	Number of trades occurring over the time interval of the data set. For example, '231' for minute granularity would mean that 231 raw trades occurred over the minute. 
#Open	Opening price in quote currency (for BTCUSD, USD is the quote curency)
#High	Highest price reached during the timeframe, in quote currency
#Low	Lowest price reached during the timeframe, in quote currency
#Close	Closing price of the timeframe in quote currency 
#Volume	Volume traded in the timeframe in base currency
#VWAP	Volume Weighted Average Price in quote currency

# set path to original kaiko files from March21
strPath2020 = '../data/kaiko-ohlcv-1h-year_2020'
strPath = '../data/kaiko-ohlcv-1h-year'

# create "_Archive" to move old files
dirName = strPath2020 + '/' + "_Archive"
if not os.path.exists(dirName):
    os.makedirs(dirName)
    print("Directory " , dirName ,  " Created ")
else:    
    print("Directory " , dirName ,  " already exists")    

# iterate through files
for filepath in glob.glob(os.path.join(strPath2020, '*.csv')):
    filename = os.path.basename(filepath)
    print("working on: " + filename + '\n')
    df = pd.read_csv(strPath2020+'/'+filename, delimiter=',', header=None, prefix="Col")
    df.to_csv(strPath2020+'/'+filename[:-4] + '_ohlcv.csv', columns=['Col0','Col2','Col3','Col4','Col5','Col6'], header=False, index=False)
    # move original files into Archive folder
    shutil.move(strPath2020+'/'+filename, dirName+'/'+filename)
    #move new / complete 2020 files into original kaiko folder
    shutil.move(strPath2020+'/'+filename[:-4] + '_ohlcv.csv', strPath+'/'+filename[:-4] + '_ohlcv.csv')
 
print('#### March 2021 Update - columns selected, files stored in: '+strPath2020+', 2020 files moved to: '+strPath)


# %%
##################################################
# add exchange identified column to all kaiko files, move all to _ex folder
##################################################
strPath = '../data/kaiko-ohlcv-1h-year'

# create "_ex" target directory for output file including exchange identified
dirName = strPath + '/' + "_ex"
if not os.path.exists(dirName):
    os.makedirs(dirName)
    print("Directory " , dirName ,  " Created ")
else:    
    print("Directory " , dirName ,  " already exists")    

# iterate through files
for filepath in glob.glob(os.path.join(strPath, '*.csv')):
    # find exchange 2 character abbreviation
    filename = os.path.basename(filepath)
    print("working on: " + filename + '\n')
    strEx = filename[0:2]
    
    # open specific file from path
    with open(filepath) as f:
        Lines = f.readlines()

        #iterate through all lines in file and add exchange 2 characters identifier
        exportLines = []
        for line in Lines:
                exportLine = []
                exportLine.append(strEx + ',' + line)
                exportLine = ''.join(exportLine)
                exportLines.append(exportLine)       
                #print(exportLine)

    # write the lines from the list to specific *_ex.csv file
    writeFilename = dirName + '/' + filename[:-4] + '_ex.csv'
    with open(writeFilename, 'w') as the_file:
        for line in exportLines:
            the_file.write(line)

print('#### end ####')

# %%
##################################################
# merge all exchange files into one all_* file
##################################################
import os
import glob

strPath = '../data/kaiko-ohlcv-1h-year'
strPathOhlc = strPath + '/_ex/'
strFnOhlc = 'all_btcusd_ohlcv_1h_ex.csv'

# write the header file containing the column information for the overall exchange file
with open(strPathOhlc + '/aa_header_file.txt', 'a') as the_file:
    the_file.write('exshort,timestamp,open,high,low,close,volume\n')

# merge exchange files into one file
read_files = glob.glob(os.path.join(strPathOhlc, '*.csv'))
read_files.insert(0,strPathOhlc + "aa_header_file.txt")

with open(strPathOhlc + strFnOhlc, "wb") as outfile:
    for f in read_files:
        with open(f, "rb") as infile:
            outfile.write(infile.read())

print('#### end ####')

# %%
