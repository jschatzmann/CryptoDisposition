
# %%
##################################################
# add exchange identified column to all kaiko files, move all to _ex folder
##################################################
import os
import glob

strPath = '/Users/jes/OneDrive - FH JOANNEUM/06 Data/kaiko-ohlcv-1h-year'

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

strPath = '/Users/jes/OneDrive - FH JOANNEUM/06 Data/kaiko-ohlcv-1h-year'
strPathOhlc = strPath + '/_ex/'
#strPathOhlc = 'C:/Users/juergen.schatzmann/OneDrive - FH JOANNEUM/02 Statistik/05 AIT Analysis/_ex/'
strFnOhlc = 'all_btcusd_ohlcv_1h_ex.csv'

read_files = glob.glob(os.path.join(strPathOhlc, '*.csv'))
read_files.insert(0,strPathOhlc + "aa_header_file.txt")

with open(strPathOhlc + strFnOhlc, "wb") as outfile:
    for f in read_files:
        with open(f, "rb") as infile:
            outfile.write(infile.read())

print('#### end ####')


    