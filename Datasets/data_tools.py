# FUNCTIONS FOR DATA MANIPULATIONS JUST IN CASE YOU NEED IT
# TOM.NGUYEN**AT**IEEE.ORG

import glob, os
import pandas as pd
import pandas as pd
import numpy as np

# Returns a dataframe that contains all of the directory's parquet files
def combine_directory_of_parquet(directory='./**/**.parquet', recursive=True, max_row=800000, columns=[]):

    # Create an empty dataframe to hold our combined data
    merged_df = pd.DataFrame(columns=columns)

    # Iterate over all of the files in the provided directory and
    # configure if we want to recursively search the directory
    for filename in glob.iglob(pathname=directory, recursive=recursive):

        # Check if the file is actually a file (not a directory) and make sure it is a parquet file
        if os.path.isfile(filename):
            try:
                # Perform a read on our dataframe
                temp_df = pd.read_parquet(filename)

                # Attempt to merge it into our combined dataframe
                # merged_df = merged_df.append(temp_df, ignore_index=True) #deprecated
                merged_df = pd.concat([merged_df, temp_df], ignore_index=True)

            except Exception as e:
                print('Skipping {} due to error: {}'.format(filename, e))
                continue;
        else:
            print('Not a file {}'.format(filename))

        if merged_df.shape[0]>max_row:
            print("Maximum allowable rows reached.")
            break #break out of the loop
    # Return the result!
    return merged_df

def optimize_data(df, timeStamp_col='ts_submit',index_col='idx',key_col='id',parent_col='workflow_id',cut_off=800000):
    timeStamp=timeStamp_col
    key_col=key_col
    parent_col=parent_col
    row_cutoff=cut_off
    # sort df the main time stamp
    df.sort_values(by=[timeStamp])
    # remove rows with zero time stamp
    df = df[df[timeStamp] >0]
    #df.set_index('idx', inplace=True, drop=False)
    print("Removed rows with empty ts_submit.")
    print("New num rows: "+str(df.shape[0]))
    df = df.reset_index(drop=True)
    df.index.names = [index_col]
    # Drop duplicates
    df = df.drop_duplicates(
    subset = [key_col, timeStamp],
    keep = 'first')

    #df[key_col] = df[parent_col].astype(str)+"-"+df[key_col].astype(str) # if you want to merge

    # Reduce df size
    # df = df[['bucket','workflow_id', 'id', 'ts_submit','runtime','wait_time']] 
    #df = df[['idx','workflow_id', 'ts_submit','runtime','wait_time']] 
    df = df.truncate(after=row_cutoff)

    return df

def funcName_2_funcID (df_in, funcName_col=''):
    df=df_in.copy()
    lst_funcNames=df[funcName_col].values.tolist()
    #print(lst_funcNames[:5])
    lst_funcNames=list(dict.fromkeys(lst_funcNames)) #dedup
    dict_funcNames={}
    i = 0
    for item in lst_funcNames:
        if(i>0 and item in dict_funcNames):
            continue
        else:    
            i = i+1
            dict_funcNames[item] = i*10 #9 unit distance between 2 dict items
    df['func_ID'] = df[funcName_col].map(dict_funcNames)
    #print(dict_funcNames)
    print(df.head(5))
    return df

# SPECIFIC ACTIONS BELOW

#parquet_in_directory='./Google-TaskStates/**.parquet'
#parquet_out_file_name='Google_trace_slim.csv'
in_file_name='Alibaba_region_01.csv' #file must have idx as the index column
out_file_name='Alibaba_region_01_withFuncID.csv'
row_cutoff=800000 # same num row across all evaluations
bucket_size=400  # this is the forecast horizon size used across all evaluations

# Replace this with your column names that you are expecting in your parquet's
# Google dataset columns: workflow_id	id	type	ts_submit	submission_site	runtime	resource_type	resource_amount_requested	parents	children	user_id	group_id	nfrs	wait_time	params	memory_requested	network_io_time	disk_space_requested	energy_consumption	resource_used	disk_io_time
# columns = ['workflow_id', 'id', 'ts_submit','runtime','wait_time','network_io_time','disk_io_time']
# Alibaba dataset columns: children - list[i64]	disk_io_time - i64	disk_space_requested - f64	energy_consumption - i64	group_id - i32	id - i64	memory_requested - f64	network_io_time - i64	nfrs - str	params - str	parents - list[i64]	resource_amount_requested - f64	resource_type - str	resource_used - i64	runtime - i64	submission_site - i32	ts_submit - i64	type - str	user_id - i32	wait_time - i64	workflow_id - i64
# columns = ['workflow_id', 'id', 'ts_submit','runtime','wait_time']

# df = combine_directory_of_parquet(directory=, recursive=True, columns=columns)

# Only pick the columns we want
# df = df[['workflow_id', 'task_id', 'start_time','end_time','cpu_rate','disk_io_time','network_io_time']]

df = pd.read_csv(in_file_name, index_col=0) #assuming first col is the index
print("Importing: "+in_file_name)
print("Num rows: "+str(df.shape[0]))

df= optimize_data(df,'__time__','idx','functionName','',row_cutoff)
df=funcName_2_funcID(df,'functionName')
df['__time__']=df['__time__']-1620489000
# Write the dataframe to a CSV file
df.to_csv(out_file_name)