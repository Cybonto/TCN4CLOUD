import pandas as pd
import numpy as np

file_name='Google_trace_slim.csv' #file must have idx as the index column
df = pd.read_csv(file_name)
print("Importing: "+file_name)
print("Num rows: "+str(df.shape[0]))

# sort df the main time stamp
df.sort_values(by=['ts_submit'])
# remove rows with zero time stamp
df = df[df['ts_submit'] >0]
df.set_index('idx', inplace=True, drop=False)
print("Removed rows with empty ts_submit.")
rows=df.shape[0]
print("New num rows: "+str(rows))

# Bucket set up
bucket_size = 400
low = 0
up = bucket_size
df['bucket']=0

# loop to fill in bucket values
for i in range(1,int(rows/bucket_size)):
    # evaluate the condition 
    df['bucket'] = np.where( df['idx']<up, np.where(df['idx']>=low,i,df['bucket']),df['bucket'])
    low = up
    up = low+bucket_size
    if (i%bucket_size)==0: print(".", end ="")

# Only pick the columns we want
# df = df[['bucket','workflow_id', 'id', 'ts_submit','runtime','wait_time']] 
# df = df[['bucket','id', 'ts_submit']] #for LANL Mustang only
df["job_id"] = df[["workflow_id", "id"]].apply("-".join, axis=1) # for Google dataset only
df = df[['bucket','job_id','ts_submit','runtime','wait_time']] # for Google dataset only

# Write the dataframe to a CSV file
df.to_csv('./LANL_Mustang_task2_bucketed.csv')