import pandas as pd
import numpy as np

in_file_name='Google_trace_slim.csv' #file must have idx as the index column
out_file_name='Google_trace_optimized.csv'
row_cutoff=800000 # same num row across all evaluations
bucket_size=400  # this is the forecast horizon size used across all evaluations

df = pd.read_csv(in_file_name)
print("Importing: "+in_file_name)
print("Num rows: "+str(df.shape[0]))

# sort df the main time stamp
df.sort_values(by=['ts_submit'])
# remove rows with zero time stamp
df = df[df['ts_submit'] >0]
df.set_index('idx', inplace=True, drop=False)
print("Removed rows with empty ts_submit.")
print("New num rows: "+str(df.shape[0]))
df = df.reset_index(drop=True)
df.index.names = ['idx']
# Drop duplicates
df = df.drop_duplicates(
  subset = ['workflow_id', 'ts_submit'],
  keep = 'first')

#df["job_id"] = df["workflow_id"].astype(str)+"-"+df["id"].astype(str) # for Google dataset only

# Reduce df size
# df = df[['bucket','workflow_id', 'id', 'ts_submit','runtime','wait_time']] 
df = df[['idx','workflow_id', 'ts_submit','runtime','wait_time']] 
#df = df.truncate(after=row_cutoff*1.5)

# Bucket set up
low = 0
up = bucket_size
df['bucket']=0
rows=df.shape[0]

# loop to fill in bucket values
for i in range(1,int(rows/bucket_size)):
    # evaluate the condition 
    df['bucket'] = np.where( df['idx']<up, np.where(df['idx']>=low,i,df['bucket']),df['bucket'])
    low = up
    up = low+bucket_size
    if (i%bucket_size)==0: print(".", end ="")


# We only care about the first appearance of a job within a time bucket
# Only use this when there are a lot of tasks submitted almost at the same time within a workflow
#df = df.drop_duplicates(
#  subset = ['workflow_id', 'bucket'],
#  keep = 'first')

print("New optimized num rows: "+str(df.shape[0]))
df = df.reset_index(drop=True)
#df.index.names = ['idx']

df = df.truncate(after=row_cutoff)
print("Final num rows: "+str(df.shape[0]))

# Write the dataframe to a CSV file
df.to_csv(out_file_name)