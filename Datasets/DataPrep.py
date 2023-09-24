import pandas as pd
import numpy as np

#Google_trace_slim.csv
df = pd.read_csv('Google_trace_slim.csv')
print("Num rows: "+str(df.shape[0]))
print(df.head(15))

# remove rows with zero time stamp
df = df[df['ts_submit'] >0]
#df.set_index('idx', inplace=True, drop=False)
rows=df.shape[0]
print("Post null removal num rows: "+str(rows))
# sort df the main time stamp
df = df.sort_values(by=['ts_submit'],ascending=True,)
print(df.head(15))

# Only pick the columns we want
#df = df[['bucket','workflow_id', 'id']] #task 1
df = df[['workflow_id', 'id', 'ts_submit','runtime','wait_time']] #task 2
# Crop df
df = df.reset_index()
df = df.truncate(after=800000)
print("Final num rows: "+str(df.shape[0]))

# Write the dataframe to a CSV file
df.to_csv('./Google_task2.csv')