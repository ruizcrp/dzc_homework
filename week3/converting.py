import numpy as np 
import pandas as pd 

df = pd.read_csv('data/fhv_tripdata_2019-01.csv.gz', compression='gzip')

for i in range(2,13):
    tds=str(i).zfill(2)
    file_name=f'data/fhv_tripdata_2019-{tds}.csv.gz'
    print(file_name)
    df_temp = pd.read_csv(file_name, compression='gzip')
    df=pd.concat([df, df_temp], axis=0)

print(df.head())
print(df.shape[0])
#df.to_csv("data/fhv_tripdata_20219-all.csv",index=False)
df.to_csv("data/fhv_tripdata_20219-all.csv.gz",compression="gzip",index=False)