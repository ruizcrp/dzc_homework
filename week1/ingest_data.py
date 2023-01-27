import pandas as pd
from platform import python_version
from sqlalchemy import create_engine
import argparse
import os
from time import time


def main(params):
    user = params.user
    password =params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    url=params.url

    csv_name = 'output.csv'

    if url=="zones":
        os.system("wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -O zones.csv")
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
        df = pd.read_csv("zones.csv")
        df.to_sql(name=table_name,con=engine)
        quit()
    
    os.system(f"wget {url} -O {csv_name}.gz;gzip -df {csv_name}.gz")


    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #engine.connect()

    #print(pd.io.sql.get_schema(df,name='yellow_taxi_data',con=engine))

    df_iter = pd.read_csv(csv_name,iterator=True,chunksize=100000)

    df= next(df_iter)

    df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime=pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')

    df.to_sql(name=table_name,con=engine,if_exists='append')

    while True:
        t_start = time()
        df = next(df_iter)
        
        df.lpep_pickup_datetime=pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime=pd.to_datetime(df.lpep_dropoff_datetime)
        
        df.to_sql(name=table_name,con=engine,if_exists='append')
        
        t_end = time()
        
        print("inserted chunk, took %.3f second" % (t_end - t_start))


if __name__ == '__main__':
    #
    # user
    # password
    # host
    # post
    # db
    # table_name
    # url

    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres.')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='db name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url')

    args = parser.parse_args()
    main(args)







