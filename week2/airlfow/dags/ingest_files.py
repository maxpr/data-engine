import os
import pandas as pd
from time import time
from sqlalchemy import create_engine

def ingest_callable(user, password, host, port, db, table_name, csv_name):

    print(user, password, host, port, db)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    if "tpep_pickup_datetime" in df.columns:
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    if "tpep_dropoff_datetime" in df.columns:
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    try:
        while True: 
            t_start = time()

            df = next(df_iter)
            
            if "tpep_pickup_datetime" in df.columns:
                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            if "tpep_dropoff_datetime" in df.columns:
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))
    except StopIteration:
        print("We finished iterating")