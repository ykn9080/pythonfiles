#!/bin/python3

import pandas as pd
import time
import json

start_time = time.time()

def read_data():
    df = pd.read_csv("/home/yknam/movies.csv")
    # df.groupby([df.Year, df.Month]).agg(
    #     {'FlightNum': 'size', 'ArrDelay': ['sum', 'mean', 'max', 'min']})
    elapsed_time = time.time()-start_time
    print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
    return parse_csv(df)

def test():
    return json.dumps(df)

def parse_csv(df):
    res = df.to_json(orient="records")
    parsed = json.loads(res)
    return parsed