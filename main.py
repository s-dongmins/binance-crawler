import argparse
from datetime import datetime, timezone
import os
import requests
import time

import numpy as np


year2010 = 1262304000000
year2011 = 1293840000000
year2012 = 1325376000000
year2013 = 1356998400000
year2014 = 1388534400000
year2015 = 1420070400000
year2016 = 1451606400000
year2017 = 1483228800000
year2018 = 1514764800000
year2019 = 1546300800000
year2020 = 1577836800000
year2021 = 1609459200000
year2022 = 1640995200000
year2023 = 1672531200000
year2024 = 1704067200000

dtype = np.dtype([
    ("open_time", "u8"),
    ("open", "f4"),
    ("high", "f4"),
    ("low", "f4"),
    ("close", "f4"),
    ("volume", "f8"),
    ("close_time", "u8"),
    ("base_volume", "f8"),
    ("trades", "u4"),
    ("taker_volume", "f8"),
    ("taker_base_volume", "f8")
])

def date_to_timestamp(date):
    date_object = datetime.strptime(date, "%Y-%m-%d")
    date_object = date_object.replace(tzinfo=timezone.utc)
    timestamp = int(date_object.timestamp())
    return timestamp * 1000

def timestamp_to_date(timestamp):
    date_object = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc)
    return date_object.strftime("%Y-%m-%d")

def get_time_string():
    current = datetime.now(timezone.utc)
    return current.strftime("%Y-%m-%d %H:%M:%S")

def fetch_klines(start_time, ticker):
    url = "https://www.binance.com/api/v3/uiKlines"
    params = {
        "startTime": start_time,
        "limit": 900,
        "symbol": ticker,
        "interval": "1s",
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        data = [tuple(d[:-1]) for d in data]

        if len(data) != 900:
            raise Exception("not 900")

        return data
    else:
        raise Exception(f"Failed to retrieve data. Status code: {response.status_code}")
    
def write_log(log, error=False):
    print(log)
    if error:
        log = f"\n{get_time_string()} ERR: " + log
    else:
        log = f"\n{get_time_string()} LOG: " + log

    with open('log.txt', "a") as log_file:
        log_file.write(log)

def main():
    parser = argparse.ArgumentParser(description="Simple Binance crawler")
    
    parser.add_argument("-s", "--start", required=True, help="Starting point of crawl, format: YYYY-MM-DD (ex: 2024-06-07)")
    parser.add_argument("-e", "--end", required=True, help="Endpoint of crawl, format: YYYY-MM-DD (ex: 2024-06-24)")
    parser.add_argument("-t", "--ticker", required=True, help="Ticker of crypto to crawl (ex: BTCUSDT)")
    parser.add_argument("-i", "--interval", type=float, default=0.7, help="Interval between each fetch (default=0.7)")
    
    args = parser.parse_args()

    write_log(f"Starting Fetch, {args.start} ~ {args.end}, Ticker: {args.ticker}")

    start = date_to_timestamp(args.start)
    end = date_to_timestamp(args.end)

    if not os.path.exists("data"):
        os.makedirs("data")
    if not os.path.exists(f"data/{args.ticker}"):
        os.makedirs(f"data/{args.ticker}")

    target = start - 86400000
    done_list = [d.replace(".bin", "") for d in os.listdir(f"data/{args.ticker}")]
    
    while True:
        target += 86400000

        if timestamp_to_date(target) in done_list:
            write_log(f"{timestamp_to_date(target)} already fetched. Move on to the next fetch.")
            continue

        if target >= end:
            write_log("All data has been fetched")
            break

        write_log(f"Fetching {timestamp_to_date(target)}")


        data = []
        fetch = True
        while fetch:
            fetch = False
            try:
                for i in range(96):
                    print(f"{i} / 96")
                    data += fetch_klines(target + (i * 900000), args.ticker)
                    time.sleep(args.interval)
            except Exception as e:
                write_log("Error Occurred! Sleep 5min", True)
                write_log(str(e), True)
                fetch = True
                time.sleep(300)


        array = np.array(data, dtype=dtype)
        array.tofile(f'data/{args.ticker}/{timestamp_to_date(target)}.bin')

        write_log(f"{timestamp_to_date(target)} Fetched!")
        write_log("Wait for 10 seconds")
        time.sleep(10)


if __name__ == "__main__":
    main()