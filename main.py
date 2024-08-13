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

def fetch_klines(start_time):
    url = "https://www.binance.com/api/v3/uiKlines"
    params = {
        "startTime": start_time,
        "limit": 900,
        "symbol": "BTCUSDT",
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

def main():
    log = open("log.txt", "a")

    print("Starting Fetch")
    log.write(f"\n{get_time_string()} LOG: Starting Fetch")

    try:
        while True:
            target = date_to_timestamp(sorted(os.listdir("data/BTCUSDT"))[-1].replace(".bin", "")) + 86400000

            if target >= year2024:
                print("All data has been fetched")
                log.write(f"\n{get_time_string()} LOG: All data has been fetched")
                break

            print(f"Fetching {timestamp_to_date(target)}")
            log.write(f"\n{get_time_string()} LOG: Fetching {timestamp_to_date(target)}")

            data = []
            for i in range(96):
                print(f"{i} / 96")
                data += fetch_klines(target + (i * 900000))
                time.sleep(0.5)

            array = np.array(data, dtype=dtype)
            array.tofile(f'data/BTCUSDT/{timestamp_to_date(target)}.bin')

            print(f"{timestamp_to_date(target)} Fetched!")
            log.write(f"\n{get_time_string()} LOG: {timestamp_to_date(target)} Fetched!")
            print("Wait for 10 seconds")
            time.sleep(10)
    except Exception as e:
        print("Error Occurred!")
        print(str(e))
        log.write(f"\n{get_time_string()} ERR: {str(e)}")

    log.close()


if __name__ == "__main__":
    main()