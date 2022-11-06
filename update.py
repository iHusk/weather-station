from datetime import datetime
from prefect import task, flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner

import pandas as pd

import os
import csv

RAIN_ITERATOR = 0.2794 # mm
ANEMOMETER_ITERATOR = 2.4 # km/h

# file to log data in
CACHE_PATH = "/home/admin/main/data/weather-station/cache"
ARCHIVE_PATH = "/home/admin/main/data/weather-station/archive"
CURRENT_DATA = "/home/admin/main/data/weather-station/current.csv"

def c_to_f(c):
    """
    Given degrees Celcius, returns Fahrenheit
    """
    return round(((c*9)/5)+32, 0)

@task
def process_data(df):
    """
    This function processes the raw data received from the sensors
    """
    temp = pd.DataFrame()
    temp['DATETIME'] = pd.to_datetime(df['DATETIME'],unit='s')
    # floor to minute frequency
    temp['DATETIME_t'] = temp['DATETIME'].dt.floor('T')
    # we want the change from last reading 
    temp['RAIN'] = (df['RAIN']-(df['RAIN'].shift(1)))*RAIN_ITERATOR
    temp['WIND_SPEED'] = (df['WIND_SPEED']-(df['WIND_SPEED'].shift(1)))*ANEMOMETER_ITERATOR
    temp['TEMPERATURE'] = c_to_f((df['TEMP_BMP']+df['TEMP_SHT'])/2)
    temp['PRESSURE'] = round(df['PRESSURE'], 2)
    temp['HUMIDITY'] = round(df['HUMIDITY'], 2)

    return temp


@task
def process_data_live(data):
    """
    Group by the datetime trunc. Were taking the median, but will need to process wind direction differently. 
    I want a wind gust measurement that is max wind speed
    """
    data = data.groupby(['DATETIME_t']).median().reset_index()
    return data


@flow(task_runner=SequentialTaskRunner())
def process_flow():
    """
    This flow processes items in the cache, updates the db, and archives data. 
    """
    logger = get_run_logger()

    files = len(os.listdir(CACHE_PATH))
    i = 1

    if len(os.listdir(CACHE_PATH)) == 0:
        logger.info(f'Nothing to process...')
    else:
        for file in os.listdir(CACHE_PATH):
            logger.info(f'Processing {i} of {files} files...')
            file_path = f'{CACHE_PATH}/{file}'
            data = pd.read_csv(file_path)

            data = process_data(data)

            live_data = process_data_live(data)

            live_data.to_csv(CURRENT_DATA, mode='a+', index=False, header=False)
            data.to_csv(f'{ARCHIVE_PATH}/{file[:7]}.csv', mode='a+', index=False, header=False)
            
            os.rename(file_path, f'{ARCHIVE_PATH}/records/{file}')

            i += 1


if __name__ == "__main__":
    process_flow()