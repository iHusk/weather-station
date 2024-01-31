#!/usr/bin/env python3

import RPi.GPIO as GPIO
import board
import busio
import digitalio
import adafruit_bmp280
import adafruit_sht31d
import time
import csv
import os
import glob
import json

import pandas as pd

from datetime import datetime
from prefect import task, flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from kafka import KafkaProducer

# https://aminoapps.com/c/studying-amino/page/item/even-more-headers-dividers/XpRD_MoHXIgjKgkNWxba4NGEYKgzaDnPJX


# ╭───────────╮
# | CONSTANTS |
# ╰───────────╯

RAIN_ITERATOR = 0.2794 # mm
ANEMOMETER_ITERATOR = 2.4 # km/h

ELEVATION = 370 # m

# which GPIO pin the gauge is connected to
PIN_RAIN_GUAGE = 17
PIN_ANENOMETER = 4
PIN_WIND_VANE_A = 27
PIN_WIND_VANE_B = 22

# global variables
RAIN = 0
WIND = 0

# file to log data in
DATA_PATH = "/home/admin/weather-station/data/cache"
ARCHIVE_PATH = "/home/admin/weather-station/data/archive"
CURRENT_PATH = "/home/admin/weather-station/data"


# ╭─────────────────╮
# | BOARD FUNCTIONS |
# ╰─────────────────╯


def discharge():
    """
    This function discharges the on-board capacitor
    """
    GPIO.setup(PIN_WIND_VANE_A, GPIO.IN)
    GPIO.setup(PIN_WIND_VANE_B, GPIO.OUT)
    GPIO.output(PIN_WIND_VANE_B, False)
    time.sleep(0.005)

def charge_time():
    """
    This function returns a value representing how long it takes for the on-board capacitor to charge.

    temp: int
    """
    temp = 0

    GPIO.setup(PIN_WIND_VANE_A, GPIO.OUT)
    GPIO.setup(PIN_WIND_VANE_B, GPIO.IN)

    GPIO.output(PIN_WIND_VANE_A, True)

    ## until we get an input on pin B, iterate temp. 
    while not GPIO.input(PIN_WIND_VANE_B):
        temp += 1

    return temp


def analog_read():
    """
    This function discharges the capacitor and then returns a value representing the time it takes
    the on-board capacitor to charge
    """
    discharge()
    return charge_time()


def read_temp(device, decimals = 4):
    """
    Reads the temperature from a 1-wire device
    https://raspberrypi-guide.github.io/electronics/temperature-monitoring
    """

    with open(device, "r") as f:
        lines = f.readlines()
    while lines[0].strip()[-3:] != "YES":
        lines = read_temp_raw()
    equals_pos = lines[1].find("t=")
    if equals_pos != -1:
        temp_string = lines[1][equals_pos+2:]
        temp = round(float(temp_string) / 1000.0, decimals)
        return temp


# callback functions
def rain_cb(channel):
    """
    This callback function iterates the rain variable
    """
    global RAIN
    RAIN += 1

def wind_cb(channel):
    """
    This callback function iterates the wind variable
    """
    global WIND
    WIND += 1

def c_to_f(c):
    """
    Given degrees Celcius, returns Fahrenheit
    """
    return round(((c*9)/5)+32, 0)




# ╭───────────────╮
# | PREFECT TASKS |
# ╰───────────────╯


@task
def setup_board():
    """
    This function sets up the raspberry pi for the sensors we have on board and returns those sensor objects. 

    bmp: object
    sht: object
    """
    # board setup for the BMP280 and SHT31D sensors - SPI and I2C interface
    spi = board.SPI()
    i2c = busio.I2C(board.SCL, board.SDA)
    cs = digitalio.DigitalInOut(board.D5)
    bmp = adafruit_bmp280.Adafruit_BMP280_SPI(spi, cs)
    sht = adafruit_sht31d.SHT31D(i2c)

    # https://raspberrypi.stackexchange.com/questions/75940/measuring-resistance-without-adc
    GPIO.setmode(GPIO.BCM)  
    GPIO.setup(PIN_RAIN_GUAGE, GPIO.IN, pull_up_down=GPIO.PUD_UP)
    GPIO.setup(PIN_ANENOMETER, GPIO.IN, pull_up_down=GPIO.PUD_UP)

    # register the call back for pin interrupts, note GPIO.FALLING looks for falling cliff. 
    # bounce time limits how often it can be called, which might need to be adjusted 
    # 11/4/22 300, 10
    GPIO.add_event_detect(PIN_RAIN_GUAGE, GPIO.FALLING, callback=rain_cb, bouncetime=300)
    GPIO.add_event_detect(PIN_ANENOMETER, GPIO.FALLING, callback=wind_cb, bouncetime=10)

    # 1-wire Temperature Sensor - DS18B20
    tmp = glob.glob("/sys/bus/w1/devices/" + "28*")[0] + "/w1_slave"

    return bmp, sht, tmp


@task
def calibrate_temperature(bmp, sht, tmp):
    """
    This function just averages temps from the sensors and checks to make sure sensors are working.
    Used to calibrate pressure and as the GRAT temp. 
    """
    logger = get_run_logger()

    bmp_t = bmp.temperature
    sht_t = sht.temperature
    tmp_t = read_temp(tmp, 2)
    logger.info(f'Real temp {tmp_t}')

    # if our sensors differ by more than 1 Celcius, there is probably an issue
    if abs(bmp_t - sht_t) > 1:
        logger.warning("Questionable differences in temperatures, check sensors...")
        logger.info(f'BMP: {bmp_t} | SHT: {sht_t}')
    
    temp = (bmp_t + sht_t) / 2

    return temp

@task
def calibrate_bmp(sensor, temperature):
    """
    This function calibrates the bmp sensor for pressure at sea level dependent on elevation and temperature

    https://keisan.casio.com/exec/system/1224575267

    calibration: float
    """
    calibration = pow((1-((0.0065*ELEVATION)/(temperature+(0.0065*ELEVATION)+273.15))),-5.257)*sensor.pressure

    sensor.sea_level_pressure = calibration

    return calibration


@task
def produce_data(sensor_bmp, sensor_sht, tmp, calibration, producer):


    ## Must change rename below if you touch this
    log_file_path = f'{CURRENT_PATH}/{datetime.now().strftime("%Y%m%d-%H%M%S")}.csv'

    with open(log_file_path, 'a+', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['DATETIME','RAIN','WIND_SPEED','WIND_DIRECTION','TEMP_TMP','TEMP_BMP','TEMP_SHT','PRESSURE','HUMIDITY','CAL_ALTITUDE','CAL_SPL'])

        while True:
            ## UTC Time
            if time.strftime("%H") == '05':
                if int(time.strftime("%M")) >= 59:
                    print("Time to break")
                    break
            try:
                data_json = {
                    "datetime":time.time(),
                    "rain":RAIN, 
                    "wind":WIND, 
                    "wind_direction":analog_read(), 
                    "tmp_temp":read_temp(tmp, 4), 
                    "bmp_temp":sensor_bmp.temperature, 
                    "sht_temp":sensor_sht.temperature, 
                    "pressure":sensor_bmp.pressure, 
                    "humidity":sensor_sht.relative_humidity, 
                    "cal_alt":sensor_bmp.altitude, 
                    "cal_slp":calibration
                    }
                data_json = json.dumps(data_json)
                data_list = json.loads(data_json)   
                # data_list = f"{data_list['data']['datetime']}, {data_list['data']['rain']}, {data_list['data']['wind']}, {data_list['data']['wind_direction']}, {data_list['data']['tmp_temp']}, {data_list['data']['bmp_temp']}, {data_list['data']['sht_temp']}, {data_list['data']['pressure']}, {data_list['data']['humidity']}, {data_list['data']['cal_alt']}, {data_list['data']['cal_slp']}"       
                # data = f'[{time.time()},{RAIN},{WIND},{analog_read()},{sensor_bmp.temperature},{sensor_sht.temperature},{read_temp(tmp, 4)},{sensor_bmp.pressure},{sensor_sht.relative_humidity},{sensor_bmp.altitude},{calibration}]'
                writer.writerow([data_list["datetime"], data_list["rain"], data_list["wind"], data_list["wind_direction"], data_list["tmp_temp"], data_list["bmp_temp"], data_list["sht_temp"], data_list["pressure"], data_list["humidity"], data_list["cal_alt"], data_list["cal_slp"]])
                encoded_message = data_json.encode("utf-8")
                producer.send("20221111-test", encoded_message)
            except Exception as e:
                print(f'EXCEPTION: {e}')


        file.close()
    
    ## Must change this if you change filename above
    os.rename(log_file_path, f'{DATA_PATH}/{log_file_path[-19:]}')

    return True

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
    # TODO: FIX THE NAN FOR RAIN AND WIND SPEED ON ROW 0 (or ?)
    temp['RAIN'] = (df['RAIN']-df['RAIN'].shift(1))*RAIN_ITERATOR
    temp['WIND_SPEED'] = (df['WIND_SPEED']-df['WIND_SPEED'].shift(1))*ANEMOMETER_ITERATOR
    temp['TEMPERATURE'] = round(c_to_f(df['TEMP_TMP']), 2)
    temp['PRESSURE'] = round(df['PRESSURE'], 2)
    temp['HUMIDITY'] = round(df['HUMIDITY'], 2)

    return temp


@flow(task_runner=SequentialTaskRunner())
def record_data_flow():
    """
    Secondary task runner for processing data just recorded
    """
    logger = get_run_logger()

    i = 1

    for file in os.listdir(DATA_PATH):
        logger.info(f'Processing file {i} of {len(os.listdir(DATA_PATH))}...')
        file_path = f'{DATA_PATH}/{file}'

        try:
            data = pd.read_csv(file_path)
            logger.info("Processing...")
            try:
                data = process_data(data)
            except Exception as e:
                logger.warning("Error in process function...")
                logger.warning(e)

            try:
                data.to_csv(f'{ARCHIVE_PATH}/{file[:7]}.csv', mode='a+', index=False, header=False)
            except Exception as e:
                logger.warning("Error writing to file...")
                logger.warning(e)

            try:
                os.rename(file_path, f'{ARCHIVE_PATH}/records/{file}')
            except Exception as e:
                logger.warning("Error renaming files...")
                logger.warning(e)

        except Exception as e:
            logger.warning(f'Not able to process {file}')
            logger.warning(f'{e}')

        i += 1

@task
def setup_producer():
    producer = KafkaProducer(bootstrap_servers=['192.168.0.25:9092'])

    return producer

@flow(task_runner=SequentialTaskRunner())
def weather_logging_flow():
    """
    Main task runner
    """
    logger = get_run_logger()

    logger.info("Setting up board...")
    try:
        bmp, sht, tmp = setup_board()
        logger.info("Board setup!")
    except Exception as e:
        logger.error("Error setting up board...")
        logger.warning(e)

    logger.info("Setting up Kafka")
    try:
        producer = setup_producer()
    except Exception as e:
        logger.error("Error settting up Kafka...")
        logger.warning(e)

    logger.info("Calibrating sensors...")
    try:
        temp = calibrate_temperature(bmp, sht, tmp)
    except Exception as e:
        logger.error("Error calibrating temperature...")
        logger.warning(e)

    try:
        calibrated = calibrate_bmp(bmp, temp)
    except Exception as e:
        logger.error("Error calibrating bmp...")
        logger.warning(e)

    logger.info("Writing data...")
    try:
        temp = produce_data(bmp, sht, tmp, calibrated, producer)
        # written_date = write_data(bmp, sht, calibrated)
    except Exception as e:
        logger.error("Error writing data...")
        logger.warning(e)

    record_data_flow()

    logger.info("Cleaning up...")
    try:
        GPIO.cleanup()
        producer.flush()
        producer.close()
    except Exception as e:
        logger.error("Error cleaning up...")
        logger.warning(e)

    # record_data_flow()


if __name__ == "__main__":
    weather_logging_flow()