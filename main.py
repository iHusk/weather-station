import RPi.GPIO as GPIO
import board
import busio
import digitalio
import adafruit_bmp280
import adafruit_sht31d
import time
import csv
import os

import pandas as pd

from datetime import datetime
from prefect import task, flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner

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

    return bmp, sht


@task
def calibrate_temperature(bmp, sht):
    """
    This function just averages temps from the sensors and checks to make sure sensors are working.
    Used to calibrate pressure and as the GRAT temp. 
    """
    logger = get_run_logger

    bmp_t = bmp.temperature
    sht_t = sht.temperature

    # if our sensors differ by more than 1 Celcius, there is probably an issue
    if abs(bmp_t - sht_t) > 1:
        logger.warning("Questionable differences in temperatures, check sensors...")
    
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
def write_data(sensor_bmp, sensor_sht, calibration):
    """
    This function writes the raw data to the cache
    """
    # log_file_path = f'{DATA_PATH}/{datetime.now()}.csv'
    log_file_path = f'{CURRENT_PATH}/{datetime.now()}.csv'

    with open(log_file_path, 'a+', newline='') as file:
        writer = csv.writer(file)

        # Write schema
        writer.writerow(['DATETIME','RAIN','WIND_SPEED','WIND_DIRECTION','TEMP_BMP','TEMP_SHT','PRESSURE','HUMIDITY','CAL_ALTITUDE','CAL_SPL'])

        i = 0 

        # new file every 5 minutes.
        while i < 300:
            try:
                writer.writerow([time.time(),RAIN,WIND,analog_read(),sensor_bmp.temperature,sensor_sht.temperature,sensor_bmp.pressure,sensor_sht.relative_humidity,sensor_bmp.altitude,calibration])
            except Exception as e:
                print(e)
            i += 1
            time.sleep(0.81)

        file.close()


    os.rename(log_file_path, f'{DATA_PATH}/{log_file_path[-30:]}')

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
    temp['TEMPERATURE'] = round(c_to_f((df['TEMP_BMP']+df['TEMP_SHT'])/2), 4)
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

@flow(task_runner=SequentialTaskRunner())
def weather_logging_flow():
    """
    Main task runner
    """
    logger = get_run_logger()

    logger.info("Setting up board...")
    try:
        bmp, sht = setup_board()
        logger.info("Board setup!")
    except Exception as e:
        logger.error("Error setting up board...")
        logger.warning(e)

    logger.info("Calibrating sensors...")
    try:
        temp = calibrate_temperature(bmp, sht)
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
        written_date = write_data(bmp, sht, calibrated)
    except Exception as e:
        logger.error("Error writing data...")
        logger.warning(e)

    record_data_flow()

    logger.info("Cleaning up...")
    try:
        GPIO.cleanup()
    except Exception as e:
        logger.error("Error cleaning up...")
        logger.warning(e)

    record_data_flow()


if __name__ == "__main__":
    weather_logging_flow()