import RPi.GPIO as GPIO
import board
import busio
import digitalio
import adafruit_bmp280
import adafruit_sht31d
import time
import csv

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
DATA_PATH = "/home/admin/main/data/cache"


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
def calibrate_bmp(sensor):
    """
    This function calibrates the bmp sensor for pressure at sea level dependent on elevation and temperature

    https://keisan.casio.com/exec/system/1224575267

    calibration: float
    """
    temp = sensor.temperature

    calibration = pow((1-((0.0065*ELEVATION)/(temp+(0.0065*ELEVATION)+273.15))),-5.257)*sensor.pressure

    sensor.sea_level_pressure = calibration

    return calibration


@task
def write_data(sensor_bmp, sensor_sht, calibration):
    """
    This function writes the raw data to the cache
    """
    log_file_path = f'{DATA_PATH}/{datetime.now()}.csv'

    with open(log_file_path, 'a+', newline='') as file:
        writer = csv.writer(file)

        # Write schema
        writer.writerow(['DATETIME','RAIN','WIND_SPEED','WIND_DIRECTION','TEMP_BMP','TEMP_SHT','PRESSURE','HUMIDITY','CAL_ALTITUDE','CAL_SPL'])

        i = 0 

        while i < 900:
            writer.writerow([time.time(),RAIN,WIND,analog_read(),sensor_bmp.temperature,sensor_sht.temperature,sensor_bmp.pressure,sensor_sht.relative_humidity,sensor_bmp.altitude,calibration])
            i += 1
            time.sleep(0.82)

        file.close()


@flow(task_runner=SequentialTaskRunner())
def main_flow():
    """
    Main task runner
    """
    logger = get_run_logger()

    logger.info("Setting up board...")
    bmp, sht = setup_board()

    logger.info("Calibrating sensors...")
    calibrated = calibrate_bmp(bmp)

    logger.info("Writing data...")
    write_data(bmp, sht, calibrated)

    logger.info("Cleaning up...")
    GPIO.cleanup()


if __name__ == "__main__":
    main_flow()