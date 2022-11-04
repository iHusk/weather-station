import RPi.GPIO as GPIO
import board
import busio
import digitalio
import adafruit_bmp280
import adafruit_sht31d
import time

# CONSTANTS
RAIN_ITERATOR = 0.2794 # mm
ANEMOMETER_ITERATOR = 2.4 # km/h
ELEVATION = 370 # m

# which GPIO pin the gauge is connected to
PIN_RAIN_GUAGE = 17
PIN_ANENOMETER = 4
PIN_WIND_VANE_A = 27
PIN_WIND_VANE_B = 22

# file to log data in
LOGFILE = "/home/admin/weather-station/data/cache/weather.csv"

spi = board.SPI()
i2c = busio.I2C(board.SCL, board.SDA)
cs = digitalio.DigitalInOut(board.D5)
bmp = adafruit_bmp280.Adafruit_BMP280_SPI(spi, cs)
sht = adafruit_sht31d.SHT31D(i2c)

# Sea pressure level
bmp.sea_level_pressure = pow((1-((0.0065*ELEVATION)/(bmp.temperature+(0.0065*ELEVATION)+273.15))),-5.257)*bmp.pressure

# https://raspberrypi.stackexchange.com/questions/75940/measuring-resistance-without-adc
GPIO.setmode(GPIO.BCM)  
GPIO.setup(PIN_RAIN_GUAGE, GPIO.IN, pull_up_down=GPIO.PUD_UP)
GPIO.setup(PIN_ANENOMETER, GPIO.IN, pull_up_down=GPIO.PUD_UP)

def discharge():
    GPIO.setup(PIN_WIND_VANE_A, GPIO.IN)
    GPIO.setup(PIN_WIND_VANE_B, GPIO.OUT)
    GPIO.output(PIN_WIND_VANE_B, False)
    time.sleep(0.005)

def chaarge_time():
    GPIO.setup(PIN_WIND_VANE_A, GPIO.OUT)
    GPIO.setup(PIN_WIND_VANE_B, GPIO.IN)
    temp = 0

    GPIO.output(PIN_WIND_VANE_A, True)
    while not GPIO.input(PIN_WIND_VANE_B):
        temp += 1

    return temp

def analog_read():
    discharge()
    return chaarge_time()

# keep track
rain = 0
wind = 0

# callback functions
def rain_cb(channel):
	global rain
	rain += 1

def wind_cb(channel):
    global wind
    wind += 1

# register the call back for pin interrupts, note GPIO.FALLING looks for falling cliff. might need to adjust bounce time.
GPIO.add_event_detect(PIN_RAIN_GUAGE, GPIO.FALLING, callback=rain_cb, bouncetime=300)
GPIO.add_event_detect(PIN_ANENOMETER, GPIO.FALLING, callback=wind_cb, bouncetime=10)

# open the log file
file = open(LOGFILE, "a")

i = 0

# display and log results
while i < 30:
    direction = analog_read()
    line = f'{time.time()},{rain},{wind},{direction},{bmp.temperature},{bmp.pressure},{bmp.altitude},{sht.temperature},{sht.relative_humidity}'
    print(line)
    file.write(line+"\n")
    file.flush()
    rain = 0
    wind = 0
    time.sleep(1)
    i += 1


# close the log file and exit nicely
file.close()
GPIO.cleanup()