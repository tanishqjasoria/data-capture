import argparse
import binance_data
import logging
import pandas as pd
import subprocess
import time
import threading

from collections import defaultdict
from influxdb import InfluxDBClient

# Default HOST and PORT for the local instance of InfluxDB
HOST = 'localhost'
PORT = 8086

# Name of the database where the values needs to be stored
DATABASE = "crypto_ticker"

# Various measurements to be used for storing various ticker data
MEASUREMENT_1M = 'CRYPTO_1M'
MEASUREMENT_5M = 'CRYPTO_5M'
MEASUREMENT_15M = 'CRYPTO_15M'
MEASUREMENT_30M = 'CRYPTO_30M'
MEASUREMENT_1H = 'CRYPTO_1H'

# Time precision of the Binance server is in milliseconds
PRECISION = 'ms'

# Arrays to store required information to calculate 5M, 15M, 30M, 1H Ticker.
TICKER_5M = []
TICKER_15M = []
TICKER_30M = []
TICKER_1H = []

# Store the amount of data points collected. Would be useful,
# in calculation of tick data for different intervals
COUNT = 0

# Quality check intervals(in hours)
INTERVAL = 2

LOCK = threading.Lock()

logging.basicConfig(format='%(asctime)s:%(levelname)s- %(message)s|%(name)s',
    level=logging.INFO)
logger = logging.getLogger(__name__)





def parse_arguments():
  """
  Parse command line arguments.

  Returns:
    <dict> parsed command line arguments.
  """

  ap = argparse.ArgumentParser()
  ap.add_argument("-M", "--mode", required=False, help="Host Address of InfluxDB")
  args = vars(ap.parse_args())

  return args



def database_setup():
  """
  Initialize the database for quality check
  Returns:
    <InfluxDBClient>
  """

  # Initialise the InfluxDB client
  client = InfluxDBClient(HOST, PORT)

  # Create the required database, if the databse is already present
  # this would just return without any exception
  client.create_database(DATABASE)
  client.switch_database(DATABASE)
  logger.info("Database setup complete.")
  return client



def convert_to_json_schema(measurement, OHLC_data):
  """
  To convert obtained data into the required format for the database insertion
  Args:
    measurement: <str> choose from available measurements
    OHLC_data: <dict> data returned by the binance_data.retrieve_OHLC

  Returns:
    <list> json_body conforming to the schema required by InfluxDB
  """
  json_body = []

  for market in OHLC_data:
    json_body.append(
        _format(measurement, market, OHLC_data[market])
    )
  return json_body



def record_update(client):
  """
  A full pipeline from fetch (from Binance) to insert (InfluxDB)
  """

  # If the server is not available, retry after 10s. If the server is still
  # not reachable - exception is raised!

  try:
    binance_data.ping()
  except Exception:
    time.sleep(10)
    binance_data.ping()

  logger.info("Record Start.")
  OHLC_data, errors = binance_data.retrieve_OHLC(binance_data.MARKETS)
  json_body = convert_to_json_schema(MEASUREMENT_1M, OHLC_data)
  client.write_points(json_body,time_precision=PRECISION)
  logger.info("[1M] Write to Database successful.")
  calculate_tick_data(client, OHLC_data)



def run_data_collection(client):
  """
  Now the primary functionality if to retrieve market data from binance
  every 1 minute (1m Ticker). To accomplish that 'record_update' need to
  be scheduled to run every one second. time.sleep() is a blocking call,
  therefore using the threading module to add parallelism to the module
  The main functionality - spawn a new 'record_update' thread every 1 minute
  Args:
    client: database client which need to be used to store the collected data

  Returns:
    <None>
  """

  try:
    while True:
      # Initialize a process
      p = threading.Thread(target=record_update, args=(client,), daemon=True)
      # Start the process to run independently of main
      p.start()
      # TODO: Define a function which can act as a garbage collector,
      #  which can periodicly check for non active processes, free the
      #  resources and also return Exceptions to parent thread.
      time.sleep(60)
  except KeyboardInterrupt:
    # Kill all the processes which are alive
    logger.error("KeyboardInterrupt! Exiting.")
    exit(1)



def setup_run_testing_module():
  """
  Start the 'check_data_quality at regular intervals
  """

  proc = subprocess.Popen(
      ["python", "check_data_quality.py", "-DB"," crypto_ticker", "-C", str(COUNT)],
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE)
  time.sleep(3600 * INTERVAL)





def calculate_tick_data(client, OHLC_data):
  """
  To calculate ticker for various time intervals
  Args:
    OHLC_data: <dict>
  Returns:
    <None>
  """
  # global - to modify globally defined variables
  global TICKER_5M
  global TICKER_15M
  global TICKER_30M
  global TICKER_1H
  global COUNT

  COUNT = COUNT + 1
  logger.info("COUNT: " + str(COUNT))
  # How to calculate 5M, 15M, 30M, 1H ticker from 1M ticker?
  # Average of 5x 1M ticker would give 5M ticker
  # Average of 3x 5M ticker would give 15M ticker
  # Average of 2x 15M ticker would give 30M ticker
  # Average of 2x 30M ticker would give 1H ticker

  TICKER_5M.append(OHLC_data)

  if COUNT % 5 == 0:
    data = _process_ticker(TICKER_5M)
    TICKER_15M.append(data)
    json_body = convert_to_json_schema(MEASUREMENT_5M, data)
    client.write_points(json_body, time_precision=PRECISION)
    logger.info("[5M] Write to database complete.")
    TICKER_5M = []

  if COUNT % 15 == 0:
    data = _process_ticker(TICKER_15M)
    TICKER_30M.append(data)
    json_body = convert_to_json_schema(MEASUREMENT_15M, data)
    client.write_points(json_body, time_precision=PRECISION)
    logger.info("[15M] Write to database complete.")
    TICKER_15M = []

  if COUNT % 30 == 0:
    data = _process_ticker(TICKER_30M)
    TICKER_1H.append(data)
    json_body = convert_to_json_schema(MEASUREMENT_30M, data)
    client.write_points(json_body, time_precision=PRECISION)
    logger.info("[30M] Write to database complete.")
    TICKER_30M = []

  if COUNT % 60 == 0:
    _process_ticker(TICKER_1H)
    json_body = convert_to_json_schema(MEASUREMENT_1H, data)
    client.write_points(json_body, time_precision=PRECISION)
    logger.info("[1H] Write to database complete.")
    TICKER_1H = []



def _process_ticker(ticker):
  """
  To calculate ticker from all the tick data present in 'ticker' list
  Args:
    ticker: <list>

  Returns:
    <dict>

  """

  ticker_collection = defaultdict(list)

  for data in ticker:
    for market in binance_data.MARKETS:
      ticker_collection[market].append(data[market])

  ticker_average = {}
  for market, data in ticker_collection.items():
    df = pd.DataFrame(data)
    aggregator = {'open': lambda x:x[0], 'high': 'max', 'low': 'min', 'close':
      lambda x: list(x).pop(), 'volume': 'sum', 'open_time': lambda x: x[0],
      'close_time': lambda x:list(x).pop()}

    ticker_average[market] = dict(df.agg(aggregator))

  return ticker_average



def _format(measurement, market, OHLC):
  """
  To format the data according to the json schema
  Args:
    market: <str>
    OHLC: <dict> data corresponding to the 'market'

  Returns:
    <dict> element of the body of the schema
  """

  json_elem = {
    "measurement": measurement,
    "tags": {
      "Market": market
    },
    "time": int(OHLC['open_time']),
    "fields": {
      "Open": OHLC['open'],
      "Close": OHLC['close'],
      "High": OHLC['high'],
      "Low": OHLC['low'],
      "Volume": OHLC['volume']
    }

  }

  return json_elem





if __name__=='__main__':

  args = parse_arguments()

  # Populate the list of markets listed on Binance
  time_server = binance_data.update_market_list()

  # Setup the database for storing market data
  client = database_setup()

  if args['mode']:
    if args['mode'] == 'run':
      # Wait for new KLINE for 1M to start
      while True:
        # time in seconds
        curr_time = binance_data.server_time()//1000
        if curr_time % 60 <= 5:
          break
        time.sleep(0.5)

      logger.info("Initiating the testing module.")
      quality_check = threading.Thread(target=setup_run_testing_module, daemon=True)
      quality_check.start()

      logger.info("Starting data collection at: " + str(binance_data.server_time()))
      run_data_collection()
    else:
      logger.error("Enter valid arguments")
      exit(1)
