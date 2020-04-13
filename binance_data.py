import concurrent.futures
import json
import logging
import requests


# The base endpoint for the Binance RESTful API
BASE_REST = "https://api.binance.com"

# API GET endpoints to perform several checks
CONN_TEST = "/api/v3/ping"
SERVER_TIME = "/api/v3/time"

# Additional End Points to retrieve information
MARKET_PRICE = "/api/v3/ticker/price"
DATA_24HR = "/api/v3/ticker/24hr"

# GET endpoint to retrieve klines data from Binance
KLINES = '/api/v3/klines'

# Global list for all the available markets
MARKETS = None

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)
logger = logging.getLogger(__name__)

def update_market_list():
  """
  To get the list of all the available markets
  Returns:
    <list> of all the market names
  """
  global MARKETS

  # GET the market price data for all the listed symbols on Binance
  URL = BASE_REST + MARKET_PRICE
  response = requests.get(URL)
  markets_price_list = json.loads(response.text)

  # Use the price list to obtains names of all the markets
  market_names = []
  for record in markets_price_list:
    name = record['symbol']
    market_names.append(name)

  # Populate the global variable
  MARKETS = market_names
  logger.info('Market list updated!')



def ping():
  """
  To check if the Binance server is reachable

  Exceptions:
    ConnectionError,, if the connection to server is not possible
  """
  URL = BASE_REST + CONN_TEST

  response = requests.get(URL)

  if response.status_code != 200:
    logger.error("Binance server unreachable")
    raise ConnectionError("Binance server unreachable" +
        response.text)
  logger.info("Binance server is UP!")



def server_time():
  """
  To get the server time in UNIX time format
  Returns:
    <int>
  """
  URL = BASE_REST + SERVER_TIME

  response = requests.get(URL)
  UNIX_time = json.loads(response.text)["serverTime"]

  logger.info("Binance server time: " + str(UNIX_time))
  return UNIX_time



def _retrieve_REST(market):
  """
  To retrieve the data using the RESTful API
  Args:
    market: <str> name of the market

  Returns:
    <dict> of raw OHLC data
  """
  URL = BASE_REST + KLINES

  params = {
    "symbol": market,
    "interval": '1m',
    "limit": 1
  }

  response = requests.get(URL, params=params)
  data = json.loads(response.text)
  return data



def retrieve_OHLC(markets):
  """
  To retrieve OHLC data for the required symbols from Binance
  Args:
    markets: <list> of symbols

  Returns:
    <dict> { "market": <name_of_market>, "data": <raw OHLC data> }
  """

  OHLC_markets = {}
  response_error = []

  logger.info("Fetching data from Binance.")
  with concurrent.futures.ThreadPoolExecutor(
      max_workers=len(MARKETS)) as executor:

    market_workers = {executor.submit(_retrieve_REST, market):
        market for market in markets}

    for worker in concurrent.futures.as_completed(market_workers):
      market = market_workers[worker]
      # print(market)
      try:
        raw_data = worker.result()
        data = raw_data[0]
        # print(data)
        OHLC = {
          "open": float(data[1]),
          "high": float(data[2]),
          "low": float(data[3]),
          "close": float(data[4]),
          "volume": float(data[5]),
          "open_time": data[0],
          "close_time": data[6]
        }
        OHLC_markets[market] = OHLC
      except Exception as error:
        print(error)
        response_error.append(market)

  logger.info("Data fetch completed!")
  return OHLC_markets, response_error
