import concurrent.futures
import json
import logging
import requests


# The base endpoint for the Binance RESTful API
BASE_REST = "https://api.binance.com"

# API GET endpoints to perform several checks
CONN_TEST = "/api/v3/ping"
SERVER_TIME = "/api/v3/time"

# Additional End Points to retrieve information for various trading markets
EXCHANGE_INFO = '/api/v3/exchangeInfo'

# GET endpoint to retrieve klines data from Binance
KLINES = '/api/v3/klines'

# Global list for all the available markets
MARKETS = None

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO)
logger = logging.getLogger(__name__)





def requests_error_handeling(func):
  """
  Decorator for error handeling,
  Mainly, log and ignore the error as this module is meant to be use periodicly,
  This ensure safe return in case of an exception.
  Args:
    func:

  Returns:
    Modified function
  """
  def handler(*args, **kwargs):
    try:
      return func(*args, **kwargs)

    except requests.exceptions.ConnectionError:
      logger.error("Connection to server failed!", exc_info=True)

    except requests.exceptions.Timeout:
      logger.warning("Request Timeout for func:" + str(func) + "! Retrying.")

      try:
        return func(*args, **kwargs)
      except Exception as e:
        logger.error("Retry Failed", exc_info=True)

    except requests.exceptions.RequestException:
      logger.error("Exception occurred while handling the request for:" + str(func), exc_info=True)
  return handler



@requests_error_handeling
def update_market_list():
  """
  To get the list of all the available markets which are currently trading at binance
  Updates the global variable MARKETS
  Returns:
    <UNIX Time> Server Time
  """
  global MARKETS

  # GET the market price data for all the listed symbols on Binance
  URL = BASE_REST + EXCHANGE_INFO
  response = requests.get(URL)
  exchange_info = json.loads(response.text)

  # Use the price list to obtains names of all the markets
  market_names = []
  for market in exchange_info['symbols']:
    if market['status'] == "TRADING":
      market_names.append(market['symbol'])

  # Populate the global variable
  MARKETS = market_names
  logger.info('Market list updated!')

  return exchange_info['serverTime']



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



@requests_error_handeling
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
