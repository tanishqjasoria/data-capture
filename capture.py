import json
import requests
import websocket


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



def ping():
  """
  To check if the Binance server is reachable

  Exceptions:
    ConnectionError,, if the connection to server is not possible
  """
  URL = BASE_REST + CONN_TEST

  response = requests.get(URL)

  if response.status_code != 200:
    raise ConnectionError("Connection to the server not possible." +
        response.text)



def server_time():
  """
  To get the server time in UNIX time format
  Returns:
    <int>
  """
  URL = BASE_REST + SERVER_TIME

  response = requests.get(URL)
  UNIX_time = json.loads(response.text)["serverTime"]

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

  OHLC_markets = []

  for market in markets:
    data = _retrieve_REST(market)
    OHLC_markets.append(data)

  return OHLC_markets
