from influxdb import InfluxDBClient





class data_schema():
  """
  This class act as a data type for the elements to be inserted in the database
  schema for the data to be inserted in the database is
  >>> json_body = [
    {
        "measurement": <name_of_measurement>,
        "tags": {
            <key>:<value>
        },
        "time": "2018-03-28T8:01:00Z",
        "fields": {
            <key>:<value>
        }
    },....
]
  """

  def __init__(self, name, OHLC_data):

    self.measurement = name
    self.json_body = []

    for market in OHLC_data:
      self.json_body.append(
          self._format(market, OHLC_data[market])
      )

  def _format(self, market, OHLC):
    """
    To format the data according to the json schema
    Args:
      market: <str>
      OHLC: <dict> data corresponding to the 'market'

    Returns:
      <dict> element of the body of the schema
    """

    json_elem = {
      "measurement": self.measurement,
      "tags":{
        "Market": market
      },
      "time": OHLC['open_time'],
      "fields":{
        "Open":OHLC['open'],
        "Close":OHLC['close'],
        "High":OHLC['high'],
        "Low":OHLC['low'],
        "Volume":OHLC['volume']
      }

    }

    return json_elem
