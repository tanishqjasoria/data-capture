import argparse
import binance_data

from influxdb import InfluxDBClient


# Default HOST and PORT for the instance of InfluxDB
HOST = 'localhost'
PORT = 8086

# Name of the database to be checked, initialized thorough command lin args.
DATABASE = None

# Various measurements to be used for storing various ticker data
MEASUREMENT_1M = 'CRYPTO_1M'
MEASUREMENT_5M = 'CRYPTO_5M'
MEASUREMENT_15M = 'CRYPTO_15M'
MEASUREMENT_30M = 'CRYPTO_30M'
MEASUREMENT_1H = 'CRYPTO_1H'

# Query to get the count of the number of records for each Market for each measurement.
COUNT_DATA_QUERY = "SELECT COUNT(\"High\") FROM " + DATABASE + "..{} GROUP BY \"Market\""





def parse_arguments():
  """
  Parse command line arguments.

  Returns:
    <dict> parsed command line arguments.
  """

  ap = argparse.ArgumentParser()

  ap.add_argument("-H", "--host", required=False, help="Host Address of InfluxDB")
  ap.add_argument("-P", "--port", required=False, help="Port for InfluxDB")
  ap.add_argument("-DB", "--database", required=True, help="Name of Database to be analyzed")
  ap.add_argument("-C", "--count", required=True, help="Number of records inserted")

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
  return client


