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
COUNT_DATA_QUERY = "SELECT COUNT(\"High\") FROM {}..{} GROUP BY \"Market\""





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



def get_number_of_record(client, measurement):
  """
  Get the number of records of a particular series for all the markets
  Args:
    client: <InfluxDBClient>
    measurement: <str> name of the series

  Returns:
    <list> - element would be of form [<name_of_market>, <number_of_records>]
  """

  records = client.query(COUNT_DATA_QUERY.format(DATABASE, measurement))
  market_numbers = []

  for market in binance_data.MARKETS:
    for record in records.get_points(tags={'Market': market}):
      if record:
        market_numbers.append([market, record['count']])

  return market_numbers





if __name__=="__main__":
  # global HOST
  # global PORT
  # global DATABASE

  report = "report.txt"
  args = parse_arguments()

  if args['host']:
    HOST = args['host']
  if args['port']:
    PORT = args['port']
  DATABASE = args['database']
  count = int(args['count'])

  # How many points in each ticker?
  points_1M = count
  points_5M = count//5
  points_15M = count//15
  points_30M = count//30
  points_1H = count//60


  binance_data.update_market_list()
  client = database_setup()

  # How many records are actually stored in the database?
  numbers_1M = get_number_of_record(client, MEASUREMENT_1M)
  numbers_5M = get_number_of_record(client, MEASUREMENT_5M)
  numbers_15M = get_number_of_record(client, MEASUREMENT_15M)
  numbers_30M = get_number_of_record(client, MEASUREMENT_30M)
  numbers_1H = get_number_of_record(client, MEASUREMENT_1H)

  # What is the percentage of actually stored records?
  percent_1M = [[x[0], 100 * x[1]/count] for x in numbers_1M]
  percent_5M = [[x[0], 100 * x[1]/(count//5)] for x in numbers_5M]
  percent_15M = [[x[0], 100 * x[1]/(count//15)] for x in numbers_15M]
  percent_30M = [[x[0], 100 * x[1]/(count//30)] for x in numbers_30M]
  percent_1H = [[x[0], 100 * x[1]/(count//60)] for x in numbers_1H]

  length_markets = len(binance_data.MARKETS)

  # Average percent over all the markets
  average_percent = [
    sum([x[1] for x in percent_1M])/length_markets,
    sum([x[1] for x in percent_5M])/length_markets,
    sum([x[1] for x in percent_15M])/length_markets,
    sum([x[1] for x in percent_30M])/length_markets,
    sum([x[1] for x in percent_1H])/length_markets
  ]

  # Create a report for the quality of data
  with open(report, 'w') as rep:
    rep.write('Number of Markets: {}\n'.format(length_markets))
    rep.write('Number of points Expected: {}\n'.format(
        points_1M + points_5M + points_15M + points_30M + points_1H))

    header = "{:12s} {:12s} {:12s} {:12s} {:12s} {:12s}\n".format(" ", MEASUREMENT_1M,
        MEASUREMENT_5M, MEASUREMENT_15M, MEASUREMENT_30M, MEASUREMENT_1H)
    rep.write(header)

    formart_record = "{:12s} {:10.2f} {:10.2f} {:10.2f} {:10.2f} {:10.2f}\n"

    for i in range(length_markets):
      rep.write(formart_record.format(percent_1M[i][0], percent_1M[i][1], percent_5M[i][1],
          percent_15M[i][1], percent_30M[i][1], percent_1H[i][1]))

    rep.write(formart_record.format("% average", average_percent[0],
        average_percent[1], average_percent[2], average_percent[3], average_percent[4]))
