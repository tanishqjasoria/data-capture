from influxdb import InfluxDBClient


# Default HOST and PORT for the local instance of InfluxDB
HOST = 'localhost'
PORT = 8086

# Name of the databse where the values needs to be stored
DATABASE = "crypto_ticker"





def database_setup():

  # Initialise the InfluxDB client
  client = InfluxDBClient(HOST, PORT)

  # Create the required database, if the databse is already present
  # this would just return without any exception
  client.create_database(DATABASE)
  client.switch_database(DATABASE)
  return client





if __name__=='__main__':
  # Setup the database for storing market data
  client = database_setup()
