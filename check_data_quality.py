import argparse





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


