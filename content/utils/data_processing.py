import pandas as pd
import sys
import string

def remove_spec_chars(in_str):
  printable = set(string.printable)
  cleanName = in_str.replace(r'[^A-Za-z0-9]+', '')
  return ''.join([c for c in cleanName if c in printable])

def load_csv_data(path="./content/sample_data/train.csv"):
  try:
    dataframeData = pd.read_csv(path, dtype=str, low_memory=False, usecols=lambda x: remove_spec_chars(x))
    return dataframeData
  except KeyError as e:
    print("Expected column headers not found")
    sys.exit(1)
  except TypeError as e:
      print("Type Error")
      sys.exit(1)
  except FileNotFoundError as e:
    print("CSV file not found " + str(e))
    sys.exit(1)
  except Exception as e:
    print(e)
    sys.exit(1)

def set_index(dataframe, column = 'id'):
  dataframe[column] = pd.to_numeric(dataframe[column], errors='coerce')
  dataframe = dataframe.set_index(column)
  return dataframe