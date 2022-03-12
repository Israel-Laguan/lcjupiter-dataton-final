import pandas as pd
import string
from skimpy import clean_columns

printable = set(string.printable)

def remove_spec_chars(in_str):
    return ''.join([c for c in in_str if c in printable])

def loadTrainData(path="content/sample_data/train.csv"):
  try:
    dataframeTrainData = pd.read_csv(path, dtype=str, low_memory=False)
  except pd.errors.EmptyDataError:
    print('CSV file empty')
  dataframeTrainData.columns.apply(remove_spec_chars)
  return clean_columns(dataframeTrainData)

def loadTestData(path="content/sample_data/test.csv"):
  try:
    dataframeTestData = pd.read_csv(path, sep=';', encoding="iso-8859-1", on_bad_lines = 'warn')
  except pd.errors.EmptyDataError:
    print('CSV file empty')
  dataframeTestData.columns.str.replace(r'[^A-Za-z0-9]+', '')
  return clean_columns(dataframeTestData)