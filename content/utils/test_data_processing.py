from .data_processing import load_csv_data

def test_loadTrainData():
  trainData = load_csv_data()
  assert all(trainData.head())
