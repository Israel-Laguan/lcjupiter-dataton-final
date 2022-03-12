from data_processing import loadTrainData,loadTestData

def test_loadTrainData():
  trainData = loadTrainData()
  assert all(trainData.head())

def test_loadTestData():
  testData = loadTestData()
  assert all(testData.head())
