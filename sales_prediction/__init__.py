TRAIN_FEATURES = ['Store', 'Dept', 'Date', 'Weekly_Sales', 'Temperature', 'Fuel_Price',
       'MarkDown1', 'MarkDown2', 'MarkDown3', 'MarkDown4', 'MarkDown5', 'CPI',
       'Unemployment', 'IsHoliday', 'Type', 'Size', 'dayofmonth', 'dayofweek',
       'quarter', 'month', 'year', 'dayofyear', 'weekofyear']

TEST_FEATURES = ['Store', 'Dept', 'Date', 'Temperature', 'Fuel_Price',
       'MarkDown1', 'MarkDown2', 'MarkDown3', 'MarkDown4', 'MarkDown5', 'CPI',
       'Unemployment', 'IsHoliday', 'Type', 'Size', 'dayofmonth', 'dayofweek',
       'quarter', 'month', 'year', 'dayofyear', 'weekofyear']

MODEL_BASE_PATH = '../models/'

FEATURES_TO_DROP = ['Store', 'Date', 'Fuel_Price',
              'CPI', 'Unemployment', 'Type']

GOOD_DATA_DIRECTORY = '../data/good_data'

BAD_DATA_DIRECTORY = '../data/bad_data'

RAW_DATA_DIRECTORY = '../data/raw_data'