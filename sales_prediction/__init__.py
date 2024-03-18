TRAIN_FEATURES = ['Store', 'Dept', 'Date', 'Weekly_Sales', 'Temperature', 'Fuel_Price',
       'MarkDown1', 'MarkDown2', 'MarkDown3', 'MarkDown4', 'MarkDown5', 'CPI',
       'Unemployment', 'IsHoliday', 'Type', 'Size', 'dayofmonth', 'dayofweek',
       'quarter', 'month', 'year', 'dayofyear', 'weekofyear',
       'CPI_Difference']

TEST_FEATURES = ['Store', 'Dept', 'Date', 'Weekly_Sales', 'Temperature', 'Fuel_Price',
       'MarkDown1', 'MarkDown2', 'MarkDown3', 'MarkDown4', 'MarkDown5', 'CPI',
       'Unemployment', 'IsHoliday', 'Type', 'Size', 'dayofmonth', 'dayofweek',
       'quarter', 'month', 'year', 'dayofyear', 'weekofyear',
       'CPI_Difference']

MODEL_BASE_PATH = '../models/'

FEATURES_TO_DROP = ['Store','Weekly_Sales', 'Date', 'Fuel_Price',
                    'CPI', 'Unemployment', 'Type']