import great_expectations as ge
import shutil
import datetime
import random
import os


def data_validation(raw_data_directory: str, good_data_directory: str, bad_data_directory: str) -> str:

    ct = datetime.datetime.now()
    ts = str(ct.timestamp())

    random_file = random.choice(os.listdir(raw_data_directory))
    file_path = raw_data_directory + "/" + random_file
    print(f'Here is the selected file: {file_path}')

    file_path_good_data = good_data_directory + '/' + 'good_data_' + str(ts) + '.csv'
    file_path_bad_data = bad_data_directory + '/' + 'bad_data_' + str(ts) + '.csv'

    # complete loop to move file based on quality

    df = ge.read_csv(file_path)

    rows = []

    for i in range(0, len(df)):
        temp_df = df.iloc[[i]]
        date_quality = dict(temp_df.expect_column_values_to_not_be_null(column='Date'))
        cpi_quality = dict(temp_df.expect_column_values_to_not_be_null(column='CPI'))

        if date_quality["success"] == False and i not in rows:
            rows.append(i)

        elif cpi_quality["success"] == False and i not in rows:
            rows.append(i)
    
    corrupted_ratio = len(rows) / len(df)
    print(f'Corrupted Ratio = {corrupted_ratio}')

    if corrupted_ratio == 0:
        shutil.move(file_path, os.path.join(good_data_directory, os.path.basename(file_path)))
    elif corrupted_ratio < 0.20:
        good_data = df.drop(index=rows)
        bad_data = df.iloc[rows]
        good_data.to_csv(file_path_good_data) 
        bad_data.to_csv(file_path_bad_data)
        print(f'Good data has been moved to: {file_path_good_data}')
        print(f'Bad data has been moved to: {file_path_bad_data}')
    else:
        shutil.move(file_path, os.path.join(bad_data_directory, os.path.basename(file_path)))

    if os.path.isfile(file_path) and corrupted_ratio != 0:
        os.remove(file_path)
        print("File deleted successfully")
    elif corrupted_ratio == 0:
        print("File is not corrupted and has been move to good data directory already")
    else:
        print("Error: %s file not found" % file_path)
