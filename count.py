import os
import pandas as pd


def count_total_rows_in_csv_files(folder_path):
    total_rows = 0
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_csv(file_path)
            total_rows += len(df)

    return total_rows


folder_path = r"N:\Source_for_sale\券商研报"
total_rows = count_total_rows_in_csv_files(folder_path)
print("Total number of rows in all CSV files: ", total_rows)
