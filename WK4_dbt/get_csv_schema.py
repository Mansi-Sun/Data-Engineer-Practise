import pandas as pd

file_path='fhv_tripdata_2019-04.csv'
csv_df=pd.read_csv(file_path)
timestamp_columns=['pickup_datetime','dropOff_datetime']
csv_df[timestamp_columns] = csv_df[timestamp_columns] / 1000

csv_df[timestamp_columns] = csv_df[timestamp_columns].apply(pd.to_datetime, errors='coerce',unit='us')

print(csv_df.dtypes)