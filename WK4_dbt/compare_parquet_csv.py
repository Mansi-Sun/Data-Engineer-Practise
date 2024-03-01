import pandas as pd
import pyarrow.parquet as pq

# Specify file paths
parquet_file_path = 'new_fhv_tripdata_2019-04.parquet'
csv_file_path = 'fhv_tripdata_2019-04.csv'

# Read data from Parquet file
parquet_table = pq.read_table(parquet_file_path)
parquet_df = parquet_table.to_pandas()

# Read data from CSV file
csv_df = pd.read_csv(csv_file_path)

# Compare data
# Use any relevant method for comparison, such as Pandas equals or any other logic
# For example, you can check if the DataFrames are equal
are_dataframes_equal = parquet_df.equals(csv_df)

# Display the result
print(f"Are the datasets equal? {are_dataframes_equal}")

# If not equal, you can identify specific differences
if not are_dataframes_equal:
    differences = parquet_df.compare(csv_df)
    print("Differences:")
    print(differences)

