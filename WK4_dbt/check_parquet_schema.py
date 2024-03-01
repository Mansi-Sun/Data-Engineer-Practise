import pyarrow.parquet as pq

for i in range (1,13):
    file_path=f'fhv_tripdata_2019-{i:02d}.parquet'
    table=pq.read_table(file_path)
    file_metadata=pq.read_metadata(file_path)
    rows=file_metadata.num_rows
    print('\n')
    print("*" * 50)
    print(f"This is the schema for {file_path}")
    print(table.schema)
    print(f"There are {rows} rows in this file.")
    print("*" * 50)
    print('\n')
