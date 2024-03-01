import pyarrow.parquet as pq
import pyarrow as pa

columns_to_change={'PUlocationID':pa.int64(),'DOlocationID':pa.int64(),'SR_Flag':pa.string()}

for i in range (1,13):
    file_path=f'fhv_tripdata_2019-{i:02d}.parquet'
    table=pq.read_table(file_path)
    
    new_fields=[]
    for field in table.schema:
        if field.name in columns_to_change:
            new_fields.append(pa.field(field.name,columns_to_change[field.name]))
        else:
            new_fields.append(field)
    
    new_schema=pa.schema(new_fields)
    new_table=table.cast(new_schema)

    new_file_path=f'new_fhv_tripdata_2019-{i:02d}.parquet'
    pq.write_table(new_table,new_file_path)

    print("*" * 50)
    print(f'{file_path} data types have been changed to {new_file_path}')
    print("*" * 50)
    print('\n')