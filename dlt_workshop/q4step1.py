import dlt

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

pipeline=dlt.pipeline(destination='duckdb',dataset_name='persons')

info=pipeline.run(
    people_1(),
    table_name='sum_age',
    write_disposition='replace'
)

print(info)