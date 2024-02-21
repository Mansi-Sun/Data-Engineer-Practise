import dlt

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

pipeline=dlt.pipeline(destination='duckdb',dataset_name='persons')

info=pipeline.run(
    people_2(),
    table_name='sum_age',
    write_disposition='merge',
    primary_key='ID'
)

print(info)