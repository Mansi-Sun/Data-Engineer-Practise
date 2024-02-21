import dlt

def people_1():
    for i in range(1, 6):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 25 + i, "City": "City_A"}

def people_2():
    for i in range(3, 9):
        yield {"ID": i, "Name": f"Person_{i}", "Age": 30 + i, "City": "City_B", "Occupation": f"Job_{i}"}

generators_pipeline=dlt.pipeline(destination='duckdb',dataset_name='generators')
info=generators_pipeline.run(people_1(),
							 table_name='personcount2',
							 write_disposition='replace')

info=generators_pipeline.run(people_2(),
							 table_name='personcount2',
							 write_disposition='append')
print(info)