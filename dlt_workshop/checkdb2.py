import duckdb

conn=duckdb.connect(database='dlt_exercise.duckdb')
conn.sql("SET search_path = 'generators'")
print(conn.sql("show tables"))

print("\n\n\n personcount2 table below:")
print(conn.sql("select * from personcount2"))
print("\n\n\n sum of age is:")
sumage=conn.sql("select sum(age) from personcount2")
print(sumage)

