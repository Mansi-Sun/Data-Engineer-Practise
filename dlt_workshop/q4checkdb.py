import duckdb

conn=duckdb.connect(database='dlt_q4step1.duckdb')
conn.sql("SET search_path='persons'")
print(conn.sql("show tables"))

print("\n\n\n")
print(conn.sql("select count(*) from sum_age"))

print("\n\n\n")
print(conn.sql("select * from sum_age"))
print(conn.sql("select sum(age) from sum_age"))
