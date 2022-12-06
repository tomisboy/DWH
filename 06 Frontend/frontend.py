import pandas as pd
import matplotlib.pyplot as plt
import psycopg2


con = psycopg2.connect("dbname='postgres' user='postgres' password='123456' host='localhost' port='5432'")
sql = "SELECT geschwindigkeit, staging.\"D_ort_land\".ort FROM staging.f_messung INNER JOIN staging.\"D_ort_land\" on CAST (staging.f_messung.ort as INT) = CAST( staging.\"D_ort_land\".d_ort_key as integer);"

pd.read_sql_query(sql, con).boxplot(by="ort")
plt.suptitle("")
plt.ylabel("Geschwindigkeit in km/h")
plt.xlabel("Ort")
plt.title("Geschwindigkeit nach Ort")
plt.savefig("Boxplot.png")
