# Collect data from MySQL and REST API
! pip install pymysql

import os
import pymysql
import pandas as pd
import requests

class Config:
  MYSQL_HOST = 'xxx'
  MYSQL_PORT = 3306              # default for MySQL
  MYSQL_USER = 'xxx'
  MYSQL_PASSWORD = 'xxx'
  MYSQL_DB = 'xxx'
  MYSQL_CHARSET = 'utf8mb4'

# Connect to the database
connection = pymysql.connect(host=Config.MYSQL_HOST,
                             port=Config.MYSQL_PORT,
                             user=Config.MYSQL_USER,
                             password=Config.MYSQL_PASSWORD,
                             db=Config.MYSQL_DB,
                             charset=Config.MYSQL_CHARSET,
                             cursorclass=pymysql.cursors.DictCursor)

# list all tables by SQL with command show tables;
cursor = connection.cursor()
cursor.execute("show tables;")
tables = cursor.fetchall()
cursor.close()
print(tables)

# use with statement instead of cursor.close() (no need to close cursor)
with connection.cursor() as cursor:
  # query ข้อมูลจาก table audible_data
  cursor.execute("SELECT * FROM audible_data;")
  result = cursor.fetchall()

audible_data = audible_data.set_index("Book_ID")

# query another table by read_sql() from pandas
sql = "SELECT * FROM audible_transaction"
audible_transaction = pd.read_sql(sql, connection)

# merge 2 dataframes
transaction = audible_transaction.merge(audible_data, how="left", left_on="book_id", right_on="Book_ID")

# Get data from REST API
url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"
r = requests.get(url)
result_conversion_rate = r.json()
conversion_rate = pd.DataFrame(result_conversion_rate)
conversion_rate = conversion_rate.reset_index().rename(columns={"index": "date"})

# create date column of transaction dataframe
transaction['date'] = transaction['timestamp']

# convert timestamp of 2 dataframe to date by dt.date
transaction['date'] = pd.to_datetime(transaction['date']).dt.date
conversion_rate['date'] = pd.to_datetime(conversion_rate['date']).dt.date

# merge 2 dataframes
final_df = transaction.merge(conversion_rate, how = "left", left_on="date", right_on="date")

# change price column from string to float
final_df["Price"] = final_df.apply(lambda x: x["Price"].replace("$",""), axis=1)
final_df["Price"] = final_df["Price"].astype(float)

# create column THBPrice and drop redundant column
final_df["THBPrice"] = final_df["Price"] * final_df["conversion_rate"]
final_df = final_df.drop("date", axis=1)

# save to csv
final_df.to_csv("output.csv", index=False) 