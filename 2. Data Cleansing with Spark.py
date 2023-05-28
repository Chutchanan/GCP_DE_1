# cleansing data with Spark
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

!apt-get update                                                                          # อัพเดท Package ทั้งหมดใน VM ตัวนี้
!apt-get install openjdk-8-jdk-headless -qq > /dev/null                                  # ติดตั้ง Java Development Kit (จำเป็นสำหรับการติดตั้ง Spark)
!wget -q https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop2.7.tgz # ติดตั้ง Spark 3.1.2
!tar xzvf spark-3.1.2-bin-hadoop2.7.tgz                                                  # Unzip ไฟล์ Spark 3.1.2
!pip install -q findspark==1.3.0                                                         # ติดตั้ง Package Python สำหรับเชื่อมต่อกับ Spark

# Set enviroment variable
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.1.2-bin-hadoop2.7"

# install PySpark
!pip install pyspark==3.1.2

# create Spark Session
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

# Download Data File
!wget -O data.zip https://file.designil.com/zdOfUE+
!unzip data.zip
dt = spark.read.csv('/content/ws2_data.csv', header = True, inferSchema = True)

# check data schema and statistics
dt.printSchema()
dt.summary().show()

# check missing column
dt.summary("count").show()
dt.where(dt.user_id.isNull()).show()

# convert Spark Dataframe to Pandas Dataframe
dt_pd = dt.toPandas()

# data overview
dt_pd.head()
sns.boxplot(x = dt_pd['book_id'])
sns.histplot(dt_pd['price'], bins=10)

# convert data type
from pyspark.sql import functions as f
dt_clean = dt.withColumn("timestamp",
                        f.to_timestamp(dt.timestamp, 'yyyy-MM-dd HH:mm:ss')
                        )

# identify Syntactical Anomalies
dt_clean.select("country").distinct().count()
dt_clean.select("Country").distinct().sort("Country").show(58, False)
dt_clean.where(dt_clean['Country'] == 'Japane').show()

from pyspark.sql.functions import when
dt_clean_country = dt_clean.withColumn("CountryUpdate", when(dt_clean['Country'] == 'Japane', 'Japan').otherwise(dt_clean['Country']))
dt_clean_country.select("countryupdate").distinct().sort("country").show(58, False)
dt_clean_country.show()
dt_clean = dt_clean_country.drop("Country").withColumnRenamed('CountryUpdate', 'Country')
dt_clean.show()

# identify Semantic Anomalies
dt_clean.select("user_id").show(10)
dt_clean.select("user_id").count()
dt_clean.where(dt_clean["user_id"].rlike("^[a-z0-9]{8}$")).count()

dt_correct_userid = dt_clean.filter(dt_clean["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean.subtract(dt_correct_userid)
dt_clean_userid = dt_clean.withColumn("user_id_update", when(dt_clean['user_id'] == 'ca86d17200', 'ca86d172').otherwise(dt_clean['user_id']))
dt_correct_userid = dt_clean_userid.filter(dt_clean_userid["user_id"].rlike("^[a-z0-9]{8}$"))
dt_incorrect_userid = dt_clean_userid.subtract(dt_correct_userid)
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('user_id_update', 'user_id')
dt_clean.show()

# identify missing values
from pyspark.sql.functions import col, sum

dt_nulllist = dt_clean.select([ sum(col(colname).isNull().cast("int")).alias(colname) for colname in dt_clean.columns ])
dt_nulllist.show()
dt_clean.where(dt_clean.user_id.isNull()).show()
dt_clean_userid = dt_clean.withColumn("user_id_update", when(dt_clean['user_id'].isNull(), '00000000').otherwise(dt_clean['user_id']))
dt_clean = dt_clean_userid.drop("user_id").withColumnRenamed('user_id_update', 'user_id')

# identify outliers
dt_clean_pd = dt_clean.toPandas()
sns.boxplot(x = dt_clean_pd['price'])
dt_clean.where( dt_clean.price > 80 ).select("book_id").distinct().show()

# save to csv
dt_clean.coalesce(1).write.csv('Cleaned_Data_Single.csv', header = True)