# Databricks notebook source
# MAGIC %md
# MAGIC ### Higher Order Functions
# MAGIC In PySpark, higher order functions refer to functions that take other functions as arguments, or return functions as values. These functions can be used to simplify data processing tasks and enable more complex data transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ###map()
# MAGIC 
# MAGIC - PySpark map (map()) is an RDD transformation that is used to apply the transformation function (lambda) on every element of RDD/DataFrame and returns a new RDD.
# MAGIC 
# MAGIC **Note** : DataFrame doesn’t have map() transformation to use with DataFrame hence you need to convert DataFrame to RDD first.

# COMMAND ----------

#importing DataTypes and SQL fucntions 
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType,BooleanType,DoubleType,ArrayType
from pyspark.sql.functions import *
from functools import reduce

# COMMAND ----------

# create a sample dataframe
df1 = spark.createDataFrame([(1, "apArna S"), (2, "aiswarya satheesh"), (3, "athulya Dev")], ["id", "value"])

# convert the dataframe to an RDD
rdd1 = df1.rdd

# COMMAND ----------

# define a function to apply to each row in the RDD
def proper_case_row(row):
    id = row[0]
    value = row[1]
    new_value = value.title()
    return (id, new_value)

# COMMAND ----------

# use the map() function to apply the transform_row() function to each row in the RDD
new_rdd = rdd1.map(proper_case_row)

# COMMAND ----------

# convert the transformed RDD back to a dataframe
new_df = spark.createDataFrame(new_rdd, ["id", "new_value"])

# COMMAND ----------

# join the original dataframe with the transformed dataframe on the "id" column
result_df = df1.join(new_df, on="id")

# show the result dataframe
result_df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###flatMap()
# MAGIC 
# MAGIC - flatMap() is a transformation operation in PySpark that is used to flatten an RDD of lists or tuples. It maps each input element to zero or more output elements and flattens the results into a single RDD.
# MAGIC - flatMap() can be used for a variety of tasks, such as splitting a text file into words, expanding lists of values, and more.

# COMMAND ----------

# create a sample RDD with nested lists
rdd = sc.parallelize([['#analytics', '#deeplearning', '#artificialintelligence', '#python', '#ai'], 
 ['#analytics', '#datascience', '#deeplearning', '#visualization', '#dataanalysis'],
 ['#datavisualization', '#machinelearning', '#dataanalysis', '#cloudcomputing', '#neuralnetworks'],
 ['#python', '#datascience', '#dataengineering', '#nlp', '#algorithms'],
 ['#statistics', '#machinelearning', '#deeplearning', '#ai', '#bigdata']])



# COMMAND ----------

# use flatMap to convert the nested list to a flat list of hashtags
flat_rdd = rdd.flatMap(lambda x: x)

# use map to create a key-value pair of (hashtag, 1) for each hashtag
hashtag_counts = flat_rdd.map(lambda x: (x, 1))

# use reduceByKey to count the number of occurrences of each hashtag
hashtag_occurrences = hashtag_counts.reduceByKey(lambda x, y: x + y)

# convert the RDD to a DataFrame with column names
df = hashtag_occurrences.toDF(["hashtag", "count"])

# show the DataFrame
df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ###reduce()
# MAGIC - reduce() is an action operation in PySpark that aggregates the elements of an RDD using a specified function. It takes a function that accepts two arguments and returns a single value, and applies that function to the elements of the RDD in a cumulative way, reducing the entire RDD down to a single value.
# MAGIC - The key advantage of reduce() is that it allows you to perform complex aggregations on large datasets in a parallel and distributed manner, making it an essential tool for big data processing.

# COMMAND ----------

# Create an RDD containing network packet data
packet_data = sc.parallelize([
    {"src_ip": "192.168.1.1", "dest_ip": "8.8.8.8", "bytes_sent": 1000, "bytes_received": 500},
    {"src_ip": "192.168.1.2", "dest_ip": "8.8.8.8", "bytes_sent": 1500, "bytes_received": 1000},
    {"src_ip": "192.168.1.3", "dest_ip": "8.8.8.8", "bytes_sent": 2000, "bytes_received": 750},
    {"src_ip": "192.168.1.1", "dest_ip": "8.8.8.8", "bytes_sent": 500, "bytes_received": 200},
    {"src_ip": "192.168.1.2", "dest_ip": "8.8.8.8", "bytes_sent": 1000, "bytes_received": 500},
    {"src_ip": "192.168.1.3", "dest_ip": "8.8.8.8", "bytes_sent": 500, "bytes_received": 250},
])

# Define a function to aggregate the bytes sent and received by each IP address
def aggregate_bytes_by_ip(x, y):
    return {"src_ip": x["src_ip"], 
            "bytes_sent": x["bytes_sent"] + y["bytes_sent"], 
            "bytes_received": x["bytes_received"] + y["bytes_received"]}


# COMMAND ----------

ip_byte_counts_rdd = packet_data.map(lambda x: (x["src_ip"], x)).reduceByKey(aggregate_bytes_by_ip)
#.map(x[src],x) basically converts the rdd into a tuple, such that the ipaddr is the key and the value has all the attr 
#('192.168.1.1', {'src_ip': '192.168.1.1',   'dest_ip': '8.8.8.8', 'bytes_sent': 1000, 'bytes_received': 500})
ip_byte_counts_rdd.collect()

# COMMAND ----------

# Convert the result to a DataFrame
ip_byte_counts_df = ip_byte_counts_rdd.map(lambda x: (x[0], x[1]["bytes_sent"], x[1]["bytes_received"])) \
                                      .toDF(["src_ip", "bytes_sent", "bytes_received"])
# Show the result as a DataFrame
ip_byte_counts_df.show()


# COMMAND ----------

# Create a dataframe of employee names and ages
df_names_ages = spark.createDataFrame([
    ("Alice", 25),
    ("Bob", 30),
    ("Charlie", 35),
    ("David", 40)
], ["name", "age"])

# Create a dataframe of employee addresses
df_addresses = spark.createDataFrame([
    ("Alice", "123 Main St"),
    ("Bob", "456 Oak Ave"),
    ("Charlie", "789 Elm St"),
    ("David", "1011 Maple Ave")
], ["name", "address"])

# Create a dataframe of employee salaries
df_salaries = spark.createDataFrame([
    ("Alice", 50000),
    ("Bob", 60000),
    ("Charlie", 70000),
    ("David", 80000)
], ["name", "salary"])

# Create a dataframe of employee departments
df_departments = spark.createDataFrame([
    ("Alice", "Sales"),
    ("Bob", "Marketing"),
    ("Charlie", "Engineering"),
    ("David", "Finance")
], ["name", "department"])

# Show the dataframes
df_names_ages.show()
df_addresses.show()
df_salaries.show()
df_departments.show()


# COMMAND ----------

# define a function to join two data frames
def join_two(df1, df2,):
    return df1.join(df2, "name")


# join all four data frames using reduce
dfs = [df_names_ages,df_addresses,df_salaries,df_departments]
joined_df = reduce(join_two, dfs)

# show the result
joined_df.show()

# COMMAND ----------

df_1=spark.read.format('csv').option("header","true").load("dbfs:/FileStore/Jan_1_15.csv")
df_2=spark.read.format('csv').option("header","true").load("dbfs:/FileStore/Jan_16_31.csv")
df_3=spark.read.format('csv').option("header","true").load("dbfs:/FileStore/Feb_1_15.csv")
df_4=spark.read.format('csv').option("header","true").load("dbfs:/FileStore/Feb_16_28.csv")
df_1.show(1)
df_2.show(1)
df_3.show(1)
df_4.show(1)

# COMMAND ----------

dfs = [df_1, df_2, df_3, df_4]
result = reduce(lambda left, right: left.union(right), dfs)

result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###filter()
# MAGIC 
# MAGIC - The filter() function in PySpark is a transformation that creates a new RDD by selecting elements from an existing RDD that satisfy a given condition.

# COMMAND ----------

appliance_rdd = sc.parallelize([("refrigerator", "In Warranty"), ("dishwasher", "Out of Warranty"),
                                ("microwave", "In Warranty"), ("oven", "Out of Warranty"),
                                ("washer", "Out of Warranty"), ("dryer", "In Warranty"),
                                ("range", "In Warranty"), ("cooktop", "Out of Warranty")])

# COMMAND ----------

out_of_warranty_rdd = appliance_rdd.filter(lambda x: x[1] == "Out of Warranty")
out_of_warranty_rdd.collect()

# COMMAND ----------

appliance_df= appliance_rdd.toDF(["Appliance", "Warranty"])
# Filter the DataFrame to only include "Out of Warranty" rows
out_of_warranty_df = appliance_df.filter(col("Warranty") == "Out of Warranty")
out_of_warranty_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###transform() 
# MAGIC - In PySpark, the transform() function is used to apply a user-defined function (UDF) to each element of an RDD or DataFrame, and returns a new RDD or DataFrame with the transformed values.
# MAGIC - The ***transform()*** function is similar to the map() function, but allows you to apply more complex transformations that require additional dependencies or external libraries. The function takes a UDF as an argument, which can be defined using either a Python lambda function or a standalone function.  

# COMMAND ----------


# Create a sample DataFrame with a sentence column
data = [("Subject: enron methanol ; meter : 988291 Subject",),
        ("Subject: ehronline web address change",),
        ("Subject: photoshop , windows , office . cheap . main trending",)]
df = spark.createDataFrame(data, ["sentence"])

# Define a function to count the number of words in a sentence
def word_count(sentence):
    return [len(sentence.split())]

df = df.transform(lambda df: df.select("*", size(split("sentence", " ")).alias("word_count")))

# Display the resulting DataFrame
df.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ###exists()
# MAGIC 
# MAGIC - In PySpark, ***exists()*** is a DataFrame function that checks if a specified column or a set of columns exists in the DataFrame. The function returns a boolean value, True if the specified column(s) exist in the DataFrame, and False otherwise.

# COMMAND ----------

schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("sensor_data", ArrayType(FloatType()), True)
])

# create a list of tuples with sensor id and sensor data
data = [(1, [1.0,1.4,4.5,-2.0,0.0,1.0]), (2, [3.8,2.0,1.9,2.0,0.0,-1.0]), (3, [0.4,1.9,0.0,8.0,3.0,-1.0])]

# create the DataFrame
df = spark.createDataFrame(data, schema)

# print the DataFrame
df.show()

# COMMAND ----------

df.select("*",(exists("sensor_data", lambda x: x >5).alias("any_negative"))).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###aggregate()
# MAGIC - The aggregate() function in PySpark is a transformation operation that applies an aggregation function to the elements of an RDD and returns a result.

# COMMAND ----------

df = df.withColumn('avg_sensor_value', aggregate('sensor_data', lit(0.0), lambda acc1, acc2: acc1 + acc2) / size('sensor_data'))

df.show()
