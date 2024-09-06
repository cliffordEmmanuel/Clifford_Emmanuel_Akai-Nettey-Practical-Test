from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum,to_timestamp, count,date_trunc, round,desc
from pyspark.sql.types import DecimalType

# Create a Spark session
spark = SparkSession.builder \
    .appName("EcommerceDataAnalysis") \
    .config("spark.jars", "jars/postgresql-42.5.6.jar") \
    .getOrCreate()

# Set the legacy time parser policy
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


# load the dataset into a spark dataframe
data = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/data.csv")


# inspect the schema
data.printSchema()

# Performing some data cleaning

# For each column, count the number of null values
missing_values_df = data.select([count(when(col(c).isNull(), c)).alias(c).alias(c) for c in data.columns])
missing_values_df.show()

# Drop rows where CustomerID is null
data = data.filter(data.CustomerID.isNotNull())
print(f"Total rows after dropping null CustomerID: {data.count()}")

# Convert 'InvoiceDate' to a date type
data = data.withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"), "M/d/yyyy HH:mm"))

data.printSchema()
data.show(5)


# Calculate daily sales and transactions
daily_summary = data.withColumn("Date", date_trunc("day", "InvoiceDate")) \
    .groupBy("Date") \
    .agg(
        round(sum(col("Quantity") * col("UnitPrice")), 2).alias("TotalSales"),
        count("InvoiceNo").alias("NumberOfTransactions")
    ) \
    .orderBy("Date")

print("\nSample of Daily sales and transaction stats:")
daily_summary.show(10, truncate=False)


# Calculate total sales per product
product_sales = data.groupBy("StockCode", "Description") \
    .agg(
        round(sum(col("Quantity") * col("UnitPrice")), 2).alias("TotalSales"),
        sum("Quantity").alias("TotalQuantity")
    ) \
    .orderBy(desc("TotalSales")) \
    .limit(10)

# Show the results
print("\nTop 10 Products with Highest Sales:")
product_sales.show(truncate=False)

# TODO: hide connection details inside an environment variable
# Database connection properties
db_properties = {
    "driver": "org.postgresql.Driver",  
    "url": "jdbc:postgresql://localhost:5432/wb_test", 
    "user": "postgres",
    "password": "postgres"
}

# Write daily summary to database
daily_summary.write \
    .jdbc(url=db_properties["url"],
          table="daily_sales_summary",
          mode="overwrite",
          properties=db_properties)

print("Daily sales summary has been written to the database.")

# Write top 10 products to database
product_sales.write \
    .jdbc(url=db_properties["url"],
          table="top_10_products",
          mode="overwrite",
          properties=db_properties)

print("Top 10 products have been written to the database.")

spark.stop()