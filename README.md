# Practical test for Data Analyst (GE) for Data lab-Req 28700


## Question 1: Data Analysis with SQL and Python

### Task: [Using the UCI Machine Learning Repository&Online Retail Dataset](https://archive.ics.uci.edu/dataset/352/online+retail)

1. Ingest the dataset into a SQL database.
2. Write complex SQL queries to:
    - Find the top 10 customers by total purchase amount.
    - Identify the most popular products based on the number of orders.
    - Calculate the monthly revenue for the dataset's time range.
3. Use Python to:
    - Connect to the SQL database and retrieve the results of the above
    queries.
    - Generate visualizations (e.g., bar charts, line graphs) for the analysis
    performed.
    - Save the visualizations as image files.

### Deliverables:
- SQL script to create and populate the database, and the queries for analysis.
- Python script for database connection, executing SQL queries, and generating
visualizations.
- Image files of the visualizations.
- Brief insights gained from the analysis.



## Question 2: Sales Data Transformation and Aggregation

Task: Transform and aggregate a large sales dataset [Retail Sales Data](https://www.kaggle.com/datasets/carrie1/ecommerce-data) and analyze sales trends using PySpark.

1. Load the dataset into a PySpark DataFrame.
2. Perform necessary data cleaning (e.g., handle missing values, correct data
types).
3. Calculate total sales and the number of transactions per day.
4. Identify the top 10 products with the highest sales.
5. Save the transformed data back to a SQL database.

### Deliverables:
- PySpark script used to load, transform, aggregation, and analysis
- Code to write/export the data back to a SQL database
- Brief insights gained from the analysis.


## Question 3: Complex SQL Queries for Data Exploration

### Task: Using the [Chinook Database](), perform the following SQL operations:

1. Load the database and create the necessary tables in an SQLite or PostgreSQL
database setup.
2. Write SQL queries to perform the following tasks:
- Find the top 5 most popular genres by total sales.
- Calculate the average invoice total by country.
- Identify the top 3 most valued customers based on the total sum of
invoices.
- Generate a report listing all employees who have sold over a specified
amount (provide examples for amounts 1000,1000,5000).
3. Export the results of each query to a CSV file.

### Deliverables:
- SQL script file (.sql)Output CSV files containing the query results
- Brief insights gained from the analysis
