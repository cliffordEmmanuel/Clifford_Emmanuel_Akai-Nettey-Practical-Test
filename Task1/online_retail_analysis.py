import pandas as pd
import re
import seaborn as sns
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import ProgrammingError
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import warnings

from ucimlrepo import fetch_ucirepo 

from query import queries as query

# Suppress pandas warning messages
warnings.simplefilter(action='ignore')


def create_database(db_name:str) -> None:
    """Utility function to create database given a specific name

    Args:
        db_name (Str): Name to give to the database.
        
    Raises:
        ProgrammingError: Raises when database creation fails
    """

    # Validate the database name to prevent SQL injection
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', db_name):
        raise ValueError("Invalid database name")

    engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres", isolation_level='AUTOCOMMIT')
    
    # PostgreSQL DDL statement doesn't support parameter binding so doing a controlled string interpolation 
    create_db_query = f"CREATE DATABASE {db_name};"

    with engine.connect() as connection:
        try:
            connection.execute(text(create_db_query))
            print(f"Database '{db_name}' created successfully.")
        except ProgrammingError as e:
            print(f"Error creating database: {e}")


def setup_db(db_name: str) -> Engine:
    """Sets up the user database and returns a connection to it

    Args:
        db_name (str): Name to give to the user database.

    Returns:
        Engine: an SQLAlchemy Engine object
    """

    print("Setting up database")
    # create a connection engine to the default postgres db using the defalt postgres role.
    engine = create_engine("postgresql://postgres:postgres@localhost:5432/postgres", isolation_level='AUTOCOMMIT')
    
    with engine.connect() as connection:
        # Check if db exists
        result = connection.execute(text(query['check_db']), {"db_name": db_name}).fetchone()

        # If the database does not exist, create it
        if result is None:
            create_database(db_name)
        else:
            print(f"Database '{db_name}' already exists.")
    
    return create_engine(f"postgresql://postgres:postgres@localhost:5432/{db_name}")

def fetch_dataset() -> pd.DataFrame:
    """Fetches retail data from UCI online repository

    Returns:
        pd.DataFrame: Dataframe containing retail data.
    """

    print("Fetching dataset from UCI online repository...")
    online_retail = fetch_ucirepo(id=352) 
    data = online_retail['data']['original']

    print(f"Dataframe shape: {data.shape}")

    # observing some nulls
    print("##########################")
    print("Checking for nulls")
    for index, entry in enumerate(data.isna().sum()):
        print(f"Column: {data.columns[index]}, null count: {entry}")
    
    data.dropna(subset=['CustomerID'], inplace=True)
    print(f"Dataframe shape after missing values drop: {data.shape}")

    data['InvoiceDate'] = pd.to_datetime(data['InvoiceDate'])
    
    data.columns = data.columns.str.lower() # postgresql is case sensitive

    return data



def ingest_to_table(data: pd.DataFrame, table_name: str, engine: Engine) -> None:
    """Dumps data from a dataframe to database table

    Args:
        data_path (str): Path to excel file
        table_name (str): Table name in which to store data.
        engine (Engine): An SQLAlchemy Engine object
    """
    
    data.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    

def run_query(query: str, engine: Engine) -> pd.DataFrame:
    """Executes a query and returns the results as a pandas dataframe.

    Args:
        query (str): Query to be executed
        engine (Engine): An SQLAlchemy Engine object

    Returns:
        pd.DataFrame: Dataframe containing the query results
    """

    print("Running query...")
    with engine.connect() as connection:
        # Execute the query and fetch results into a DataFrame
        df = pd.read_sql_query(text(query), connection)

    return df
    

def generate_visualization_top_10_customers(data, save_path):
    """Creates a bar plot of total purchase amounts by customer ID,
    sorted from highest to lowest purchase amount,

    Args:
        data (pd.Dataframe): Contains customers and their corresponding purchase amounts
        save_path (String): Defines where image should be stored
    """

    print("Generating visualization")
    sns.set_style("darkgrid")
    plt.figure(figsize=(10,6))

    ax = sns.barplot(x=data.index, y='totalpurchaseamount', data=data)

    # Customizing the plot
    plt.title('Top 10 customers by total purchase amount', fontsize=14)
    plt.xlabel('Customer', fontsize=10)
    plt.ylabel('Total Purchase Amount (£)', fontsize=10)

    # Set x-axis labels to customer IDs
    ax.set_xticklabels(data['customerid'])
    plt.xticks(rotation=45)

    # Add value labels on top of each bar
    for i, v in enumerate(data['totalpurchaseamount']):
        plt.text(i, v, f'£{v:,.2f}', ha='center', va='bottom')

    # Adjust layout and display the plot
    plt.tight_layout()
    plt.savefig(save_path)

    print(f"Visualization saved: {save_path}")
    

def generate_visualization_popular_products(data, save_path):
    """Creates a horizontal bar plot of number of orders for each product.

    Args:
        data (DataFrame): Contains products and their corresponding number of orders
        save_path (String): Defines where image should be stored
    """
    
    print("Generating visualization")
    sns.set_style("darkgrid")
    plt.figure(figsize=(10,6))

    ax = sns.barplot(x='numberoforders', y='description', data=data, orient='h')

    # Customizing the plot
    plt.title('Most popular products ordered(showing top 11)', fontsize=14, loc='left')
    plt.xlabel('Number of Orders', fontsize=10)
    plt.ylabel('Products', fontsize=10)

    # Add value labels on top of each bar
    for i, v in enumerate(data['numberoforders']):
        ax.text(v, i, f'{v}', ha='left', va='center')

    # Adjust layout 
    plt.tight_layout()
    plt.savefig(save_path)
    print(f"Visualization saved: {save_path}")


def generate_visualization_monthly_revenue(data, save_path):
    """Creates a line plot of monthly revenue

    Args:
        data (DataFrame): Contains total revenue for observed months
        save_path (String): Defines where image should be stored
    """

    print("Generating visualization")
    sns.set_style("darkgrid")

    plt.figure(figsize=(10,6))

    ax = sns.lineplot(x='month', y='totalrevenue', data=data, marker='o')

    # Customizing the plot
    plt.title('Monthly Revenue over time', fontsize=14)
    plt.xlabel('Month', fontsize=10)
    plt.ylabel('Total Revenue (Millions)', fontsize=10)
    plt.xticks(rotation=45)

    # show monthly output in millions
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: f'£{x/1e6:.1f}M'))

    for i, row in data.iterrows():
        ax.text(row['month'], row['totalrevenue'], f'£{row["totalrevenue"]/1e6:.1f}M', 
                ha='left', va='bottom', rotation=0, fontsize=8)

    # Adjust layout and save the plot
    plt.tight_layout()

    plt.savefig(save_path)

    print(f"Visualization saved: {save_path}")

if __name__ == "__main__":
    
    DB_NAME="wb_test"
    TABLE_NAME="online_retail"

    images_path = "Task1/images"

    engine = setup_db(DB_NAME)

    online_retail_data = fetch_dataset()

    print('\n')
    print("Ingesting data to database")
    ingest_to_table(online_retail_data,TABLE_NAME, engine)
    
    print("#"*30)
    print("Analyzing top 10 customers...")
    top_10_customers = run_query(query['top_10'], engine)
    # ids coming as floats
    top_10_customers.customerid=top_10_customers.customerid.astype('int')
    generate_visualization_top_10_customers(data=top_10_customers,save_path=f'{images_path}/top_10_customers_by_purchase_amount.png')
    print('\n')

    print("#"*30)
    print("Analyzing popular products...")
    popular_products = run_query(query['popular_products'], engine)
    # filtering for just the products with more than 1000 orders.
    top_11_products = popular_products[popular_products['numberoforders'] >= 1000]
    generate_visualization_popular_products(data=top_11_products, save_path=f'{images_path}/popular_products.png')
    print('\n')

    print("#"*30)
    print("Analyzing monthly revenue...")
    monthly_revenue = run_query(query['monthly_revenue'], engine)
    generate_visualization_monthly_revenue(data=monthly_revenue, save_path=f'{images_path}/monthly_revenue.png')
    print('\n')
