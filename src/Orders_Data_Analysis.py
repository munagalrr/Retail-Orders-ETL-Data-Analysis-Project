#!/usr/bin/env python
# coding: utf-8

# # ETL project using Retail Orders dataset 
# <li><p> <b>Extracted</b> data from Kaggle API</p></li>
# <li><p> Data cleaning and <b>Transformation</b> using Python(Pandas module)</p></li>
# <li><p> <b>Loaded</b> data to the SQL Server</p></li>
# <li><p> Perfomed data analysis using SQL Server</p></li>

# In[90]:


#Import libraries/Modules
#!pip install kaggle
import kaggle
get_ipython().system('kaggle datasets download ankitbansal06/retail-orders -f orders.csv;')


# In[55]:


#extract file from zipfile
import zipfile
zip_ref = zipfile.ZipFile('orders.csv.zip')
zip_ref.extractall() #extract file to dir
zip_ref.close() #close file


# In[85]:


# import pandas module/library and load data
import pandas as pd
df = pd.read_csv('orders.csv', na_values=['Not Available','unknown'])
df.head();


# In[86]:


# check for data types, data format, missing data, wrong data types, anomaly data
df.describe()
df.dtypes
df.info()
df['Ship Mode'].unique();


# In[87]:


#rename columns make them lower case and replace space with underscore
#df.rename(columns={'Order Id': 'order_id', 'Order Date': 'order_date'}) # one method and need to apply all coloumns manually
df.columns=df.columns.str.lower() # alternative and efficient method where all column names changes once.
df.columns = df.columns.str.replace(' ', '_') # And then replace the space between strings with underscore
df.head();


# In[88]:


#derive new columns discount, sale price and profit
df['discount'] = df['list_price']*df['discount_percent']*0.01
df['sale_price'] = df['list_price']- df['discount']
df['profit_per_item'] = df['sale_price'] - df['cost_price']
#df['total_order_value'] = df['sale_price']*df['quantity']
#df['order_total_profit'] = df['profit/item']*df['quantity']
df.head();


# In[66]:


#convert order date from object data type to date
df['order_date']=pd.to_datetime(df['order_date'], format="%Y-%m-%d")


# In[89]:


#drop cost price, list price and discount percent columns
df.drop(columns=['cost_price','list_price','discount_percent'], inplace=True)
df.head();


# In[79]:


#load the data intp sql server
import sqlalchemy as sal
from sqlalchemy import text
##using window authentication
#engine = sal.create_engine('mssql://Legion/master?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
#conn = engine.connect()
# Database credentials
#using sql server authentication connection
username = 'userName'
password = 'password'
server = 'serverName'  # e.g., 'localhost', 'SERVER\INSTANCE'
database = 'master'
driver = 'ODBC Driver 17 for SQL Server' # or another appropriate driver like 'SQL Server' or 'SQL Server Native Client 11.0'

# Construct the connection string
connection_string = (
    f'mssql+pyodbc://{username}:{password}@{server}/{database}?'
    f'driver={driver}'
)

# Create the SQLAlchemy engine
engine = sal.create_engine(connection_string)

# Example usage (optional)
try:
    with engine.connect() as connection:
        result = connection.execute(text("Test Query"))
        print("Connection successful:", result.scalar())
except Exception as e:
    print(f"Error connecting to the database: {e}")


# In[84]:


##Load data to the existing data table in SQL Server
#df.to_sql('df_orders', con=engine.connect(), index=False, if_exists='replace') # replace will create a table and add data to the database
# Create table in database is the most efficient way while pandas will create a table with highest possible data types eg. varchar(max), bigint.  and it will consume more memory space.
df.to_sql('df_orders', con=engine.connect(), index=False, if_exists='append') # append will add/append the data to the existing table in the database

