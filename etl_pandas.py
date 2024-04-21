import pandas as pd
import sqlite3

conn = sqlite3.connect('S30 ETL Assignment.db')

#Convert Tables into DataFrames
sales_df = pd.read_sql_query("SELECT * FROM Sales", conn)
customers_df = pd.read_sql_query("SELECT * FROM Customers", conn)
orders_df = pd.read_sql_query("SELECT * FROM Orders", conn)
items_df = pd.read_sql_query("SELECT * FROM Items", conn)

#Merge tables (left join) to retrieve required data into single dataset
merged_df = pd.merge(sales_df, customers_df, on='customer_id', how='left')
merged_df = pd.merge(merged_df, orders_df, on='sales_id', how='left')
merged_df = pd.merge(merged_df, items_df, on='item_id', how='left')

#Age Filter between 18 to 35
filtered_df = merged_df[(merged_df['age'] >= 18) & (merged_df['age'] <= 35)]

#Group by customer, item, and calculate total quantity
result_df = filtered_df.groupby(['customer_id', 'age', 'item_name']).agg({'quantity': 'sum'}).reset_index()

#Remove items with no purchase (NaN)
result_df = result_df[result_df['quantity'].notna()]

#Since we don't sell half of an item, we can convert it into integer.
result_df['quantity'] = result_df['quantity'].astype(int)
result_df = result_df.rename(columns={'customer_id': 'Customer', 'age': 'Age', 'item_name': 'Item', 'quantity': 'Quantity'})

#For Test Case purposes, I only saved 5 data to csv
result_df.head(5).to_csv('output.csv', index=False, sep=';')

#Comparing the Output file queried to expected output file
df_output = pd.read_csv('output.csv', sep=';')
df_expected_output = pd.read_csv('expected_output.csv', sep=';')

if df_output.equals(df_expected_output):
    print("Test case passed: Content of Queried Output File matched the contents of Expected Output File.")
else:
    print("Test case failed: Content of Queried Output File do not matched the contents of Expected Output File.")


conn.close()