import sqlite3
import pandas as pd
import csv
conn = sqlite3.connect('S30 ETL Assignment.db')

def execute_query(query):
    #Establish SQL Database Connection and Execute the Query
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    return result

query = '''
SELECT 
    Sales.customer_id AS Customer,
    Customers.age AS Age,
    Items.item_name AS Item,
    SUM(Orders.quantity) AS Quantity
FROM 
    Sales
LEFT JOIN 
    Customers ON Sales.customer_id = Customers.customer_id
LEFT JOIN 
    Orders ON Sales.sales_id = Orders.sales_id
LEFT JOIN
	Items ON Orders.item_id = Items.item_id
WHERE 
    Customers.age BETWEEN 18 AND 35
    AND Orders.quantity IS NOT NULL
GROUP BY 
    Sales.customer_id,
    Customers.age,
    Items.item_name
LIMIT 5;'''


#Execute the query
query_result = execute_query(query)

#Initialize empty csv write file
init_csv_file ='output.csv'


def create_csv_file(output_file,result):
    with open(output_file, 'w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(['Customer', 'Age', 'Item', 'Quantity'])
        writer.writerows(result)
        return output_file

output_csv_file = create_csv_file(init_csv_file,query_result)

for row in query_result:
    print(row)


df_output = pd.read_csv(output_csv_file, sep=';')
df_expected_output = pd.read_csv('expected_output.csv', sep=';')

def compare_csv_file(output_file,expected_output_file):
    if output_file.equals(expected_output_file):
        print("Test case passed: Content of Queried Output File matched the contents of Expected File.")
    else:
        print("Test case failed: Content of Queried Output File do not matched the contents of Expected File.")

compare_csv_file(df_output,df_expected_output)



conn.close()
