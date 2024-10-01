# Lufthansa project (Airton Muskollari)

## Building an ETL pipeline

### Overview

This project focuses on building an ETL pipeline for a dataset containing over 100,000 orders from various marketplaces in Brazil.
The dataset includes information about orders, products, customers, sellers, deliveries, and order reviews.
The aim is to clean and transform the data, focusing on orders that were successfully delivered.

### 1. Data Cleaning and Transformation

#### a) Cleaning the Orders Table
Filtered the Orders table to only include orders where order_status = 'delivered'.
Dropped related data from all other tables where the order_id was not associated with a delivered order.

#### b) Handling Missing Data
Orders Table:
Removed 8 rows with missing order_delivered_customer_date, as their absence did not significantly affect the analysis.

Order Reviews Table:
Replaced null values in the review_comment_title and review_comment_message fields with "No Title" and "No Comment", respectively.

Products Table:
Dropped rows with null values in product_name_length and product_description_length, since these fields were not critical.
For missing product_category_name, replaced null values with "Other".
Set missing product_photos_qty values to 0.

#### c) Saving Cleaned Data

Saved the cleaned datasets to a separate location for further transformation and analysis in subsequent notebooks.

### 2. Creating Calculated Columns

#### a) Total Price: Sum of product price and freight value.
To calculate the total price, sum the product price and freight value, and store the result in a new column called total_price in the order_items table.

#### b) Delivery Time: Difference between the delivery date and the order purchase date.
To calculate the delivery time, subtract order_purchase_timestamp from order_delivered_customer_date. Make sure both columns are converted to datetime format before the calculation. The result, delivery_time, is measured in days.

#### c) Payment Count: Sum of payment installments for each order.
To calculate the payment count, sum all payment installments for each order from the order_payments table and group by order_id. Create a new column called total_payments_count. Once created, merge this column into the order_items table.

#### d) Profit Margin: Subtract freight value from product price to calculate a rough profit estimate.
To calculate the profit margin, subtract the freight value from the product price. Store the result in a new column called profit_margin in the order_items table.

### 3. Using Window Functions Over Partitions (Pandas)

#### a) Total Sales per Customer: A running total of product price for each customer partitioned by Customer ID.
To calculate Total Sales per Customer, first merge the order_items table with orders and customers to obtain customer_id and customer_unique_id. Then, group the data by customer_unique_id and sum the total_price for each customer to get the total sales.

#### b) Average Delivery Time per Product Category: A rolling average of delivery time partitioned by product category.
To calculate the Average Delivery Time per Product Category, first join the order_items table with the products table to obtain product_category_name. Then, sort the data by product_category_name and order_purchase_timestamp. Use the rolling function with a window of 3 rows to calculate the rolling average of delivery_time for each product category.

### 4. Saving Processed Data to SQL Server

After selecting the appropriate columns for the fact and dimension tables, I established a connection to SQL Server using SQLAlchemy. The connection details are stored in the db_utils.py file, which handles the database connection properties.

Using the established connection, I used the to_sql function to store the DataFrames in the SQL Server.

