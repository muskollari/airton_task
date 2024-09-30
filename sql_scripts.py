from sqlalchemy import text
from db_utils import get_engine

def execute_query(query):
    engine = get_engine()
    with engine.connect() as connection:
        result = connection.execute(text(query))
        for row in result:
            print(row)

query1 = """
    SELECT p.product_category_name, SUM(o.total_price) AS total_sales 
    FROM fact_order_items o 
    JOIN dim_products p ON o.product_id = p.product_id 
    GROUP BY p.product_category_name
"""

query2 = """
    SELECT seller_id, AVG(delivery_time) AS avg_delivery_time 
    FROM fact_order_items  
    GROUP BY seller_id
"""
query3 = """Select c.customer_state, COUNT(o.order_id) as total_orders from fact_order_items o JOIN dim_customers c ON o.customer_id = c.customer_id GROUP BY c.customer_state"""



execute_query(query1)
execute_query(query2)
execute_query(query3)
