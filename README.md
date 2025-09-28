Bronze Layer Autoloader

This project implements the Bronze Layer of a Data Lakehouse pipeline for an E-Commerce Data Platform with AI-Powered Self-Healing Pipelines.
The script (autoloader.py) leverages Apache Spark Structured Streaming with Delta Lake to continuously ingest raw data from CSV files into a bronze layer, applying schema validation, timestamping, and minimal transformations.



🛠️ Requirements

Apache Spark 3.5+\
Delta Lake (io.delta:delta-spark_2.12:3.1.0)\
Python 3.8+\
Java JDK 8 or 11



📊 Schemas Used

Customers\
customer_id, first_name, last_name, email, phone, address, city, country, signup_date, last_updated

Products\
product_id, name, category, price, stock_quantity, created_at, last_updated

Orders\
order_id, transaction_id, customer_id, product_id, quantity, total_amount, order_date, payment_method, order_status, last_updated

Deliveries\
delivery_id, order_id, transaction_id, customer_id, customer_name, product_id, product_name, total_amount, payment_method, delivery_address, delivery_city, delivery_country, delivery_date, delivery_status, last_updated
