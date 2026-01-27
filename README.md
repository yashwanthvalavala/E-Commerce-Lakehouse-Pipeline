E-Commerce Product Intelligence using Machine Learning & LLMs

This project focuses on building a machine-learning–ready e-commerce intelligence system on large, real-world datasets. The goal was to transform raw, messy commerce data into structured features and insights that support analytics, predictive modeling, and natural-language querying — similar to problems solved in large-scale digital commerce platforms.

Instead of a notebook-only workflow, the system was designed end-to-end to support data preprocessing, feature generation, and downstream ML/AI use cases.

Key Features
Bronze Layer → Raw ingestion (Autoloader)
Silver Layer → Clean & transform with Delta\
Gold Layer → SCD Type-2 Dimensions + Fact Sales\
Unified fact_sales table for analytics\
Airflow DAG → Orchestrates Bronze → Silver → Gold ETL\
Snowflake → Cloud data warehouse for analytics & storage\
Groq LLM Layer → Natural language → SQL → Visualization\
Interactive Dashboard → Streamlit + Plotly with drilldowns


Tech Stack
Data Lakehouse: Delta Lake, Apache Spark\
Orchestration: Apache Airflow\
Storage/Warehouse: Snowflake\
AI/LLM: Groq API (llama-3.3-70b-versatile)\
Visualization: Streamlit + Plotly\
Programming: Python, SQL, Jupyter Notebooks


📸 Architecture


![E-Commerce Flow](https://github.com/yashwanthvalavala/E-Commerce-Lakehouse-Pipeline/blob/main/architecture%20and%20demo/architecture.png)

📸 Demo
![E-Commerce Flow](https://github.com/yashwanthvalavala/E-Commerce-Lakehouse-Pipeline/blob/main/architecture%20and%20demo/img1.jpg)
![E-Commerce Flow](https://github.com/yashwanthvalavala/E-Commerce-Lakehouse-Pipeline/blob/main/architecture%20and%20demo/img2.jpg)






👨‍💻 Author
Yashwanth Valavala
