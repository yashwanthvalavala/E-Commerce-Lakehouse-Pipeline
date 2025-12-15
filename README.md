E-Commerce Data Lakehouse 
I built this project to understand how production-grade ETL pipelines are designed beyond notebooks — especially around incremental loads, data quality, and orchestration failures. I intentionally designed Bronze–Silver–Gold layers to mirror real lakehouse patterns used in industry rather than a demo-style pipeline.

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
