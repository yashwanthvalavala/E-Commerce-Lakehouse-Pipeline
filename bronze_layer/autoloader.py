# autoloader.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, concat_ws

spark = (
    SparkSession.builder
    .appName("BronzeAutoloader")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.1.0"
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

schemas = {
    "customers": """
        customer_id STRING,
        first_name STRING,
        last_name STRING,
        email STRING,
        phone STRING,
        address STRING,
        city STRING,
        country STRING,
        signup_date DATE,
        last_updated TIMESTAMP
    """,
    "products": """
        product_id STRING,
        name STRING,
        category STRING,
        price DOUBLE,
        stock_quantity INT,
        created_at DATE,
        last_updated TIMESTAMP
    """,
    "orders": """
        order_id STRING,
        transaction_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        total_amount DOUBLE,
        order_date DATE,
        payment_method STRING,
        order_status STRING,
        last_updated TIMESTAMP
    """,
    "deliveries": """
        delivery_id STRING,
        order_id STRING,
        transaction_id STRING,
        customer_id STRING,
        customer_name STRING,
        product_id STRING,
        product_name STRING,
        total_amount DOUBLE,
        payment_method STRING,
        delivery_address STRING,
        delivery_city STRING,
        delivery_country STRING,
        delivery_date DATE,
        delivery_status STRING,
        last_updated TIMESTAMP
    """
}

base_path = r"C:\Users\User\Desktop\E-Commerce Data Lakaehouse with AI-Powered Self-Healing Pipelines"

for src, schema in schemas.items():
    raw_path = f"{base_path}\\raw_data\\{src}"
    bronze_data_path = f"{base_path}\\bronze_layer\\bronze_data\\{src}\\data"
    checkpoint_path = f"{base_path}\\bronze_layer\\bronze_data\\{src}\\checkpoint"

    # Read stream with schema
    data = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .schema(schema)
        .load(raw_path)
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    # Special transformation for customers to combine first_name + last_name
    if src == "customers":
        data = data.withColumn("name", concat_ws(" ", "first_name", "last_name"))

    # Write stream to Delta
    (
        data.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("path", bronze_data_path)
        .trigger(once=True)
        .start()
    )

# Wait for all streams to finish
spark.streams.awaitAnyTermination()




