
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# We intentionally define unit_price, event_ts, and updated_at as strings here. 
# Bronze preserves source values; validation and type conversion belong in the Silver preparation layer.

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", StringType()),
    StructField("status", StringType()),
    StructField("event_ts", StringType()),
    StructField("updated_at", StringType()),
    StructField("operation", StringType()),
])