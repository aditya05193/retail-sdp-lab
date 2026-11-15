from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="sdp_gold_daily_sales",
    comment="Daily sales from current completed, non-deleted orders.",
    table_properties={"quality": "gold"},
)
def gold_daily_sales():
    return (
        spark.read
        .table("sdp_silver_orders")
        .where(
            (F.col("is_deleted") == F.lit(False))
            & (F.col("status") == "completed")
        )
        .groupBy(
            F.to_date("event_ts").alias("order_date")
        )
        .agg(
            F.count(F.lit(1)).alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum(
                F.col("quantity") * F.col("unit_price")
            ).alias("sales_amount"),
        )
    )


@dp.materialized_view(
    name="sdp_gold_product_sales",
    comment="Product sales from current completed, non-deleted orders.",
    table_properties={"quality": "gold"},
)
def gold_product_sales():
    return (
        spark.read
        .table("sdp_silver_orders")
        .where(
            (F.col("is_deleted") == F.lit(False))
            & (F.col("status") == "completed")
        )
        .groupBy("product_id")
        .agg(
            F.count(F.lit(1)).alias("order_count"),
            F.sum("quantity").alias("units_sold"),
            F.sum(
                F.col("quantity") * F.col("unit_price")
            ).alias("sales_amount"),
        )
    )

# We use @dp.materialized_view because Gold is an aggregation for reporting, not an append-only event stream.

# The daily table groups by date, while the product table groups by product. Databricks manages refreshing these views when Silver changes.