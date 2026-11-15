from pyspark.sql import functions as F


def add_typed_order_columns(dataframe):
    """Preserve raw values and add safely converted values for validation."""
    return (
        dataframe
        .withColumn("unit_price_raw", F.col("unit_price"))
        .withColumn(
            "unit_price",
            F.expr("try_cast(unit_price AS DECIMAL(12, 2))"),
        )
        .withColumn("event_ts_raw", F.col("event_ts"))
        .withColumn(
            "event_ts",
            F.expr("try_cast(event_ts AS TIMESTAMP)"),
        )
        .withColumn("updated_at_raw", F.col("updated_at"))
        .withColumn(
            "updated_at",
            F.expr("try_cast(updated_at AS TIMESTAMP)"),
        )
    )


def rejection_reason():
    """Return the first validation failure found for an order event."""
    return (
        F.when(
            F.col("_rescued_data").isNotNull(),
            "Unexpected fields or type mismatch",
        )
        .when(
            F.col("order_id").isNull() | (F.trim("order_id") == ""),
            "Missing order ID",
        )
        .when(
            F.col("customer_id").isNull() | (F.trim("customer_id") == ""),
            "Missing customer ID",
        )
        .when(
            F.col("product_id").isNull() | (F.trim("product_id") == ""),
            "Missing product ID",
        )
        .when(
            F.col("quantity").isNull() | (F.col("quantity") <= 0),
            "Invalid quantity",
        )
        .when(
            F.col("unit_price").isNull() | (F.col("unit_price") < 0),
            "Invalid unit price",
        )
        .when(
            F.col("event_ts").isNull() | F.col("updated_at").isNull(),
            "Invalid timestamp",
        )
        .when(
            F.col("status").isNull()
            | ~F.col("status").isin("completed", "pending", "cancelled"),
            "Invalid status",
        )
        .when(
            F.col("operation").isNull()
            | ~F.col("operation").isin("INSERT", "UPDATE", "DELETE"),
            "Unsupported operation",
        )
    )