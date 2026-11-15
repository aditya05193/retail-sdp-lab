from pyspark import pipelines as dp

from retail_sdp.quality_rules import (
    add_typed_order_columns,
    rejection_reason,
)


@dp.table(
    name="sdp_validated_orders",
    comment="Typed order events with validation results for audit and monitoring.",
    table_properties={"quality": "validated"},
)

## It appears in the Pipeline UI and event logs, so you can see metrics such as:
 # order_event_satisfies_contract
 #   Passed: 11
 #   Failed: 5
## Choose a meaningful, unique name for each expectation.

@dp.expect(
    "order_event_satisfies_contract", # rule name
    "rejection_reason IS NULL",       # rule condition
)

def validated_orders():
    bronze_orders = spark.readStream.table("sdp_bronze_orders")

    return (
        add_typed_order_columns(bronze_orders)
        .withColumn("rejection_reason", rejection_reason())
    )