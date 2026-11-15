from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="sdp_quarantine_orders",
    comment="Rejected order events retained for investigation and remediation.",
    table_properties={"quality": "quarantine"},
)
def quarantine_orders():
    rejected_orders = (
        spark.readStream
        .table("sdp_validated_orders")
        .where(F.col("rejection_reason").isNotNull())
    )

    return rejected_orders.withColumn(
        "_quarantine_id",
        F.sha2(
            F.to_json(
                F.struct(
                    "order_id",
                    "customer_id",
                    "product_id",
                    "quantity",
                    "unit_price_raw",
                    "status",
                    "event_ts_raw",
                    "updated_at_raw",
                    "operation",
                    "_rescued_data",
                    "_source_file",
                )
            ),
            256,
        ),
    )


## This creates:
# sdp_validated_orders
#       ↓ invalid only
# sdp_quarantine_orders

#  .where(F.col("rejection_reason").isNotNull())
#  Selects only invalid records.


#  _quarantine_id : Creates a stable hash from the original event contents and source file. It gives every rejected event an identity for investigation, ticketing, or later remediation.


## This pipeline table should remain an immutable audit record. If a data issue is corrected later, 
# a corrected source event flows through normally; we do not manually update a pipeline-managed quarantine table.