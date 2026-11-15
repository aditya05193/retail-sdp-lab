from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view(name="sdp_valid_order_events")
def valid_order_events():
    return (
        spark.readStream
        .table("sdp_validated_orders")
        .where(F.col("rejection_reason").isNull())
        .withColumn(     # For every row, Marks records as deleted when operation is DELETE, else false. Adds a new column to the DataFrame. It is not a permanent table column. It is only used for processing.  
            "is_deleted",
            F.col("operation") == F.lit("DELETE"),
        )
    )


dp.create_streaming_table(
    name="sdp_silver_orders",
    comment="Latest valid order state maintained from ordered CDC events.",
    table_properties={"quality": "silver"},      # Adds a custom label to the Delta table. 
                                                 # It does not transform data or enforce any quality rule. 
                                                 # It is metadata for documentation, governance, discovery, and operational tooling.
)


dp.create_auto_cdc_flow(
    target="sdp_silver_orders",
    source="sdp_valid_order_events",
    keys=["order_id"], # Tells AUTO CDC that order_id identifies one order.
    sequence_by=F.col("updated_at"), # Tells AUTO CDC which event is newer. A newer event for an order replaces an older event. An old, late-arriving event cannot overwrite a newer state.
    except_column_list=[ # Excludes processing-only columns from Silver. For example, Silver does not need the original invalid raw values or the CDC operation label.
        "operation",
        "_rescued_data",
        "rejection_reason",
        "unit_price_raw",
        "event_ts_raw",
        "updated_at_raw",
    ],
    stored_as_scd_type=1, # Creates an SCD Type 1 table: it stores only the latest version of each order.
)


#    sdp_validated_orders
#        ↓ valid events only
#    sdp_valid_order_events       ← temporary pipeline view
#         ↓ AUTO CDC
#    sdp_silver_orders            ← latest state per order



## Key ideas:

#  @dp.temporary_view(...) : Creates an internal pipeline view. It is not a permanent table. We use it only to prepare clean events for CDC.

# keys=["order_id"] : Tells AUTO CDC that order_id identifies one order.

# sequence_by=F.col("updated_at") : Tells AUTO CDC which event is newer. A newer event for an order replaces an older event. An old, late-arriving event cannot overwrite a newer state.

# stored_as_scd_type=1 : Creates an SCD Type 1 table: it stores only the latest version of each order.

# For every row, it checks whether the CDC operation is DELETE. "is_deleted" is set to true only when the operation is DELETE; it is false for INSERT and UPDATE events.
# Adds a new column to the DataFrame. It is not a permanent table column. It is only used for processing.
#   is_deleted = true
# This matches your earlier soft-delete design. Gold tables will exclude deleted orders.


# except_column_list=[...] : Excludes processing-only columns from Silver. For example, Silver does not need the original invalid raw values or the CDC operation label.