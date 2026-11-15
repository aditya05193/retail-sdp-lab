
### This file has one responsibility: ingest source JSON files into the Bronze table.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

from retail_sdp.config import get_source_path
from retail_sdp.schemas import ORDERS_SCHEMA

### Declares sdp_bronze_orders as a pipeline-managed streaming table. 
# Raw retail order events ingested from governed source files.

@dp.table(
    name="sdp_bronze_orders",
    table_properties={"quality": "bronze"},
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useManagedFileEvents", "false")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(ORDERS_SCHEMA)
        .load(get_source_path(spark))
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias(
                "_source_file_modified_at"
            ),
        )
    )


## .option("cloudFiles.schemaEvolutionMode", "rescue")
##  Keeps unexpected fields and incompatible types in _rescued_data; it does not silently discard them.


###  .option("cloudFiles.useManagedFileEvents", "false")
###  Uses directory listing for file discovery because file events are not configured successfully yet.