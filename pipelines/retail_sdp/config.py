# This file holds environment-specific settings in one place.

# Later, Bronze code will import it like this: from retail_sdp.config import SOURCE_PATH

#That means the Bronze code does not need to know whether it is running in development, test, or production.

DEFAULT_CATALOG = "workspace"
DEFAULT_SCHEMA = "retail_dev"

DEFAULT_SOURCE_PATH = (
    "/Volumes/workspace/retail_dev/source_files/orders/"
)


def get_source_path(spark_session):
    return spark_session.conf.get(
        "retail.source_path",
        DEFAULT_SOURCE_PATH,
    )