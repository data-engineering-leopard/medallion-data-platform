from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform_gold(df: DataFrame) -> DataFrame:
    """
    Gold layer - aggregate silver data into business-ready metrics.
    Produces a count of active customers per country.
    """
    return (
        df
        .filter(F.col("status") == "active")
        .groupBy("country")
        .agg(F.count("id").alias("active_customer_count"))
        .orderBy("Country")
    )