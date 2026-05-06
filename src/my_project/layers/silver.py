from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform_silver(df: DataFrame) -> DataFrame:
    """
    Silver layer - clean and validate the bronze data.
    - Standardise status to lowercase
    - Uppercase customer names
    - Remove rows with null names or null countries
    """
    return (
        df
        .filter(F.col("name").isNotNull())
        .filter(F.col("country").isNotNull())
        .withColumn("name", F.upper(F.col("name")))
        .withColumn("status", F.lower(F.col("status")))
    )