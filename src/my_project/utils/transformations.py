from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def filter_active_customers(df: DataFrame) -> DataFrame:
    """
    Filters out any customers who are not active.
    Only returns rows where status == 'active'.
    """
    return df.filter(F.col("status") =="active")

def uppercase_customer_names(df: DataFrame) -> DataFrame:
    """
    Transforms the name column to uppercase.
    e.g. 'alice' becomes 'Alice'
    """
    return df.withColumn("name",F.upper(F.col("name")))
    