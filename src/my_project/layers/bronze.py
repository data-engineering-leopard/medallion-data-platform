from pyspark.sql import DataFrame, SparkSession

def load_bronze(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Bronze layer - load raw data exactly as-is from source.
    No transformations, no cleaning. Just raw data preserved.
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)
