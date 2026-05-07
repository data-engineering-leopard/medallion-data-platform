from my_project.utils.logger import get_logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = get_logger(__name__)


def add_surrogate_key(df: DataFrame, key_column_name: str) -> DataFrame:
    """
    Adds a unique surrogate key column to a DataFrame.

    Uses monotonically_increasing_id() to generate a unique
    long integer per row. The key is added as the first column.

    Args:
        df: Input DataFrame
        key_column_name: Name of the surrogate key column to add

    Returns:
        DataFrame with surrogate key as the first column
    """
    logger.info(f"Adding surrogate key column: {key_column_name}")

    df_with_key = df.withColumn(
        key_column_name,
        F.monotonically_increasing_id()
    )

    # Reorder so surrogate key is the first column
    cols = [key_column_name] + [
        col for col in df.columns if col != key_column_name
    ]

    return df_with_key.select(cols)